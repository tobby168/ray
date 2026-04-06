import {
  Box,
  Chip,
  Collapse,
  Divider,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableRow,
  Typography,
  useTheme,
} from "@mui/material";
import dagre from "dagre";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  Edge,
  Handle,
  MarkerType,
  MiniMap,
  Node,
  Position,
  useEdgesState,
  useNodesState,
} from "reactflow";
import "reactflow/dist/style.css";
import { DAGSummary, NestedJobProgress } from "../../../type/job";
import { formatStateCountsToProgress } from "../hook/useJobProgress";
import { MiniTaskProgressBar } from "../TaskProgressBar";

const DAG_NODE_WIDTH = 240;
const DAG_NODE_HEIGHT = 100;

type BottleneckType = "failing" | "slow" | "blocked" | "healthy" | "done";

const detectBottleneck = (stateCounts: {
  [key: string]: number;
}): BottleneckType => {
  const total = Object.values(stateCounts).reduce((a, b) => a + b, 0);
  const finished = stateCounts["FINISHED"] ?? 0;
  const failed = stateCounts["FAILED"] ?? 0;

  if (finished === total && total > 0) {
    return "done";
  }
  if (total > 0 && failed / total > 0.05) {
    return "failing";
  }

  const pendingArgs = stateCounts["PENDING_ARGS_AVAIL"] ?? 0;
  const pending =
    pendingArgs +
    (stateCounts["PENDING_NODE_ASSIGNMENT"] ?? 0) +
    (stateCounts["PENDING_OBJ_STORE_MEM_AVAIL"] ?? 0) +
    (stateCounts["PENDING_ARGS_FETCH"] ?? 0);

  if (pending > 0 && pendingArgs === pending) {
    return "blocked";
  }
  if (total > 0 && finished / total < 0.3 && pending > total * 0.5) {
    return "slow";
  }

  return "healthy";
};

const bottleneckLabel: Record<BottleneckType, string> = {
  failing: "Failing",
  slow: "Bottleneck",
  blocked: "Blocked",
  healthy: "",
  done: "Done",
};

const DAGNodeComponent = ({
  data,
}: {
  data: {
    summary: NestedJobProgress;
    selected: boolean;
    onSelect: (key: string) => void;
  };
}) => {
  const theme = useTheme();
  const { summary, selected, onSelect } = data;
  const progress = formatStateCountsToProgress(summary.state_counts);
  const total = Object.values(progress).reduce((a, b) => a + (b ?? 0), 0);
  const finished = progress.numFinished ?? 0;
  const failed = progress.numFailed ?? 0;
  const pct = total > 0 ? Math.round((finished / total) * 100) : 0;

  const bottleneck = detectBottleneck(summary.state_counts);
  const borderColorMap: Record<BottleneckType, string> = {
    failing: theme.palette.error.main,
    slow: theme.palette.warning.main,
    blocked: theme.palette.info.main,
    healthy: theme.palette.grey[300],
    done: theme.palette.success.main,
  };

  return (
    <React.Fragment>
      <Handle type="target" position={Position.Left} style={{ opacity: 0 }} />
      <Paper
        variant="outlined"
        onClick={() => onSelect(summary.key)}
        sx={{
          p: 1.5,
          borderColor: borderColorMap[bottleneck],
          borderWidth: selected ? 3 : 2,
          width: DAG_NODE_WIDTH,
          cursor: "pointer",
          boxShadow: selected ? 3 : 0,
          "&:hover": { boxShadow: 2 },
        }}
      >
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            mb: 0.5,
          }}
        >
          <Typography variant="body2" fontWeight={600} noWrap sx={{ flex: 1 }}>
            {summary.name}
          </Typography>
          <Typography
            variant="body2"
            fontWeight={700}
            sx={{
              ml: 1,
              color:
                pct === 100
                  ? theme.palette.success.main
                  : theme.palette.text.secondary,
            }}
          >
            {pct}%
          </Typography>
        </Box>
        <MiniTaskProgressBar {...progress} showTotal />
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            mt: 0.5,
          }}
        >
          <Typography variant="caption" color="text.secondary">
            {finished.toLocaleString()}/{total.toLocaleString()}
            {failed > 0 && (
              <Typography
                component="span"
                variant="caption"
                color="error.main"
              >
                {" "}
                ({failed} failed)
              </Typography>
            )}
          </Typography>
          {bottleneckLabel[bottleneck] && (
            <Chip
              label={bottleneckLabel[bottleneck]}
              size="small"
              sx={{
                height: 18,
                fontSize: 10,
                backgroundColor: borderColorMap[bottleneck],
                color: "#fff",
              }}
            />
          )}
        </Box>
      </Paper>
      <Handle type="source" position={Position.Right} style={{ opacity: 0 }} />
    </React.Fragment>
  );
};

const nodeTypes = { dagNode: DAGNodeComponent };

// --- Detail Panel ---

type DurationStats = {
  count: number;
  p50: number | null;
  p95: number | null;
  max: number | null;
  min: number | null;
};

const formatDuration = (ms: number | null): string => {
  if (ms === null) return "-";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
};

// --- Mock task data for testing ---
// TODO: Remove mock data and use real API (fetchTasksByFuncName) once backend is deployed.
type MockDetail = {
  durationStats: DurationStats;
  resources: Record<string, number>;
  retryStats: { retriedCount: number; maxAttempt: number };
  callSite: string | null;
};

const FALLBACK_DETAIL: MockDetail = {
  durationStats: { count: 0, min: null, p50: null, p95: null, max: null },
  resources: { CPU: 1 },
  retryStats: { retriedCount: 0, maxAttempt: 0 },
  callSite: null,
};

const MOCK_TASK_DETAILS: Record<string, MockDetail> = {
  // --- Default / Training pipeline ---
  read_parquet: {
    durationStats: { count: 50000, min: 12, p50: 45, p95: 120, max: 380 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "pipeline.py:42 in build_pipeline\n  read_parquet.remote(path)",
  },
  preprocess: {
    durationStats: { count: 36000, min: 80, p50: 120, p95: 340, max: 890 },
    resources: { CPU: 2, memory: 4000000000 },
    retryStats: { retriedCount: 23, maxAttempt: 2 },
    callSite: "pipeline.py:55 in build_pipeline\n  preprocess.remote(block)",
  },
  train_batch: {
    durationStats: { count: 1500, min: 1200, p50: 2100, p95: 3200, max: 8700 },
    resources: { CPU: 1, GPU: 1 },
    retryStats: { retriedCount: 12, maxAttempt: 3 },
    callSite: "pipeline.py:68 in build_pipeline\n  train_batch.remote(data, weights)",
  },
  load_weights: {
    durationStats: { count: 1, min: 3200, p50: 3200, p95: 3200, max: 3200 },
    resources: { CPU: 1, memory: 8000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "pipeline.py:35 in build_pipeline\n  load_weights.remote(model_path)",
  },
  save_model: {
    durationStats: { count: 0, min: null, p50: null, p95: null, max: null },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "pipeline.py:82 in build_pipeline\n  save_model.remote(checkpoint)",
  },
  // --- Big Data Processing (ETL) ---
  ReadCSV: {
    durationStats: { count: 120000, min: 5, p50: 18, p95: 45, max: 120 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "etl.py:23 in build_etl\n  ReadCSV.remote(file_path)",
  },
  FilterInvalid: {
    durationStats: { count: 120000, min: 2, p50: 8, p95: 22, max: 65 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "etl.py:30 in build_etl\n  FilterInvalid.remote(record)",
  },
  ParseJSON: {
    durationStats: { count: 115000, min: 10, p50: 35, p95: 80, max: 250 },
    resources: { CPU: 1, memory: 2000000000 },
    retryStats: { retriedCount: 45, maxAttempt: 2 },
    callSite: "etl.py:38 in build_etl\n  ParseJSON.remote(raw_text)",
  },
  FetchUserProfile: {
    durationStats: { count: 80000, min: 50, p50: 180, p95: 800, max: 3200 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 320, maxAttempt: 3 },
    callSite: "etl.py:45 in build_etl\n  FetchUserProfile.remote(user_id)",
  },
  FetchGeoData: {
    durationStats: { count: 95000, min: 30, p50: 90, p95: 400, max: 1800 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 180, maxAttempt: 3 },
    callSite: "etl.py:52 in build_etl\n  FetchGeoData.remote(ip_address)",
  },
  JoinFeatures: {
    durationStats: { count: 60000, min: 20, p50: 65, p95: 150, max: 420 },
    resources: { CPU: 2, memory: 4000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "etl.py:60 in build_etl\n  JoinFeatures.remote(user, geo, parsed)",
  },
  Deduplicate: {
    durationStats: { count: 40000, min: 15, p50: 50, p95: 120, max: 300 },
    resources: { CPU: 1, memory: 2000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "etl.py:68 in build_etl\n  Deduplicate.remote(features)",
  },
  WriteParquet: {
    durationStats: { count: 25000, min: 30, p50: 85, p95: 200, max: 600 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 5, maxAttempt: 1 },
    callSite: "etl.py:75 in build_etl\n  WriteParquet.remote(batch, output_path)",
  },
  // --- Model Training ---
  ReadParquet: {
    durationStats: { count: 50000, min: 10, p50: 40, p95: 100, max: 350 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "train.py:28 in data_pipeline\n  ReadParquet.remote(shard_path)",
  },
  Tokenize: {
    durationStats: { count: 48000, min: 15, p50: 55, p95: 150, max: 400 },
    resources: { CPU: 1, memory: 2000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "train.py:35 in data_pipeline\n  Tokenize.remote(text_block)",
  },
  Augment: {
    durationStats: { count: 45000, min: 20, p50: 70, p95: 200, max: 550 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "train.py:42 in data_pipeline\n  Augment.remote(tokens)",
  },
  ShuffleAndBatch: {
    durationStats: { count: 40000, min: 5, p50: 25, p95: 60, max: 180 },
    resources: { CPU: 1, memory: 4000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "train.py:50 in data_pipeline\n  ShuffleAndBatch.remote(augmented)",
  },
  LoadCheckpoint: {
    durationStats: { count: 1, min: 4500, p50: 4500, p95: 4500, max: 4500 },
    resources: { CPU: 1, memory: 16000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "train.py:22 in setup\n  LoadCheckpoint.remote(ckpt_path)",
  },
  TrainStep: {
    durationStats: { count: 8500, min: 800, p50: 1500, p95: 2800, max: 6200 },
    resources: { CPU: 4, GPU: 1, memory: 16000000000 },
    retryStats: { retriedCount: 3, maxAttempt: 2 },
    callSite: "train.py:65 in training_loop\n  TrainStep.remote(batch, model_state)",
  },
  ValidateEpoch: {
    durationStats: { count: 17, min: 12000, p50: 15000, p95: 18000, max: 22000 },
    resources: { CPU: 4, GPU: 1, memory: 16000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "train.py:80 in training_loop\n  ValidateEpoch.remote(model_state, val_data)",
  },
  SaveCheckpoint: {
    durationStats: { count: 17, min: 2000, p50: 2500, p95: 3500, max: 4000 },
    resources: { CPU: 1, memory: 16000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "train.py:90 in training_loop\n  SaveCheckpoint.remote(model_state, epoch)",
  },
  // --- Model Serving ---
  HTTPIngress: {
    durationStats: { count: 285000, min: 1, p50: 3, p95: 8, max: 25 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  Preprocess: {
    durationStats: { count: 284500, min: 2, p50: 5, p95: 15, max: 40 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  Tokenizer: {
    durationStats: { count: 284000, min: 3, p50: 12, p95: 35, max: 80 },
    resources: { CPU: 1, memory: 2000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  EmbeddingModel: {
    durationStats: { count: 280000, min: 8, p50: 25, p95: 60, max: 150 },
    resources: { CPU: 1, GPU: 1, memory: 4000000000 },
    retryStats: { retriedCount: 50, maxAttempt: 1 },
    callSite: null,
  },
  RetrievalIndex: {
    durationStats: { count: 280000, min: 5, p50: 15, p95: 45, max: 120 },
    resources: { CPU: 2, memory: 8000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  LLMGenerate: {
    durationStats: { count: 260000, min: 200, p50: 850, p95: 2400, max: 8500 },
    resources: { CPU: 1, GPU: 1, memory: 32000000000 },
    retryStats: { retriedCount: 150, maxAttempt: 2 },
    callSite: null,
  },
  Guardrails: {
    durationStats: { count: 259000, min: 5, p50: 20, p95: 50, max: 120 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  ResponseFormatter: {
    durationStats: { count: 258000, min: 1, p50: 4, p95: 10, max: 30 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  // --- Eval pipeline ---
  LoadModel: {
    durationStats: { count: 1, min: 5200, p50: 5200, p95: 5200, max: 5200 },
    resources: { CPU: 1, GPU: 1, memory: 16000000000 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:15 in run_eval\n  LoadModel.remote(model_path)",
  },
  LoadTestDataset: {
    durationStats: { count: 5000, min: 8, p50: 30, p95: 80, max: 200 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:22 in run_eval\n  LoadTestDataset.remote(dataset_path)",
  },
  RunInference: {
    durationStats: { count: 4200, min: 50, p50: 180, p95: 500, max: 1200 },
    resources: { CPU: 1, GPU: 1 },
    retryStats: { retriedCount: 8, maxAttempt: 2 },
    callSite: "eval.py:30 in run_eval\n  RunInference.remote(model, sample)",
  },
  LoadGoldenLabels: {
    durationStats: { count: 5000, min: 3, p50: 10, p95: 25, max: 60 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:25 in run_eval\n  LoadGoldenLabels.remote(label_path)",
  },
  ComputeAccuracy: {
    durationStats: { count: 3800, min: 2, p50: 8, p95: 20, max: 50 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:42 in run_eval\n  ComputeAccuracy.remote(pred, label)",
  },
  ComputeLatencyStats: {
    durationStats: { count: 4200, min: 1, p50: 3, p95: 8, max: 20 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:48 in run_eval\n  ComputeLatencyStats.remote(timing)",
  },
  ComputeF1Score: {
    durationStats: { count: 3800, min: 3, p50: 10, p95: 25, max: 55 },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:54 in run_eval\n  ComputeF1Score.remote(pred, label)",
  },
  AggregateMetrics: {
    durationStats: { count: 0, min: null, p50: null, p95: null, max: null },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:62 in run_eval\n  AggregateMetrics.remote(*metrics)",
  },
  GenerateReport: {
    durationStats: { count: 0, min: null, p50: null, p95: null, max: null },
    resources: { CPU: 1 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: "eval.py:70 in run_eval\n  GenerateReport.remote(aggregated)",
  },
};

type DAGNodeDetailPanelProps = {
  summary: NestedJobProgress;
  dagSummary: DAGSummary;
  jobId: string;
};

const DAGNodeDetailPanel = ({
  summary,
}: DAGNodeDetailPanelProps) => {
  const theme = useTheme();

  // TODO: Replace with real API call:
  // const { data: taskData } = useSWR(
  //   ["dagNodeDetail", jobId, summary.name],
  //   async (): Promise<Task[]> => {
  //     const rsp = await fetchTasksByFuncName(jobId, summary.name);
  //     if (rsp.data.result) return rsp.data.data.result as unknown as Task[];
  //     return [];
  //   },
  //   { revalidateOnFocus: false },
  // );
  // const tasks: Task[] = taskData ?? [];
  // const durationStats = useMemo(() => computeDurationStats(tasks), [tasks]);
  // const resources = useMemo(() => { ... from tasks ... }, [tasks]);
  // const retryStats = useMemo(() => { ... from tasks ... }, [tasks]);
  // const callSite = useMemo(() => tasks.find(t => t.call_site)?.call_site ?? null, [tasks]);

  const mock = MOCK_TASK_DETAILS[summary.name] ?? FALLBACK_DETAIL;
  const durationStats = mock.durationStats;
  const resources = mock.resources;
  const retryStats = mock.retryStats;
  const callSite = mock.callSite;

  // State distribution
  const stateCounts = summary.state_counts;
  const total = Object.values(stateCounts).reduce((a, b) => a + b, 0);

  const bottleneck = detectBottleneck(stateCounts);
  const borderColorMap: Record<BottleneckType, string> = {
    failing: theme.palette.error.main,
    slow: theme.palette.warning.main,
    blocked: theme.palette.info.main,
    healthy: theme.palette.grey[300],
    done: theme.palette.success.main,
  };

  const formatBytes = (bytes: number): string => {
    if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`;
    if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(0)} MB`;
    return `${bytes}`;
  };

  const formatResourceValue = (key: string, val: number): string => {
    if (key.toLowerCase() === "memory" || key.toLowerCase() === "object_store_memory") {
      return formatBytes(val);
    }
    return String(val);
  };

  return (
    <Paper
      variant="outlined"
      sx={{
        mt: 2,
        p: 2,
        borderColor: borderColorMap[bottleneck],
        borderWidth: 2,
      }}
    >
      <Typography variant="subtitle1" fontWeight={700} gutterBottom>
        {summary.name}
      </Typography>

      <Box sx={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
        {/* State Distribution */}
        <Box sx={{ minWidth: 200 }}>
          <Typography variant="caption" color="text.secondary" fontWeight={600}>
            State Distribution
          </Typography>
          <Table size="small" sx={{ mt: 0.5 }}>
            <TableBody>
              {Object.entries(stateCounts)
                .sort(([, a], [, b]) => b - a)
                .map(([state, count]) => (
                  <TableRow key={state} sx={{ "&:last-child td": { border: 0 } }}>
                    <TableCell sx={{ py: 0.25, pl: 0, width: 160 }}>
                      <Typography variant="caption">{state}</Typography>
                    </TableCell>
                    <TableCell align="right" sx={{ py: 0.25 }}>
                      <Typography variant="caption" fontWeight={600}>
                        {count.toLocaleString()}
                      </Typography>
                    </TableCell>
                    <TableCell align="right" sx={{ py: 0.25, pr: 0 }}>
                      <Typography variant="caption" color="text.secondary">
                        {total > 0 ? `${((count / total) * 100).toFixed(1)}%` : ""}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
        </Box>

        {/* Duration Distribution */}
        <Box sx={{ minWidth: 140 }}>
          <Typography variant="caption" color="text.secondary" fontWeight={600}>
            Duration ({durationStats.count.toLocaleString()} completed)
          </Typography>
          <Table size="small" sx={{ mt: 0.5 }}>
            <TableBody>
              {(
                [
                  ["min", durationStats.min],
                  ["p50", durationStats.p50],
                  ["p95", durationStats.p95],
                  ["max", durationStats.max],
                ] as [string, number | null][]
              ).map(([label, val]) => (
                <TableRow key={label} sx={{ "&:last-child td": { border: 0 } }}>
                  <TableCell sx={{ py: 0.25, pl: 0 }}>
                    <Typography variant="caption">{label}</Typography>
                  </TableCell>
                  <TableCell align="right" sx={{ py: 0.25, pr: 0 }}>
                    <Typography variant="caption" fontWeight={600}>
                      {formatDuration(val)}
                    </Typography>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>

        {/* Resources */}
        <Box sx={{ minWidth: 140 }}>
          <Typography variant="caption" color="text.secondary" fontWeight={600}>
            Resources (per task)
          </Typography>
          <Table size="small" sx={{ mt: 0.5 }}>
            <TableBody>
              {Object.entries(resources).map(([key, val]) => (
                <TableRow key={key} sx={{ "&:last-child td": { border: 0 } }}>
                  <TableCell sx={{ py: 0.25, pl: 0 }}>
                    <Typography variant="caption">{key}</Typography>
                  </TableCell>
                  <TableCell align="right" sx={{ py: 0.25, pr: 0 }}>
                    <Typography variant="caption" fontWeight={600}>
                      {formatResourceValue(key, val)}
                    </Typography>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>

        {/* Retries */}
        <Box sx={{ minWidth: 120 }}>
          <Typography variant="caption" color="text.secondary" fontWeight={600}>
            Retries
          </Typography>
          <Box sx={{ mt: 0.5 }}>
            {retryStats.retriedCount > 0 ? (
              <React.Fragment>
                <Typography variant="caption" display="block">
                  {retryStats.retriedCount} tasks retried
                </Typography>
                <Typography variant="caption" display="block" color="text.secondary">
                  max attempt: {retryStats.maxAttempt}
                </Typography>
              </React.Fragment>
            ) : (
              <Typography variant="caption" color="text.secondary">
                No retries
              </Typography>
            )}
          </Box>
        </Box>
      </Box>

      {/* Call Site */}
      <Divider sx={{ my: 1.5 }} />
      <Typography variant="caption" color="text.secondary" fontWeight={600}>
        Call Site
      </Typography>
      {callSite ? (
        <Typography
          variant="caption"
          display="block"
          sx={{
            fontFamily: "monospace",
            bgcolor: theme.palette.grey[100],
            p: 0.5,
            borderRadius: 0.5,
            mt: 0.5,
            whiteSpace: "pre-wrap",
            wordBreak: "break-all",
          }}
        >
          {callSite}
        </Typography>
      ) : (
        <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
          Not available. Set RAY_record_ref_creation_sites=1 to enable.
        </Typography>
      )}
    </Paper>
  );
};

// --- Layout ---

const computeLayout = (
  summary: DAGSummary,
): { nodes: Node[]; edges: Edge[] } => {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "LR", ranksep: 100, nodesep: 50 });

  for (const node of summary.nodes) {
    g.setNode(node.key, { width: DAG_NODE_WIDTH, height: DAG_NODE_HEIGHT });
  }
  for (const edge of summary.edges) {
    g.setEdge(edge.source, edge.target);
  }
  dagre.layout(g);

  return {
    nodes: summary.nodes.map((node) => {
      const { x, y } = g.node(node.key);
      return {
        id: node.key,
        type: "dagNode",
        position: {
          x: x - DAG_NODE_WIDTH / 2,
          y: y - DAG_NODE_HEIGHT / 2,
        },
        data: { summary: node },
      };
    }),
    edges: summary.edges.map((edge) => ({
      id: `${edge.source}-${edge.target}`,
      source: edge.source,
      target: edge.target,
      animated: true,
      style: { strokeWidth: 2 },
      markerEnd: { type: MarkerType.ArrowClosed },
    })),
  };
};

// --- Main Component ---

export type DAGProgressBarProps = {
  summary: DAGSummary | undefined;
  jobId?: string;
};

export const DAGProgressBar = ({ summary, jobId }: DAGProgressBarProps) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  const handleNodeSelect = useCallback((key: string) => {
    setSelectedNode((prev) => (prev === key ? null : key));
  }, []);

  useEffect(() => {
    if (!summary || summary.nodes.length === 0) {
      return;
    }
    const layout = computeLayout(summary);
    // Inject selection state and click handler into node data
    const nodesWithSelection = layout.nodes.map((node) => ({
      ...node,
      data: {
        ...node.data,
        selected: node.id === selectedNode,
        onSelect: handleNodeSelect,
      },
    }));
    setNodes(nodesWithSelection);
    setEdges(layout.edges);
  }, [summary, selectedNode, handleNodeSelect, setNodes, setEdges]);

  const selectedSummary = useMemo(() => {
    if (!selectedNode || !summary) return null;
    return summary.nodes.find((n) => n.key === selectedNode) ?? null;
  }, [selectedNode, summary]);

  if (!summary || summary.nodes.length === 0) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ p: 2 }}>
        No dataflow information available for this job.
      </Typography>
    );
  }

  return (
    <Box>
      <Box sx={{ height: 400, width: "100%" }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeTypes={nodeTypes}
          fitView
          attributionPosition="bottom-left"
          proOptions={{ hideAttribution: true }}
        >
          <Background />
          <Controls />
          <MiniMap />
        </ReactFlow>
      </Box>
      <Collapse in={selectedSummary !== null}>
        {selectedSummary && summary && jobId && (
          <DAGNodeDetailPanel
            summary={selectedSummary}
            dagSummary={summary}
            jobId={jobId}
          />
        )}
      </Collapse>
    </Box>
  );
};
