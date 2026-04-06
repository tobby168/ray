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

// Based on real Ray task API output. Names use task.name field (not func_or_class_name).
// Ray Data: func_or_class_name="_map_task", name="ReadParquet"/"Map(fn)"/"Filter(fn)" etc.
// Ray Core: func_or_class_name matches function name directly.
// Ray Serve: no task-level DAG (actors handle requests internally).
const MOCK_TASK_DETAILS: Record<string, MockDetail> = {
  // --- Ray Data ETL (real task.name values) ---
  // func_or_class_name is always "_map_task", resource is {CPU: 1} or {CPU: 1, memory: 128}
  ReadParquet: {
    durationStats: { count: 120000, min: 5, p50: 18, p95: 45, max: 120 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,  // Ray Data operators don't have call_site
  },
  "Map(parse_json)": {
    durationStats: { count: 120000, min: 10, p50: 35, p95: 80, max: 250 },
    resources: { CPU: 1.0, memory: 128.0 },
    retryStats: { retriedCount: 45, maxAttempt: 2 },
    callSite: null,
  },
  "Filter(validate)": {
    durationStats: { count: 115000, min: 2, p50: 8, p95: 22, max: 65 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  "Map(fetch_user_profile)": {
    durationStats: { count: 80000, min: 50, p50: 180, p95: 800, max: 3200 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 320, maxAttempt: 3 },
    callSite: null,
  },
  "Map(fetch_geo_data)": {
    durationStats: { count: 95000, min: 30, p50: 90, p95: 400, max: 1800 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 180, maxAttempt: 3 },
    callSite: null,
  },
  _split_single_block: {
    durationStats: { count: 60000, min: 1, p50: 5, p95: 15, max: 40 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  "MapBatches(deduplicate)": {
    durationStats: { count: 40000, min: 15, p50: 50, p95: 120, max: 300 },
    resources: { CPU: 1.0, memory: 128.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  "Map(write_parquet)": {
    durationStats: { count: 25000, min: 30, p50: 85, p95: 200, max: 600 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 5, maxAttempt: 1 },
    callSite: null,
  },
  // --- Ray Core Training (real func names, verified) ---
  load_data: {
    durationStats: { count: 8, min: 280, p50: 320, p95: 480, max: 510 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  preprocess: {
    durationStats: { count: 8, min: 180, p50: 210, p95: 290, max: 310 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  train_step: {
    durationStats: { count: 20, min: 280, p50: 340, p95: 520, max: 580 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 1, maxAttempt: 1 },
    callSite: null,
  },
  validate: {
    durationStats: { count: 2, min: 190, p50: 210, p95: 230, max: 230 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  save_checkpoint: {
    durationStats: { count: 2, min: 100, p50: 120, p95: 140, max: 140 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  // --- Ray Core Eval (real func names, verified) ---
  load_model: {
    durationStats: { count: 1, min: 520, p50: 520, p95: 520, max: 520 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  load_dataset: {
    durationStats: { count: 2, min: 290, p50: 310, p95: 330, max: 330 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  run_inference: {
    durationStats: { count: 4200, min: 40, p50: 55, p95: 90, max: 180 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 8, maxAttempt: 2 },
    callSite: null,
  },
  compute_accuracy: {
    durationStats: { count: 1, min: 210, p50: 210, p95: 210, max: 210 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  compute_latency: {
    durationStats: { count: 1, min: 105, p50: 105, p95: 105, max: 105 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  compute_f1: {
    durationStats: { count: 1, min: 195, p50: 195, p95: 195, max: 195 },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  aggregate_metrics: {
    durationStats: { count: 0, min: null, p50: null, p95: null, max: null },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
  },
  generate_report: {
    durationStats: { count: 0, min: null, p50: null, p95: null, max: null },
    resources: { CPU: 1.0 },
    retryStats: { retriedCount: 0, maxAttempt: 0 },
    callSite: null,
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
