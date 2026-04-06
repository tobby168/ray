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
import useSWR from "swr";
import { DAGSummary, NestedJobProgress } from "../../../type/job";
import { Task } from "../../../type/task";
import { StateApiResponse } from "../../../type/stateApi";
import { get } from "../../../service/requestHandlers";
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

const fetchTasksByFuncName = (jobId: string, funcName: string) => {
  const url =
    `api/v0/tasks?detail=1&limit=10000` +
    `&filter_keys=job_id,func_or_class_name` +
    `&filter_predicates=%3D,%3D` +
    `&filter_values=${jobId},${encodeURIComponent(funcName)}`;
  return get<StateApiResponse<Task>>(url);
};

type DurationStats = {
  count: number;
  p50: number | null;
  p95: number | null;
  max: number | null;
  min: number | null;
};

const computeDurationStats = (tasks: Task[]): DurationStats => {
  const durations = tasks
    .filter((t) => t.start_time_ms !== null && t.end_time_ms !== null)
    .map((t) => t.end_time_ms! - t.start_time_ms!)
    .sort((a, b) => a - b);

  if (durations.length === 0) {
    return { count: 0, p50: null, p95: null, max: null, min: null };
  }

  return {
    count: durations.length,
    p50: durations[Math.floor(durations.length * 0.5)],
    p95: durations[Math.floor(durations.length * 0.95)],
    max: durations[durations.length - 1],
    min: durations[0],
  };
};

const formatDuration = (ms: number | null): string => {
  if (ms === null) return "-";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
};

type DAGNodeDetailPanelProps = {
  summary: NestedJobProgress;
  dagSummary: DAGSummary;
  jobId: string;
};

const DAGNodeDetailPanel = ({
  summary,
  dagSummary,
  jobId,
}: DAGNodeDetailPanelProps) => {
  const theme = useTheme();

  // Fetch task details for this function
  const { data: taskData } = useSWR(
    ["dagNodeDetail", jobId, summary.name],
    async (): Promise<Task[]> => {
      const rsp = await fetchTasksByFuncName(jobId, summary.name);
      if (rsp.data.result) {
        return rsp.data.data.result as unknown as Task[];
      }
      return [] as Task[];
    },
    { revalidateOnFocus: false },
  );

  const tasks: Task[] = taskData ?? [];

  // State distribution
  const stateCounts = summary.state_counts;
  const total = Object.values(stateCounts).reduce((a, b) => a + b, 0);

  // Duration stats
  const durationStats = useMemo(() => computeDurationStats(tasks), [tasks]);

  // Resource requirements (from first task that has them)
  const resources = useMemo(() => {
    const t = tasks.find(
      (t) => t.required_resources && Object.keys(t.required_resources).length > 0,
    );
    return t?.required_resources ?? {};
  }, [tasks]);

  // Retry stats
  const retryStats = useMemo(() => {
    const retried = tasks.filter((t) => t.attempt_number > 0);
    const maxRetry = tasks.reduce(
      (max, t) => Math.max(max, t.attempt_number),
      0,
    );
    return { retriedCount: retried.length, maxAttempt: maxRetry };
  }, [tasks]);

  // Call site (from first task that has it)
  const callSite = useMemo(() => {
    return tasks.find((t) => t.call_site)?.call_site ?? null;
  }, [tasks]);

  // Upstream / downstream from edges
  const upstream = dagSummary.edges
    .filter((e) => e.target === summary.key)
    .map((e) => {
      const node = dagSummary.nodes.find((n) => n.key === e.source);
      return node;
    })
    .filter(Boolean) as NestedJobProgress[];

  const downstream = dagSummary.edges
    .filter((e) => e.source === summary.key)
    .map((e) => {
      const node = dagSummary.nodes.find((n) => n.key === e.target);
      return node;
    })
    .filter(Boolean) as NestedJobProgress[];

  const bottleneck = detectBottleneck(stateCounts);
  const borderColorMap: Record<BottleneckType, string> = {
    failing: theme.palette.error.main,
    slow: theme.palette.warning.main,
    blocked: theme.palette.info.main,
    healthy: theme.palette.grey[300],
    done: theme.palette.success.main,
  };

  const nodeProgressSummary = (node: NestedJobProgress) => {
    const t = Object.values(node.state_counts).reduce((a, b) => a + b, 0);
    const f = node.state_counts["FINISHED"] ?? 0;
    const fail = node.state_counts["FAILED"] ?? 0;
    const p = t > 0 ? Math.round((f / t) * 100) : 0;
    return `${f.toLocaleString()}/${t.toLocaleString()} (${p}%)${fail > 0 ? ` ${fail} failed` : ""}`;
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
          <Typography
            variant="caption"
            color="text.secondary"
            fontWeight={600}
          >
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
        <Box sx={{ minWidth: 160 }}>
          <Typography
            variant="caption"
            color="text.secondary"
            fontWeight={600}
          >
            Duration ({durationStats.count} completed)
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

        {/* Resources & Retries */}
        <Box sx={{ minWidth: 160 }}>
          <Typography
            variant="caption"
            color="text.secondary"
            fontWeight={600}
          >
            Resources
          </Typography>
          <Table size="small" sx={{ mt: 0.5 }}>
            <TableBody>
              {Object.keys(resources).length > 0 ? (
                Object.entries(resources).map(([key, val]) => (
                  <TableRow key={key} sx={{ "&:last-child td": { border: 0 } }}>
                    <TableCell sx={{ py: 0.25, pl: 0 }}>
                      <Typography variant="caption">{key}</Typography>
                    </TableCell>
                    <TableCell align="right" sx={{ py: 0.25, pr: 0 }}>
                      <Typography variant="caption" fontWeight={600}>
                        {val}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow sx={{ "&:last-child td": { border: 0 } }}>
                  <TableCell sx={{ py: 0.25, pl: 0 }}>
                    <Typography variant="caption" color="text.secondary">
                      {tasks.length > 0 ? "No resources specified" : "Loading..."}
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>

          <Typography
            variant="caption"
            color="text.secondary"
            fontWeight={600}
            sx={{ mt: 1, display: "block" }}
          >
            Retries
          </Typography>
          <Typography variant="caption">
            {retryStats.retriedCount > 0
              ? `${retryStats.retriedCount} tasks retried (max attempt: ${retryStats.maxAttempt})`
              : tasks.length > 0
                ? "No retries"
                : "Loading..."}
          </Typography>
        </Box>

        {/* Upstream / Downstream */}
        <Box sx={{ minWidth: 180 }}>
          <Typography
            variant="caption"
            color="text.secondary"
            fontWeight={600}
          >
            Upstream
          </Typography>
          {upstream.length > 0 ? (
            upstream.map((u) => (
              <Typography key={u.key} variant="caption" display="block">
                ← {u.name} {nodeProgressSummary(u)}
              </Typography>
            ))
          ) : (
            <Typography variant="caption" color="text.secondary" display="block">
              None (source)
            </Typography>
          )}

          <Typography
            variant="caption"
            color="text.secondary"
            fontWeight={600}
            sx={{ mt: 1, display: "block" }}
          >
            Downstream
          </Typography>
          {downstream.length > 0 ? (
            downstream.map((d) => (
              <Typography key={d.key} variant="caption" display="block">
                → {d.name} {nodeProgressSummary(d)}
              </Typography>
            ))
          ) : (
            <Typography variant="caption" color="text.secondary" display="block">
              None (sink)
            </Typography>
          )}
        </Box>
      </Box>

      {/* Call Site */}
      {callSite && (
        <React.Fragment>
          <Divider sx={{ my: 1.5 }} />
          <Typography variant="caption" color="text.secondary" fontWeight={600}>
            Call Site
          </Typography>
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
        </React.Fragment>
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
