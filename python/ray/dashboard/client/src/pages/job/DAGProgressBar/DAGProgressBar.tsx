import { Box, Paper, Typography, useTheme } from "@mui/material";
import dagre from "dagre";
import React, { useEffect } from "react";
import ReactFlow, {
  Background,
  Controls,
  Edge,
  Handle,
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

const DAG_NODE_WIDTH = 220;
const DAG_NODE_HEIGHT = 90;

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

const DAGNodeComponent = ({
  data,
}: {
  data: { summary: NestedJobProgress };
}) => {
  const theme = useTheme();
  const { summary } = data;
  const progress = formatStateCountsToProgress(summary.state_counts);
  const total = Object.values(progress).reduce(
    (a, b) => a + (b ?? 0),
    0,
  );
  const finished = progress.numFinished ?? 0;
  const failed = progress.numFailed ?? 0;

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
        sx={{
          p: 1.5,
          borderColor: borderColorMap[bottleneck],
          borderWidth: 2,
          width: DAG_NODE_WIDTH,
          "&:hover": { boxShadow: 2 },
        }}
      >
        <Typography variant="body2" fontWeight={600} noWrap>
          {summary.name}
        </Typography>
        <MiniTaskProgressBar {...progress} showTotal />
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
      </Paper>
      <Handle type="source" position={Position.Right} style={{ opacity: 0 }} />
    </React.Fragment>
  );
};

const nodeTypes = { dagNode: DAGNodeComponent };

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
      markerEnd: { type: "arrowclosed" as const },
    })),
  };
};

export type DAGProgressBarProps = {
  summary: DAGSummary | undefined;
};

export const DAGProgressBar = ({ summary }: DAGProgressBarProps) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    if (!summary || summary.nodes.length === 0) {
      return;
    }
    const layout = computeLayout(summary);
    setNodes(layout.nodes);
    setEdges(layout.edges);
  }, [summary, setNodes, setEdges]);

  if (!summary || summary.nodes.length === 0) {
    return (
      <Typography variant="body2" color="text.secondary" sx={{ p: 2 }}>
        No dataflow information available for this job.
      </Typography>
    );
  }

  return (
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
  );
};
