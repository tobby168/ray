import _ from "lodash";
import { useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { sliceToPage } from "../../../common/util";
import {
  getStateApiJobProgressByDataflow,
  getStateApiJobProgressByLineage,
  getStateApiJobProgressByTaskName,
} from "../../../service/job";
import {
  DAGSummary,
  JobProgressGroup,
  NestedJobProgress,
  StateApiJobProgressByTaskName,
  StateApiNestedJobProgress,
  TaskProgress,
} from "../../../type/job";
import { TypeTaskStatus } from "../../../type/task";

export enum TaskStatus {
  PENDING_ARGS_AVAIL = "PENDING_ARGS_AVAIL",
  PENDING_NODE_ASSIGNMENT = "PENDING_NODE_ASSIGNMENT",
  SUBMITTED_TO_WORKER = "SUBMITTED_TO_WORKER",
  RUNNING = "RUNNING",
  FINISHED = "FINISHED",
  FAILED = "FAILED",
  UNKNOWN = "UNKNOWN",
}

const TASK_STATE_NAME_TO_PROGRESS_KEY: Record<TypeTaskStatus, TaskStatus> = {
  [TypeTaskStatus.PENDING_ARGS_AVAIL]: TaskStatus.PENDING_ARGS_AVAIL,
  [TypeTaskStatus.PENDING_NODE_ASSIGNMENT]: TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.PENDING_OBJ_STORE_MEM_AVAIL]:
    TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.PENDING_ARGS_FETCH]: TaskStatus.PENDING_NODE_ASSIGNMENT,
  [TypeTaskStatus.SUBMITTED_TO_WORKER]: TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.PENDING_ACTOR_TASK_ARGS_FETCH]:
    TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY]:
    TaskStatus.SUBMITTED_TO_WORKER,
  [TypeTaskStatus.RUNNING]: TaskStatus.RUNNING,
  [TypeTaskStatus.RUNNING_IN_RAY_GET]: TaskStatus.RUNNING,
  [TypeTaskStatus.RUNNING_IN_RAY_WAIT]: TaskStatus.RUNNING,
  [TypeTaskStatus.FINISHED]: TaskStatus.FINISHED,
  [TypeTaskStatus.FAILED]: TaskStatus.FAILED,
  [TypeTaskStatus.NIL]: TaskStatus.UNKNOWN,
};

export const TaskStatusToTaskProgressMapping: Record<
  TaskStatus,
  keyof TaskProgress
> = {
  [TaskStatus.PENDING_ARGS_AVAIL]: "numPendingArgsAvail",
  [TaskStatus.PENDING_NODE_ASSIGNMENT]: "numPendingNodeAssignment",
  [TaskStatus.SUBMITTED_TO_WORKER]: "numSubmittedToWorker",
  [TaskStatus.RUNNING]: "numRunning",
  [TaskStatus.FINISHED]: "numFinished",
  [TaskStatus.FAILED]: "numFailed",
  [TaskStatus.UNKNOWN]: "numUnknown",
};

const useFetchStateApiProgressByTaskName = (
  jobId: string | undefined,
  isRefreshing: boolean,
  setMsg: (msg: string) => void,
  setError: (error: boolean) => void,
  setRefresh: (refresh: boolean) => void,
  disableRefresh: boolean,
  setLatestFetchTimestamp?: (time: number) => void,
) => {
  return useSWR(
    jobId ? ["useJobProgressByTaskName", jobId] : null,
    async ([_, jobId]) => {
      const rsp = await getStateApiJobProgressByTaskName(jobId);
      setMsg(rsp.data.msg);

      if (rsp.data.result) {
        setLatestFetchTimestamp?.(new Date().getTime());
        const summary = formatSummaryToTaskProgress(
          rsp.data.data.result.result,
        );
        return { summary, totalTasks: rsp.data.data.result.num_filtered };
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    {
      refreshInterval:
        isRefreshing && !disableRefresh ? API_REFRESH_INTERVAL_MS : 0,
      revalidateOnFocus: false,
    },
  );
};

/**
 * Hook for fetching a job's task progress.
 * Refetches every 4 seconds unless refresh switch is toggled off.
 *
 * If jobId is undefined, we will not fetch the job progress.
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgress = (
  jobId: string | undefined,
  disableRefresh = false,
) => {
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const [latestFetchTimestamp, setLatestFetchTimestamp] = useState(0);
  const { data, isLoading } = useFetchStateApiProgressByTaskName(
    jobId,
    isRefreshing,
    setMsg,
    setError,
    setRefresh,
    disableRefresh,
    setLatestFetchTimestamp,
  );

  const summed = (data?.summary ?? []).reduce((acc, task) => {
    Object.entries(task.progress).forEach(([k, count]) => {
      const key = k as keyof TaskProgress;
      acc[key] = (acc[key] ?? 0) + count;
    });
    return acc;
  }, {} as TaskProgress);

  const driverExists = !jobId ? false : true;
  return {
    progress: summed,
    totalTasks: data?.totalTasks,
    isLoading,
    msg,
    error,
    driverExists,
    latestFetchTimestamp,
  };
};

/**
 * Hook for fetching a job's task progress grouped by task name.
 * Refetches every 4 seconds unless refresh switch is toggled off.
 *
 * If jobId is not provided, will fetch the task progress across all jobs.
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 */
export const useJobProgressByTaskName = (jobId: string) => {
  const [page, setPage] = useState(1);
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const onSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };

  const { data, isLoading } = useFetchStateApiProgressByTaskName(
    jobId,
    isRefreshing,
    setMsg,
    setError,
    setRefresh,
    false,
  );

  const formattedTasks = (data?.summary ?? []).map((task) => {
    const {
      numFailed = 0,
      numPendingArgsAvail = 0,
      numPendingNodeAssignment = 0,
      numRunning = 0,
      numSubmittedToWorker = 0,
      numFinished = 0,
    } = task.progress;

    const numActive =
      numPendingArgsAvail +
      numPendingNodeAssignment +
      numRunning +
      numSubmittedToWorker;

    return { ...task, numFailed, numActive, numFinished };
  });
  const sortedTasks = _.orderBy(
    formattedTasks,
    ["numFailed", "numActive", "numFinished"],
    ["desc", "desc", "desc"],
  );
  const paginatedTasks = sliceToPage(sortedTasks, page).items;

  return {
    progress: paginatedTasks,
    page: { pageNo: page, pageSize: 10 },
    total: formattedTasks.length,
    totalTasks: data?.totalTasks,
    isLoading,
    setPage,
    msg,
    error,
    onSwitchChange,
  };
};

export const formatStateCountsToProgress = (stateCounts: {
  [stateName: string]: number;
}) => {
  const formattedProgress: TaskProgress = {};
  Object.entries(stateCounts).forEach(([state, count]) => {
    const taskStatus: TaskStatus =
      TASK_STATE_NAME_TO_PROGRESS_KEY[state as TypeTaskStatus];

    const key: keyof TaskProgress =
      TaskStatusToTaskProgressMapping[taskStatus] ?? "numUnknown";

    formattedProgress[key] = (formattedProgress[key] ?? 0) + count;
  });

  return formattedProgress;
};

export const formatSummaryToTaskProgress = (
  summary: StateApiJobProgressByTaskName,
) => {
  const tasks = summary.node_id_to_summary.cluster.summary;
  const formattedTasks = Object.entries(tasks).map(([name, task]) => {
    const formattedProgress = formatStateCountsToProgress(task.state_counts);
    return { name, progress: formattedProgress };
  });

  return formattedTasks;
};

const formatToJobProgressGroup = (
  nestedJobProgress: NestedJobProgress,
  showFinishedTasks = true,
): JobProgressGroup | undefined => {
  const formattedProgress = formatStateCountsToProgress(
    nestedJobProgress.state_counts,
  );

  const total = Object.values(formattedProgress).reduce(
    (acc, count) => acc + count,
    0,
  );
  if (
    !showFinishedTasks &&
    total - (formattedProgress.numFinished ?? 0) === 0
  ) {
    return undefined;
  }

  return {
    name: nestedJobProgress.name,
    key: nestedJobProgress.key,
    progress: formattedProgress,
    children: nestedJobProgress.children
      .map((child) => formatToJobProgressGroup(child, showFinishedTasks))
      .filter((child): child is JobProgressGroup => child !== undefined),
    type: nestedJobProgress.type,
    link: nestedJobProgress.link,
  };
};

export const formatNestedJobProgressToJobProgressGroup = (
  summary: StateApiNestedJobProgress,
  showFinishedTasks = true,
) => {
  const tasks = summary.node_id_to_summary.cluster.summary;
  const progressGroups = tasks
    .map((task) => formatToJobProgressGroup(task, showFinishedTasks))
    .filter((group): group is JobProgressGroup => group !== undefined);

  const total = tasks.reduce<TaskProgress>((acc, group) => {
    const formattedProgress = formatStateCountsToProgress(group.state_counts);
    Object.entries(formattedProgress).forEach(([key, count]) => {
      const progressKey = key as keyof TaskProgress;
      acc[progressKey] = (acc[progressKey] ?? 0) + count;
    });
    return acc;
  }, {});

  return { progressGroups, total };
};

/**
 * Hook for fetching a job's task progress grouped by lineage. This is
 * used for the Advanced progress bar.
 * Refetches every 4 seconds.
 *
 * @param jobId The id of the job whose task progress to fetch or undefined
 *              to fetch all progress for all jobs
 *              If null, we will avoid fetching.
 */
export const useJobProgressByLineage = (
  jobId: string | undefined,
  disableRefresh = false,
  showFinishedTasks = true,
) => {
  const [msg, setMsg] = useState("Loading progress...");
  const [error, setError] = useState(false);
  const [isRefreshing, setRefresh] = useState(true);
  const [latestFetchTimestamp, setLatestFetchTimestamp] = useState(0);

  const { data, isLoading } = useSWR(
    jobId ? ["useJobProgressByLineageAndName", jobId, showFinishedTasks] : null,
    async ([_, jobId, showFinishedTasks]) => {
      const rsp = await getStateApiJobProgressByLineage(jobId);
      setMsg(rsp.data.msg);

      if (rsp.data.result) {
        setLatestFetchTimestamp(new Date().getTime());
        const summary = formatNestedJobProgressToJobProgressGroup(
          rsp.data.data.result.result,
          showFinishedTasks,
        );
        return { summary, totalTasks: rsp.data.data.result.num_filtered };
      } else {
        setError(true);
        setRefresh(false);
      }
    },
    {
      refreshInterval:
        isRefreshing && !disableRefresh ? API_REFRESH_INTERVAL_MS : 0,
      revalidateOnFocus: false,
    },
  );

  return {
    progressGroups: data?.summary?.progressGroups,
    total: data?.summary?.total,
    totalTasks: data?.totalTasks,
    isLoading,
    msg,
    error,
    latestFetchTimestamp,
  };
};

// --- Mock DAG scenarios ---
// Based on REAL Ray task API output from running actual workloads.
// Key finding: Ray Data uses func_or_class_name="_map_task" for all operators,
// but the `name` field has the actual operator name (e.g., "ReadRange", "Map(fn)").
// The aggregation logic uses `name || func_or_class_name` as the grouping key.
// Ray Serve uses long-lived actor replicas — request handling does NOT produce
// task-level entries in the State API, so there's no task DAG for Serve.

const n = (
  name: string,
  state_counts: { [k: string]: number },
): NestedJobProgress => ({
  name,
  key: name,
  type: "NORMAL_TASK" as any,
  state_counts,
  children: [],
});

const MOCK_SCENARIOS: Record<string, DAGSummary> = {
  // --- Ray Data ETL Pipeline ---
  // Real func_or_class_name: all "_map_task"
  // Real name field: "ReadParquet", "Map(parse_json)", "Filter(validate)", etc.
  // Also: "_split_single_block" for repartition, "reduce" for aggregations
  // Internal actors: _StatsActor, AutoscalingRequester (filtered out in real impl)
  "data-pipeline": {
    nodes: [
      n("ReadParquet", { FINISHED: 120000 }),
      n("Map(parse_json)", { FINISHED: 120000 }),
      n("Filter(validate)", { FINISHED: 115000, RUNNING: 200, PENDING_NODE_ASSIGNMENT: 4800 }),
      n("Map(fetch_user_profile)", { FINISHED: 80000, RUNNING: 500, PENDING_ARGS_AVAIL: 39500 }),
      n("Map(fetch_geo_data)", { FINISHED: 95000, RUNNING: 300, PENDING_ARGS_AVAIL: 24700 }),
      n("_split_single_block", { FINISHED: 60000, RUNNING: 150, PENDING_ARGS_AVAIL: 59850 }),
      n("MapBatches(deduplicate)", { FINISHED: 40000, PENDING_ARGS_AVAIL: 80000 }),
      n("Map(write_parquet)", { FINISHED: 25000, PENDING_ARGS_AVAIL: 95000 }),
    ],
    actors: [
      {
        name: "_StatsActor",
        key: "actor:_StatsActor",
        type: "ACTOR" as any,
        state_counts: { ALIVE: 1 },
        children: [],
      },
    ],
    edges: [
      { source: "ReadParquet", target: "Map(parse_json)" },
      { source: "Map(parse_json)", target: "Filter(validate)" },
      { source: "Filter(validate)", target: "Map(fetch_user_profile)" },
      { source: "Filter(validate)", target: "Map(fetch_geo_data)" },
      { source: "Map(fetch_user_profile)", target: "_split_single_block" },
      { source: "Map(fetch_geo_data)", target: "_split_single_block" },
      { source: "_split_single_block", target: "MapBatches(deduplicate)" },
      { source: "MapBatches(deduplicate)", target: "Map(write_parquet)" },
    ],
  },

  // --- Ray Core Training Pipeline ---
  // Real func_or_class_name matches name for @ray.remote functions.
  // Verified: load_data, preprocess, train_step, validate, save_checkpoint
  training: {
    nodes: [
      n("load_data", { FINISHED: 8 }),
      n("preprocess", { FINISHED: 8 }),
      n("train_step", { FINISHED: 20, RUNNING: 4, FAILED: 1, PENDING_ARGS_AVAIL: 3 }),
      n("validate", { FINISHED: 2, RUNNING: 1 }),
      n("save_checkpoint", { FINISHED: 2, PENDING_ARGS_AVAIL: 1 }),
    ],
    actors: [],
    edges: [
      { source: "load_data", target: "preprocess" },
      { source: "preprocess", target: "train_step" },
      { source: "train_step", target: "validate" },
      { source: "validate", target: "save_checkpoint" },
    ],
  },

  // --- Evaluation Pipeline ---
  // Real func_or_class_name: load_model, load_dataset, run_inference,
  // compute_accuracy, compute_f1, compute_latency, aggregate_metrics, generate_report
  // Verified matches exactly.
  eval: {
    nodes: [
      n("load_model", { FINISHED: 1 }),
      n("load_dataset", { FINISHED: 2 }),
      n("run_inference", { FINISHED: 4200, RUNNING: 80, PENDING_ARGS_AVAIL: 720 }),
      n("compute_accuracy", { FINISHED: 1, PENDING_ARGS_AVAIL: 0 }),
      n("compute_latency", { FINISHED: 1 }),
      n("compute_f1", { FINISHED: 1, PENDING_ARGS_AVAIL: 0 }),
      n("aggregate_metrics", { PENDING_ARGS_AVAIL: 1 }),
      n("generate_report", { PENDING_ARGS_AVAIL: 1 }),
    ],
    actors: [],
    edges: [
      { source: "load_model", target: "run_inference" },
      { source: "load_dataset", target: "run_inference" },
      { source: "run_inference", target: "compute_accuracy" },
      { source: "run_inference", target: "compute_latency" },
      { source: "run_inference", target: "compute_f1" },
      { source: "load_dataset", target: "compute_accuracy" },
      { source: "load_dataset", target: "compute_f1" },
      { source: "compute_accuracy", target: "aggregate_metrics" },
      { source: "compute_latency", target: "aggregate_metrics" },
      { source: "compute_f1", target: "aggregate_metrics" },
      { source: "aggregate_metrics", target: "generate_report" },
    ],
  },
};

// Default: Ray Data pipeline (most common use case)
const DEFAULT_MOCK: DAGSummary = MOCK_SCENARIOS["data-pipeline"];

/**
 * Hook for fetching a job's task progress as a dataflow DAG.
 * Currently returns mock data for testing.
 * Add ?dag=<scenario> to URL to switch DAG mock:
 *   ?dag=data-pipeline  → Ray Data ETL (real operator names from task.name)
 *   ?dag=training       → Ray Core training pipeline (real func names)
 *   ?dag=eval           → Ray Core evaluation pipeline (real func names)
 *   (default)           → Ray Data pipeline
 *
 * Note: Ray Serve does NOT produce task-level DAG entries — request handling
 * uses long-lived actor replicas that don't appear in the task State API.
 * TODO: Remove mock data and use real API once backend is deployed.
 */
export const useJobProgressByDataflow = (
  jobId: string | undefined,
  enabled = true,
) => {
  // Read ?dag= param from URL for mock scenario switching
  const params = new URLSearchParams(window.location.search);
  const scenario = params.get("dag") ?? "";

  const mockSummary: DAGSummary | undefined =
    jobId && enabled
      ? MOCK_SCENARIOS[scenario] ?? DEFAULT_MOCK
      : undefined;

  return {
    dagSummary: mockSummary,
    isLoading: false,
    msg: "Mock data loaded",
    error: false,
  };
};
