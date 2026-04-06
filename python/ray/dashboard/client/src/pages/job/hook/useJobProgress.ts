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
// Navigate to different job IDs to see different DAG patterns:
//   #/jobs/data-pipeline   → Big Data Processing (ETL)
//   #/jobs/training        → Model Training (Ray Train + Data)
//   #/jobs/serving         → Model Serving (Ray Serve deployment graph)
//   #/jobs/eval            → Evaluation Pipeline
//   #/jobs/<anything-else> → Default training pipeline

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
  // --- Big Data Processing (ETL) ---
  "data-pipeline": {
    nodes: [
      n("ReadCSV", { FINISHED: 120000 }),
      n("FilterInvalid", { FINISHED: 120000 }),
      n("ParseJSON", { FINISHED: 115000, RUNNING: 200, PENDING_NODE_ASSIGNMENT: 4800 }),
      n("FetchUserProfile", { FINISHED: 80000, RUNNING: 500, PENDING_ARGS_AVAIL: 39500 }),
      n("FetchGeoData", { FINISHED: 95000, RUNNING: 300, PENDING_ARGS_AVAIL: 24700 }),
      n("JoinFeatures", { FINISHED: 60000, RUNNING: 150, PENDING_ARGS_AVAIL: 59850 }),
      n("Deduplicate", { FINISHED: 40000, PENDING_ARGS_AVAIL: 80000 }),
      n("WriteParquet", { FINISHED: 25000, PENDING_ARGS_AVAIL: 95000 }),
    ],
    actors: [],
    edges: [
      { source: "ReadCSV", target: "FilterInvalid" },
      { source: "FilterInvalid", target: "ParseJSON" },
      { source: "ParseJSON", target: "FetchUserProfile" },
      { source: "ParseJSON", target: "FetchGeoData" },
      { source: "FetchUserProfile", target: "JoinFeatures" },
      { source: "FetchGeoData", target: "JoinFeatures" },
      { source: "JoinFeatures", target: "Deduplicate" },
      { source: "Deduplicate", target: "WriteParquet" },
    ],
  },

  // --- Model Training (Ray Train + Data) ---
  training: {
    nodes: [
      n("ReadParquet", { FINISHED: 50000 }),
      n("Tokenize", { FINISHED: 48000, RUNNING: 120, PENDING_ARGS_AVAIL: 1880 }),
      n("Augment", { FINISHED: 45000, RUNNING: 200, PENDING_ARGS_AVAIL: 4800 }),
      n("ShuffleAndBatch", { FINISHED: 40000, RUNNING: 80, PENDING_ARGS_AVAIL: 9920 }),
      n("LoadCheckpoint", { FINISHED: 1 }),
      n("TrainStep", { FINISHED: 8500, RUNNING: 64, FAILED: 3, PENDING_ARGS_AVAIL: 1433 }),
      n("ValidateEpoch", { FINISHED: 17, RUNNING: 1, PENDING_ARGS_AVAIL: 2 }),
      n("SaveCheckpoint", { FINISHED: 17, PENDING_ARGS_AVAIL: 3 }),
    ],
    actors: [
      {
        name: "TorchTrainer",
        key: "actor:TorchTrainer",
        type: "ACTOR" as any,
        state_counts: { ALIVE: 8 },
        children: [],
      },
      {
        name: "DataWorker",
        key: "actor:DataWorker",
        type: "ACTOR" as any,
        state_counts: { ALIVE: 4 },
        children: [],
      },
    ],
    edges: [
      { source: "ReadParquet", target: "Tokenize" },
      { source: "Tokenize", target: "Augment" },
      { source: "Augment", target: "ShuffleAndBatch" },
      { source: "ShuffleAndBatch", target: "TrainStep" },
      { source: "LoadCheckpoint", target: "TrainStep" },
      { source: "TrainStep", target: "ValidateEpoch" },
      { source: "ValidateEpoch", target: "SaveCheckpoint" },
    ],
  },

  // --- Model Serving (Ray Serve deployment graph) ---
  serving: {
    nodes: [
      n("HTTPIngress", { FINISHED: 285000, RUNNING: 120 }),
      n("Preprocess", { FINISHED: 284500, RUNNING: 95, PENDING_ARGS_AVAIL: 25 }),
      n("Tokenizer", { FINISHED: 284000, RUNNING: 80, PENDING_ARGS_AVAIL: 40 }),
      n("EmbeddingModel", { FINISHED: 280000, RUNNING: 60, PENDING_NODE_ASSIGNMENT: 500, PENDING_ARGS_AVAIL: 3560 }),
      n("RetrievalIndex", { FINISHED: 280000, RUNNING: 40, PENDING_ARGS_AVAIL: 4080 }),
      n("LLMGenerate", { FINISHED: 260000, RUNNING: 48, FAILED: 150, PENDING_ARGS_AVAIL: 23922 }),
      n("Guardrails", { FINISHED: 259000, RUNNING: 30, PENDING_ARGS_AVAIL: 25090 }),
      n("ResponseFormatter", { FINISHED: 258000, RUNNING: 25, PENDING_ARGS_AVAIL: 26095 }),
    ],
    actors: [
      {
        name: "LLMReplica",
        key: "actor:LLMReplica",
        type: "ACTOR" as any,
        state_counts: { ALIVE: 16 },
        children: [],
      },
      {
        name: "EmbeddingReplica",
        key: "actor:EmbeddingReplica",
        type: "ACTOR" as any,
        state_counts: { ALIVE: 4 },
        children: [],
      },
    ],
    edges: [
      { source: "HTTPIngress", target: "Preprocess" },
      { source: "Preprocess", target: "Tokenizer" },
      { source: "Tokenizer", target: "EmbeddingModel" },
      { source: "Tokenizer", target: "LLMGenerate" },
      { source: "EmbeddingModel", target: "RetrievalIndex" },
      { source: "RetrievalIndex", target: "LLMGenerate" },
      { source: "LLMGenerate", target: "Guardrails" },
      { source: "Guardrails", target: "ResponseFormatter" },
    ],
  },

  // --- Evaluation Pipeline ---
  eval: {
    nodes: [
      n("LoadModel", { FINISHED: 1 }),
      n("LoadTestDataset", { FINISHED: 5000 }),
      n("RunInference", { FINISHED: 4200, RUNNING: 80, PENDING_ARGS_AVAIL: 720 }),
      n("LoadGoldenLabels", { FINISHED: 5000 }),
      n("ComputeAccuracy", { FINISHED: 3800, PENDING_ARGS_AVAIL: 1200 }),
      n("ComputeLatencyStats", { FINISHED: 4200 }),
      n("ComputeF1Score", { FINISHED: 3800, PENDING_ARGS_AVAIL: 1200 }),
      n("AggregateMetrics", { PENDING_ARGS_AVAIL: 1 }),
      n("GenerateReport", { PENDING_ARGS_AVAIL: 1 }),
    ],
    actors: [],
    edges: [
      { source: "LoadModel", target: "RunInference" },
      { source: "LoadTestDataset", target: "RunInference" },
      { source: "RunInference", target: "ComputeAccuracy" },
      { source: "RunInference", target: "ComputeLatencyStats" },
      { source: "RunInference", target: "ComputeF1Score" },
      { source: "LoadGoldenLabels", target: "ComputeAccuracy" },
      { source: "LoadGoldenLabels", target: "ComputeF1Score" },
      { source: "ComputeAccuracy", target: "AggregateMetrics" },
      { source: "ComputeLatencyStats", target: "AggregateMetrics" },
      { source: "ComputeF1Score", target: "AggregateMetrics" },
      { source: "AggregateMetrics", target: "GenerateReport" },
    ],
  },
};

// Default: training pipeline (same as before)
const DEFAULT_MOCK: DAGSummary = {
  nodes: [
    n("read_parquet", { FINISHED: 50000 }),
    n("preprocess", { FINISHED: 36000, RUNNING: 80, PENDING_ARGS_AVAIL: 13920 }),
    n("train_batch", { FINISHED: 1500, RUNNING: 80, FAILED: 12, PENDING_ARGS_AVAIL: 3408 }),
    n("save_model", { PENDING_ARGS_AVAIL: 1 }),
    n("load_weights", { FINISHED: 1 }),
  ],
  actors: [
    {
      name: "TrainWorker",
      key: "actor:TrainWorker",
      type: "ACTOR" as any,
      state_counts: { ALIVE: 8 },
      children: [],
    },
  ],
  edges: [
    { source: "read_parquet", target: "preprocess" },
    { source: "preprocess", target: "train_batch" },
    { source: "load_weights", target: "train_batch" },
    { source: "train_batch", target: "save_model" },
  ],
};

/**
 * Hook for fetching a job's task progress as a dataflow DAG.
 * Currently returns mock data for testing.
 * Add ?dag=<scenario> to URL to switch DAG mock:
 *   ?dag=data-pipeline  → Big Data Processing (ETL)
 *   ?dag=training       → Model Training (Ray Train + Data)
 *   ?dag=serving        → Model Serving (Ray Serve deployment graph)
 *   ?dag=eval           → Evaluation Pipeline
 *   (default)           → Simple training pipeline
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
