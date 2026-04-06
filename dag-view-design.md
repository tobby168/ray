# Ray Dashboard DAG View Design

## 現狀分析

### 現有的聚合能力（Ray Core Overview）

Job Detail 頁面的 Ray Core Overview 已經有兩種聚合模式：

| 模式 | API | 做了什麼 | 缺什麼 |
|------|-----|---------|--------|
| `summary_by=func_name` | `GET /api/v0/tasks/summarize` | 按 `func_or_class_name` 分組，計算每組的 state counts | 沒有任何關係 |
| `summary_by=lineage` | `GET /api/v0/tasks/summarize?summary_by=lineage` | 用 `parent_task_id` 建樹，同名 siblings merge 成 GROUP | **是 submit 層級，不是資料流** |

### `lineage` 模式的問題

`parent_task_id` 代表「是誰 submit 了這個 task」，不是「這個 task 的 input 來自誰」：

```python
@ray.remote
def driver():
    a = read.remote()           # parent = driver
    b = preprocess.remote(a)    # parent = driver (不是 read!)
    c = train.remote(b)         # parent = driver (不是 preprocess!)
```

現有的 lineage 樹：
```
driver
├── [GROUP: read]         (x50000)  ■■■■■■■■■■ 50K/50K
├── [GROUP: preprocess]   (x50000)  ■■■■■■■□□□ 36K/50K
└── [GROUP: train_batch]  (x5000)   ■■■□□□□□□□ 1.5K/5K
```

全部都是 driver 的 children，互相之間**沒有邊**。
用戶看到的是一個 flat list with progress bars，無法得知 read → preprocess → train 的資料流關係。

### 已有的資料 vs 缺的資料

| 資料 | 在哪 | State API | Dashboard | 用途 |
|------|------|-----------|-----------|------|
| `parent_task_id` | TaskInfoEntry (proto field 8) | ✅ 有 | ✅ lineage 模式用 | submit 層級（不是資料流） |
| `func_or_class_name` | TaskInfoEntry (proto field 4) | ✅ 有 | ✅ 聚合 key | Node 名稱 |
| `state` (state_counts) | TaskStateUpdate | ✅ 有 | ✅ progress bar | 進度統計 |
| `actor_id` / `actorClass` | TaskInfoEntry / ActorDetail | ✅ 有 | ✅ Actor 分組 | Actor grouping |
| **`args[].object_ref`** | **TaskSpec (C++ only)** | **❌ 沒有** | **❌** | **資料流邊** |
| **return object IDs** | **TaskSpec (C++ only)** | **❌ 沒有** | **❌** | **資料流邊** |

### 現有程式碼路徑

```
TaskSpec (C++ 完整 task 描述，含 args/ObjectRef)
    ↓ FillTaskInfo()  ← 這裡只抄了部分欄位，沒有抄 args
TaskInfoEntry (proto，送到 GCS 的精簡版)
    ↓ task_event_buffer.cc 上報到 GCS
TaskEvents (GCS 儲存)
    ↓ state_aggregator.py 查詢
TaskState (Python dict)
    ↓ to_summary_by_lineage() 聚合
NestedTaskSummary (前端 tree 結構)
    ↓ AdvancedProgressBar 渲染
前端 UI
```

**關鍵檔案：**
- Protobuf: `src/ray/protobuf/common.proto` — `TaskInfoEntry` (line 627)
- C++ 填充: `src/ray/common/protobuf_utils.cc` — `FillTaskInfo()` (line 191)
- C++ 依賴提取: `src/ray/common/task/task_spec.cc` — `GetDependencyIds()` (line 353)
- Python 聚合: `python/ray/util/state/common.py` — `to_summary_by_lineage()` (line 1078)
- Python state: `python/ray/util/state/common.py` — `TaskState` dataclass (line 733)
- Python endpoint: `python/ray/dashboard/state_aggregator.py` — `summarize_tasks()` (line 571)
- 前端 type: `python/ray/dashboard/client/src/type/job.ts` — `NestedJobProgress` (line 156)
- 前端 hook: `python/ray/dashboard/client/src/pages/job/hook/useJobProgress.ts`
- 前端渲染: `python/ray/dashboard/client/src/pages/job/AdvancedProgressBar/AdvancedProgressBar.tsx`

---

## 設計方案：新增 `summary_by=dataflow` 模式

在現有架構上加一個新的聚合模式，複用 `NestedTaskSummary` + `AdvancedProgressBar` 的基礎設施，
但把 tree 結構換成 DAG 結構，邊從「誰 submit 的」換成「誰的 output 是我的 input」。

### 改動範圍一覽

```
Layer           File                                          Change
─────────────── ───────────────────────────────────────────── ──────────────────────
Proto           src/ray/protobuf/common.proto                 TaskInfoEntry +2 fields
C++             src/ray/common/protobuf_utils.cc              FillTaskInfo() +10 lines
Python state    python/ray/util/state/common.py               TaskState +2 fields
Python convert  python/ray/util/state/common.py               protobuf_to_task_state_dict() +4 lines
Python agg      python/ray/util/state/common.py               +to_summary_by_dataflow() ~120 lines
Python endpoint python/ray/dashboard/state_aggregator.py      summarize_tasks() +5 lines
Frontend type   .../client/src/type/job.ts                     +DAG edge type
Frontend hook   .../client/src/pages/job/hook/useJobProgress.ts  +useJobProgressByDataflow()
Frontend view   .../client/src/pages/job/DAGProgressBar/      new component ~400 lines
```

---

### Step 1: 擴展 TaskInfoEntry protobuf

```protobuf
// src/ray/protobuf/common.proto, inside message TaskInfoEntry

  // ObjectIDs this task depends on (extracted from pass-by-ref args).
  // Used to build dataflow DAG edges between task groups.
  repeated bytes dependency_object_ids = 30;

  // ObjectIDs this task produces (return values).
  // Used to build dataflow DAG edges between task groups.
  repeated bytes return_object_ids = 31;
```

為什麼用 30/31: TaskInfoEntry 現有最大 field number 是 29，留出空間避免衝突。

### Step 2: FillTaskInfo() 填入依賴

```cpp
// src/ray/common/protobuf_utils.cc, inside FillTaskInfo(), append at end (~line 238)

  // Populate dependency and return object IDs for dataflow DAG.
  for (const auto &dep_id : task_spec.GetDependencyIds()) {
    task_info->add_dependency_object_ids(dep_id.Binary());
  }
  for (size_t i = 0; i < task_spec.NumReturns(); i++) {
    task_info->add_return_object_ids(task_spec.ReturnId(i).Binary());
  }
```

**性能考量：**
- `GetDependencyIds()` 已經存在，只是遍歷 args 收集 by-ref 的 ObjectID，O(num_args)
- 每個 task 平均 1-5 個 args，overhead 極小
- 這些 bytes 會增加 TaskEvents 的大小，但每個 ObjectID 只有 28 bytes
- 對於百萬級 task 的 job，額外記憶體 ≈ 1M tasks × 3 deps × 28 bytes ≈ 84MB（可接受）

### Step 3: 擴展 Python TaskState

```python
# python/ray/util/state/common.py, inside TaskState dataclass

    #: ObjectIDs this task depends on (pass-by-ref arguments).
    dependency_object_ids: Optional[List[str]] = state_column(
        detail=True, filterable=False
    )
    #: ObjectIDs this task returns.
    return_object_ids: Optional[List[str]] = state_column(
        detail=True, filterable=False
    )
```

```python
# python/ray/util/state/common.py, inside protobuf_to_task_state_dict()
# Add to the task_info extraction section:

    if task_info.dependency_object_ids:
        task_state["dependency_object_ids"] = [
            dep_id.hex() for dep_id in task_info.dependency_object_ids
        ]
    if task_info.return_object_ids:
        task_state["return_object_ids"] = [
            ret_id.hex() for ret_id in task_info.return_object_ids
        ]
```

### Step 4: 新增 `to_summary_by_dataflow()` 聚合方法

```python
# python/ray/util/state/common.py, new classmethod on TaskSummaries

@dataclass
class DataflowDAGSummary:
    """DAG structure where nodes are task groups and edges are ObjectRef dependencies."""
    #: Nodes grouped by func_or_class_name, each with state_counts
    nodes: List[NestedTaskSummary]
    #: Edges derived from ObjectRef producer→consumer relationships
    edges: List[Dict[str, str]]  # [{"from": "read", "to": "preprocess"}, ...]
    #: Total counts
    total_tasks: int
    total_actor_tasks: int
    total_actor_scheduled: int
    summary_by: str = "dataflow"


@classmethod
def to_summary_by_dataflow(
    cls, *, tasks: List[Dict], actors: List[Dict]
) -> "TaskSummaries":
    """
    Summarize tasks as a dataflow DAG.

    Unlike lineage (parent_task_id = who submitted), dataflow uses ObjectRef
    dependencies (who produced the data I consume) to build edges.

    Algorithm:
    1. Build object_id → producer_func_name mapping from return_object_ids
    2. Build consumer edges from dependency_object_ids
    3. Group tasks by func_or_class_name with state counts
    4. Deduplicate edges at the group level
    """
    # --- Build object → producer mapping ---
    object_to_producer: Dict[str, str] = {}
    for task in tasks:
        func_name = task["name"] or task["func_or_class_name"]
        for obj_id in (task.get("return_object_ids") or []):
            object_to_producer[obj_id] = func_name

    # --- Build group-level edges ---
    edges: Set[Tuple[str, str]] = set()
    for task in tasks:
        consumer = task["name"] or task["func_or_class_name"]
        for obj_id in (task.get("dependency_object_ids") or []):
            producer = object_to_producer.get(obj_id)
            if producer and producer != consumer:
                edges.add((producer, consumer))

    # --- Group by func name with state counts ---
    nodes: Dict[str, NestedTaskSummary] = {}
    total_tasks = 0
    total_actor_tasks = 0
    total_actor_scheduled = 0

    for task in tasks:
        func_name = task["name"] or task["func_or_class_name"]
        if func_name not in nodes:
            nodes[func_name] = NestedTaskSummary(
                name=func_name,
                key=func_name,
                type=task["type"],
                timestamp=task.get("creation_time_ms"),
            )
        node = nodes[func_name]

        state = task["state"]
        node.state_counts[state] = node.state_counts.get(state, 0) + 1

        # Update timestamp to earliest
        task_ts = task.get("creation_time_ms")
        if task_ts and (node.timestamp is None or task_ts < node.timestamp):
            node.timestamp = task_ts

        type_enum = TaskType.DESCRIPTOR.values_by_name[task["type"]].number
        if type_enum == TaskType.NORMAL_TASK:
            total_tasks += 1
        elif type_enum == TaskType.ACTOR_CREATION_TASK:
            total_actor_scheduled += 1
        elif type_enum == TaskType.ACTOR_TASK:
            total_actor_tasks += 1

    # --- Add actor nodes ---
    actor_groups: Dict[str, NestedTaskSummary] = {}
    for actor in actors:
        class_name = actor.get("repr_name") or actor.get("class_name", "UnknownActor")
        if class_name not in actor_groups:
            actor_groups[class_name] = NestedTaskSummary(
                name=class_name,
                key=f"actor:{class_name}",
                type="ACTOR",
            )
        ag = actor_groups[class_name]
        actor_state = actor.get("state", "UNKNOWN")
        ag.state_counts[actor_state] = ag.state_counts.get(actor_state, 0) + 1

    # --- Topological sort nodes by edges ---
    sorted_nodes = _topological_sort(list(nodes.values()), edges)

    return TaskSummaries(
        summary={
            "nodes": sorted_nodes,
            "actors": list(actor_groups.values()),
            "edges": [{"source": e[0], "target": e[1]} for e in edges],
        },
        total_tasks=total_tasks,
        total_actor_tasks=total_actor_tasks,
        total_actor_scheduled=total_actor_scheduled,
        summary_by="dataflow",
    )


def _topological_sort(
    nodes: List[NestedTaskSummary],
    edges: Set[Tuple[str, str]],
) -> List[NestedTaskSummary]:
    """Sort nodes so upstream nodes come before downstream nodes."""
    in_degree = {n.name: 0 for n in nodes}
    adj = {n.name: [] for n in nodes}
    for src, dst in edges:
        if src in adj and dst in in_degree:
            adj[src].append(dst)
            in_degree[dst] = in_degree.get(dst, 0) + 1

    queue = [name for name, deg in in_degree.items() if deg == 0]
    order = []
    while queue:
        # Sort by timestamp within same level for determinism
        queue.sort(key=lambda n: next(
            (node.timestamp or 0) for node in nodes if node.name == n
        ))
        name = queue.pop(0)
        order.append(name)
        for neighbor in adj.get(name, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Append any remaining nodes (cycles or disconnected)
    remaining = [n.name for n in nodes if n.name not in order]
    order.extend(remaining)

    node_map = {n.name: n for n in nodes}
    return [node_map[name] for name in order if name in node_map]
```

### Step 5: Endpoint 加 `summary_by=dataflow`

```python
# python/ray/dashboard/state_aggregator.py, inside summarize_tasks()

async def summarize_tasks(self, option: SummaryApiOptions) -> SummaryApiResponse:
    summary_by = option.summary_by or "func_name"
    if summary_by not in ["func_name", "lineage", "dataflow"]:  # ← 加 "dataflow"
        raise ValueError(...)

    result = await self.list_tasks(
        option=ListApiOptions(
            timeout=option.timeout,
            limit=RAY_MAX_LIMIT_FROM_API_SERVER,
            filters=option.filters,
            detail=summary_by in ("lineage", "dataflow"),  # ← dataflow 也需要 detail
        )
    )

    if summary_by == "func_name":
        summary_results = TaskSummaries.to_summary_by_func_name(tasks=result.result)
    elif summary_by == "lineage":
        actors = await self.list_actors(...)
        summary_results = TaskSummaries.to_summary_by_lineage(...)
    else:  # dataflow
        actors = await self.list_actors(...)
        summary_results = TaskSummaries.to_summary_by_dataflow(
            tasks=result.result, actors=actors.result
        )
    ...
```

### Step 6: 前端 — 新增 DAG 視覺化

#### API Response 格式

```
GET /api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values={jobId}&summary_by=dataflow
```

```json
{
  "result": {
    "node_id_to_summary": {
      "cluster": {
        "summary": {
          "nodes": [
            {
              "name": "read_parquet",
              "key": "read_parquet",
              "type": "NORMAL_TASK",
              "state_counts": {"FINISHED": 50000},
              "children": []
            },
            {
              "name": "preprocess",
              "key": "preprocess",
              "type": "NORMAL_TASK",
              "state_counts": {"FINISHED": 36000, "RUNNING": 80, "PENDING_ARGS_AVAIL": 13920},
              "children": []
            },
            {
              "name": "train_batch",
              "key": "train_batch",
              "type": "NORMAL_TASK",
              "state_counts": {"FINISHED": 1500, "RUNNING": 80, "FAILED": 12, "PENDING_ARGS_AVAIL": 3408},
              "children": []
            }
          ],
          "actors": [
            {
              "name": "TrainWorker",
              "key": "actor:TrainWorker",
              "type": "ACTOR",
              "state_counts": {"ALIVE": 8}
            }
          ],
          "edges": [
            {"source": "read_parquet", "target": "preprocess"},
            {"source": "preprocess", "target": "train_batch"}
          ]
        },
        "summary_by": "dataflow",
        "total_tasks": 55000
      }
    }
  }
}
```

#### 前端元件結構

```
JobDetail.tsx
  └── JobProgressBar.tsx
      ├── TaskProgressBar.tsx           (現有: 總 progress bar)
      ├── AdvancedProgressBar.tsx        (現有: lineage tree view)
      └── DAGProgressBar.tsx            (新增: dataflow DAG view)
          ├── DAGCanvas.tsx             (用 reactflow 畫 DAG)
          ├── DAGNode.tsx               (每個 node: 名稱 + mini progress bar)
          └── DAGNodeDetail.tsx          (點擊展開: state 分布、失敗 task、actor 關聯)
```

#### 前端視覺化方案選型

**現狀**: Dashboard 目前沒有任何圖表/視覺化 library。所有 UI 都是 MUI Table + CSS flexbox。
引入 DAG 視覺化等於加入第一個視覺化依賴，需要慎重選擇。

**方案比較:**

| 方案 | Bundle Size | 學習成本 | 功能 | 跟 MUI 整合 |
|------|------------|---------|------|-------------|
| **A. 純 SVG + dagre** | dagre ~30KB | 低（手寫 SVG） | 夠用，但要自己做互動 | 完美（直接用 MUI theme） |
| **B. ReactFlow + dagre** | ~150KB (gzipped) | 中 | 完整（zoom, pan, 互動） | 好（支援 custom node） |
| **C. D3** | ~80KB | 高（跟 React 衝突） | 過度 | 差（自己管 DOM） |
| **D. MUI Table 模擬** | 0KB | 低 | 有限（只能做 tree） | 完美 |

**選擇: 方案 B — ReactFlow + dagre**

理由：
1. DAG 可能有十幾個 node（Ray Data pipeline、複雜的 Core task graph），需要 zoom/pan
2. 純 SVG 要自己解決的問題太多：zoom/pan（含 touch）、edge routing（避免交叉）、
   resize reflow、animated edges、drag-to-rearrange、minimap 導航
3. ReactFlow 全部內建，且支援 custom node（可以放 MUI 元件）
4. 150KB gzipped 換上述所有功能是合理的 trade-off
5. dagre 負責 layout 算法（自動排列），ReactFlow 負責渲染和互動
6. Dashboard 是第一次引入視覺化 library，ReactFlow 作為 React-first 的方案，
   比 D3 更符合現有的 React + MUI 架構

```
依賴關係：
dagre (layout) → 算出每個 node 的 x/y 座標
                      ↓
ReactFlow (render) → 渲染 nodes + edges + zoom/pan/minimap
                      ↓
MUI components → custom node 內容 (Paper + MiniTaskProgressBar)
```

```typescript
// DAGProgressBar.tsx — ReactFlow + dagre layout
import ReactFlow, {
  Node, Edge, Background, Controls, MiniMap,
  useNodesState, useEdgesState,
} from 'reactflow';
import 'reactflow/dist/style.css';
import dagre from 'dagre';

const DAG_NODE_WIDTH = 220;
const DAG_NODE_HEIGHT = 90;

// dagre 計算 layout，ReactFlow 負責渲染
const computeLayout = (summary: DAGSummary): { nodes: Node[]; edges: Edge[] } => {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: 'LR', ranksep: 100, nodesep: 50 });

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
        type: 'dagNode',              // ← custom node type
        position: { x: x - DAG_NODE_WIDTH / 2, y: y - DAG_NODE_HEIGHT / 2 },
        data: { summary: node },
      };
    }),
    edges: summary.edges.map((edge) => ({
      id: `${edge.source}-${edge.target}`,
      source: edge.source,
      target: edge.target,
      animated: true,                 // ← animated dash = data flowing
      style: { strokeWidth: 2 },
      markerEnd: { type: 'arrowclosed' },
    })),
  };
};

// 註冊 custom node type
const nodeTypes = { dagNode: DAGNodeComponent };

const DAGProgressBar = ({ summary }: { summary: DAGSummary }) => {
  const { nodes: initialNodes, edges: initialEdges } = computeLayout(summary);
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // summary 更新時（polling 4s），重新計算 layout
  useEffect(() => {
    const { nodes, edges } = computeLayout(summary);
    setNodes(nodes);
    setEdges(edges);
  }, [summary]);

  return (
    <Box sx={{ height: 400 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView                       // ← 自動 fit 到 viewport
        attributionPosition="bottom-left"
      >
        <Background />
        <Controls />                  {/* ← zoom in/out/fit 按鈕 */}
        <MiniMap />                   {/* ← 大 DAG 的導航小地圖 */}
      </ReactFlow>
    </Box>
  );
};
```

```typescript
// DAGNode.tsx — ReactFlow custom node，內容用 MUI 元件
import { Handle, Position } from 'reactflow';

const DAGNodeComponent = ({ data: { summary } }: { data: { summary: NestedJobProgress } }) => {
  const theme = useTheme();
  const progress = formatStateCountsToProgress(summary.state_counts);
  const total = Object.values(progress).reduce((a, b) => a + b, 0);
  const finished = progress.numFinished ?? 0;
  const failed = progress.numFailed ?? 0;

  // 瓶頸偵測 → 邊框顏色
  const bottleneck = detectBottleneck(summary);
  const borderColor = bottleneckColors[bottleneck](theme);

  return (
    <>
      {/* ReactFlow 的 Handle = edge 連接點 */}
      <Handle type="target" position={Position.Left} />

      <Paper
        variant="outlined"
        sx={{
          p: 1.5,
          borderColor,
          borderWidth: 2,
          width: DAG_NODE_WIDTH,
          cursor: 'pointer',
          '&:hover': { boxShadow: 2 },
        }}
      >
        <Typography variant="body2" fontWeight={600} noWrap>
          {summary.name}
        </Typography>
        <MiniTaskProgressBar {...progress} showTotal />
        <Typography variant="caption" color="text.secondary">
          {finished.toLocaleString()}/{total.toLocaleString()}
          {failed > 0 && (
            <Typography component="span" variant="caption" color="error.main">
              {' '}({failed} failed)
            </Typography>
          )}
        </Typography>
      </Paper>

      <Handle type="source" position={Position.Right} />
    </>
  );
};

// 瓶頸顏色用 MUI theme palette，跟 dashboard 一致
const bottleneckColors: Record<BottleneckType, (theme: Theme) => string> = {
  failing: (t) => t.palette.error.main,
  slow:    (t) => t.palette.warning.main,
  blocked: (t) => t.palette.info.main,
  healthy: (t) => t.palette.grey[300],
  done:    (t) => t.palette.success.main,
};
```

**ReactFlow 帶來的內建功能（不需要自己實作）：**
- Zoom/pan（含 trackpad pinch、touch 支援）
- MiniMap（大 DAG 的鳥瞰導航）
- Controls（zoom in/out/fit-view 按鈕）
- Animated edges（虛線流動動畫，表示 data flowing）
- Edge routing（自動避免 node-edge 交叉）
- fitView（自動縮放到剛好顯示整個 DAG）
- Node drag（用戶可以手動調整 node 位置）
- 暗色模式支援（跟 MUI theme 整合）

**Custom node 的好處：**
Node 內容完全是 MUI 元件（Paper, Typography, MiniTaskProgressBar），
跟 dashboard 其他地方的風格完全一致，不會有視覺斷裂感。

---

## 最終展示效果

### Default View: Job Detail 頁面全貌

```
┌──────────────────────────────────────────────────────────────────────┐
│  Job: train_pipeline                                Running 15m 20s │
│                                                                      │
│  Ray Core Overview                                                   │
│  ■■■■■■■■■■■■■■■■■■□□□□□□□□□□□□  55K/110K tasks                    │
│  Total: 110,000  Finished: 55,000  Running: 168  Failed: 12         │
│                                                                      │
│  ▼ Dataflow DAG ─────────────────────────────────────────────────    │
│  │                                                               │   │
│  │  ╭──────────────╮       ╭──────────────╮                      │   │
│  │  │ read_parquet  │─────▷│ preprocess   │────┐                 │   │
│  │  │ ■■■■■■■■■■   │       │ ■■■■■■■□□□  │    │                 │   │
│  │  │ 50K/50K  ✓   │       │ 36K/50K     │    │                 │   │
│  │  ╰──────────────╯       ╰──────────────╯    │                 │   │
│  │                                              ▽                │   │
│  │  ╭──────────────╮       ╭━━━━━━━━━━━━━━╮                      │   │
│  │  │ load_weights  │─────▷┃ train_batch  ┃  ← 紅色邊框          │   │
│  │  │ ■■■■■■■■■■   │       ┃ ■■■□□□□□□□  ┃                      │   │
│  │  │ 1/1  ✓       │       ┃ 1.5K/5K     ┃                      │   │
│  │  ╰──────────────╯       ╰━━━━━┯━━━━━━━━╯                      │   │
│  │                                │                               │   │
│  │                                ▽                               │   │
│  │                         ╭──────────────╮                      │   │
│  │                         │ save_model   │  ← 藍色邊框          │   │
│  │                         │ □□□□□□□□□□  │                      │   │
│  │                         │ 0/1 waiting  │                      │   │
│  │                         ╰──────────────╯                      │   │
│  │                                                               │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ▶ Task Lineage ─────────────────────────────────────── (collapsed)  │
│                                                                      │
│  ▶ Task Table ───────────────────────────────────────── (collapsed)  │
│                                                                      │
│  ▶ Actor Table ──────────────────────────────────────── (collapsed)  │
│                                                                      │
│  ▶ Placement Group Table ────────────────────────────── (collapsed)  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### 點擊 Node 展開

點擊 `train_batch` node，在 DAG 右側或下方展開 detail panel：

```
┌─ train_batch ──────────────────────────────────────────┐
│                                                         │
│  Progress              State Distribution               │
│  ■■■□□□□□□□ 30%       ■ Finished   1,500  (30.0%)      │
│  1,500 / 5,000        ■ Running       80  (1.6%)       │
│                        ■ Failed        12  (0.2%)       │
│                        □ Pending    3,408  (68.2%)      │
│                                                         │
│  Upstream Dependencies                                  │
│  ← preprocess  (36K/50K, 72% done)                     │
│  ← load_weights (1/1, ✓)                               │
│                                                         │
│  Downstream                                             │
│  → save_model  (0/1, waiting)                           │
│                                                         │
│  Recent Failures (12)                                   │
│  ┊ abc123  OOM killed on node-5    2m ago               │
│  ┊ def456  OOM killed on node-5    3m ago               │
│  ┊ [View all in Task Table →]                           │
│                                                         │
│  Executing Actors                                       │
│  ┊ TrainWorker (8 instances)                            │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 瓶頸自動偵測邏輯

```typescript
type BottleneckType = 'failing' | 'slow' | 'blocked' | 'healthy' | 'done';

const detectBottleneck = (
  node: DAGNode,
  upstreamNodes: DAGNode[],
): BottleneckType => {
  const total = sumStateCounts(node.state_counts);
  const finished = node.state_counts['FINISHED'] ?? 0;
  const failed = node.state_counts['FAILED'] ?? 0;
  const pending = sumPendingCounts(node.state_counts);

  // 全部完成
  if (finished === total && total > 0) return 'done';

  // 失敗率 > 5%
  if (total > 0 && failed / total > 0.05) return 'failing';

  // 所有 upstream 都完成了，但我還有很多 pending → 我是瓶頸
  const allUpstreamDone = upstreamNodes.every(u => {
    const uTotal = sumStateCounts(u.state_counts);
    const uFinished = u.state_counts['FINISHED'] ?? 0;
    return uFinished === uTotal && uTotal > 0;
  });
  if (allUpstreamDone && pending > total * 0.5) return 'slow';

  // 有 upstream 還沒完成，且我全部都在 PENDING_ARGS_AVAIL → blocked
  const pendingArgs = node.state_counts['PENDING_ARGS_AVAIL'] ?? 0;
  if (pendingArgs === pending && pending > 0) return 'blocked';

  return 'healthy';
};

// 顏色映射
const bottleneckColors: Record<BottleneckType, string> = {
  failing: '#ef4444',  // 紅
  slow:    '#f59e0b',  // 黃
  blocked: '#3b82f6',  // 藍
  healthy: '#e5e7eb',  // 灰
  done:    '#22c55e',  // 綠
};
```

---

## User Stories: 這個 DAG 能解決什麼問題

### 1. 新人 Onboarding — "這個 job 到底在做什麼？"

**角色**: 剛加入團隊的 ML Engineer，被要求維護一個別人寫的 training pipeline。

**現狀體驗**:
1. 打開 Job Detail 頁面
2. 看到 Ray Core Overview：一個 progress bar 顯示 "120,000 tasks, 85% finished"
3. 展開 Advanced Progress Bar，看到：
   ```
   driver
   ├── read_parquet        (x50000)  ■■■■■■■■■■
   ├── validate_schema     (x50000)  ■■■■■■■■■□
   ├── preprocess          (x50000)  ■■■■■■■□□□
   ├── TrainWorker         (x8 actors)
   │   └── train_step      (x10000)  ■■■□□□□□□□
   ├── evaluate            (x200)    ■□□□□□□□□□
   └── save_checkpoint     (x5)      □□□□□□□□□□
   ```
4. 問自己："validate_schema 的 input 是什麼？train_step 要等誰完成？
   evaluate 是在 evaluate 什麼？"
5. 只能去翻程式碼或問同事

**有 DAG 後**:
```
read_parquet ──→ validate_schema ──→ preprocess ──┐
                                                   ├──→ train_step ──→ evaluate
                                    load_config ──┘                       │
                                                                          ▼
                                                                  save_checkpoint
```
2 秒看懂整個 pipeline 的結構。新人不需要看 code 就知道資料怎麼流動的。

---

### 2. 瓶頸定位 — "為什麼我的 job 跑這麼慢？"

**角色**: Data Engineer，跑一個 batch inference pipeline，預期 30 分鐘完成，但已經跑了 2 小時。

**現狀體驗**:
1. 看 Ray Core Overview：120K tasks, 只有 40% finished
2. 展開看 func_name 分組，所有 stage 都在跑，不知道誰拖慢了誰
3. 推測流程：read → transform → inference → write（但不確定）
4. 手動 filter Task Table by `func_or_class_name = "transform"`，數一下各個 state 的比例
5. 再 filter `inference`，數一下比例
6. 自己比較：read 全完成了，transform 90% 完成，inference 只有 20%
7. 結論：inference 是瓶頸。花了 15 分鐘才找到

**有 DAG 後**:
```
╭──────────────╮     ╭──────────────╮     ╭━━━━━━━━━━━━━━╮     ╭──────────────╮
│ read_files   │────▷│ transform    │────▷┃ inference    ┃────▷│ write_output │
│ ■■■■■■■■■■  │     │ ■■■■■■■■■□  │     ┃ ■■□□□□□□□□  ┃     │ □□□□□□□□□□  │
│ 30K/30K  ✓  │     │ 27K/30K     │     ┃ 6K/30K      ┃     │ 0/30K       │
│ (green)      │     │ (green)      │     ┃ (yellow)     ┃     │ (blue)       │
╰──────────────╯     ╰──────────────╯     ╰━━━━━━━━━━━━━━╯     ╰──────────────╯
```
- `inference` 黃色邊框 = 瓶頸（上游都完成了，它嚴重落後）
- `write_output` 藍色邊框 = blocked（在等 inference 的 output）
- **5 秒定位問題**，不需要手動 filter 任何東西

**接下來的動作**: 點擊 `inference` node → 看到 executing actor 是 `InferenceWorker (x4)`
→ pending 有 24K → 明顯需要 scale up actor pool

---

### 3. 失敗診斷 — "Job 掛了，但我不知道是哪個環節"

**角色**: ML Engineer，收到 alert 說 nightly training job 失敗了。

**現狀體驗**:
1. 打開 Job Detail，看到 status: FAILED
2. 看 Ray Core Overview：有些 task FINISHED，有些 FAILED，混在一起
3. 打開 Task Table，filter by state = FAILED，看到 47 筆
4. 這 47 筆散落在不同 func_name 裡：12 筆 `train_step`，35 筆 `evaluate`
5. 一個一個點開看 error message
6. `train_step` 的 error: "CUDA OOM on node gpu-3"
7. `evaluate` 的 error: "dependency failed" — 這是因為 train_step 失敗導致的連鎖反應
8. 花了 20 分鐘才搞清楚：根因是 `train_step` OOM，`evaluate` 只是連帶的

**有 DAG 後**:
```
read ──→ preprocess ──→ train_step ──→ evaluate ──→ save_model
(green)    (green)       (RED: 12       (RED: 35      (blue:
                          failed)        failed)       blocked)
```
- 看到兩個紅色 node，但 `train_step` 在 `evaluate` 上游
- 點擊 `train_step` → error: "CUDA OOM on node gpu-3"
- 點擊 `evaluate` → error: "dependency failed"
- **立刻知道 root cause 是 train_step，evaluate 是 cascading failure**
- DAG 的方向性讓 cascading failure 的因果關係一目了然

---

### 4. Backpressure 偵測 — "Pipeline 跑著跑著變慢了"

**角色**: Data Engineer，跑一個 streaming-style 的 ETL pipeline。前 10 分鐘很快，後來越來越慢。

**現狀體驗**:
1. 看 Ray Core Overview：task 還在跑，但 progress 幾乎不動
2. 展開看各 stage，每個 stage 都有一些 RUNNING、一些 PENDING
3. 不知道是誰卡住了誰
4. 猜測可能是某個 stage 太慢導致上游 backpressure
5. 開始手動比較各 stage 的 throughput（但 dashboard 沒有 throughput metric）
6. 最後只能加 logging 重跑

**有 DAG 後**:
```
╭──────────────╮     ╭──────────────╮     ╭──────────────╮     ╭──────────────╮
│ ingest       │────▷│ parse        │────▷│ enrich       │────▷│ write_to_db  │
│ ■■■■■■■■■■  │     │ ■■■■■■■■■□  │     │ ■■■■■□□□□□  │     │ ■■□□□□□□□□  │
│ 100K/100K ✓ │     │ 90K/100K    │     │ 50K/100K    │     │ 20K/100K    │
╰──────────────╯     ╰──────────────╯     ╰──────────────╯     ╰──────────────╯
```

DAG 讓你看到一個**完成比例遞減**的 waterfall：
- `ingest` 100% → `parse` 90% → `enrich` 50% → `write_to_db` 20%
- 差距在 `enrich` → `write_to_db` 之間最大
- 點擊 `write_to_db` → PENDING_ARGS_AVAIL: 20K（在等 enrich 的 output），RUNNING: 5
- 點擊 `enrich` → RUNNING: 50, PENDING_NODE_ASSIGNMENT: 30K
- **結論**: `enrich` 有大量 task 在排隊等 node，資源不夠 → 需要加 node 或調整 resource 配置

這在 flat progress bar 裡幾乎不可能看出來，因為你需要**同時比較多個 stage 的完成比例和它們的上下游關係**。

---

### 5. Resource 規劃 — "我的 GPU actor 夠嗎？"

**角色**: MLOps Engineer，要決定一個 distributed training job 需要多少 GPU。

**現狀體驗**:
1. 跑一次 job，看 Actor Table：8 個 TrainWorker
2. 看 Task Table：train_step 有 20K 筆 PENDING
3. 不知道這 20K 是在等 data（上游還沒產出）還是在等 GPU（actor 不夠）
4. 需要交叉比對 preprocess 的完成量和 train_step 的 PENDING 原因

**有 DAG 後**:
```
preprocess (50K/50K ✓) ──→ train_step (5K/50K, 🟡 slow)
                            │ RUNNING: 8
                            │ PENDING_NODE_ASSIGNMENT: 20K  ← 等 GPU！
                            │ PENDING_ARGS_AVAIL: 17K       ← 等 data
                            │
                            │ Actors: TrainWorker (x8), GPU: 8x A100
```

一眼可以看到：
- 上游 `preprocess` 已經全部完成，不是 data 的問題
- `PENDING_NODE_ASSIGNMENT` 20K = 有 20K task 等不到執行 node → **GPU 不夠**
- 目前 8 個 actor 只能同時跑 8 個 task → 需要 scale up

---

### 6. 多分支 Pipeline 理解 — "這個 fan-out / fan-in 在做什麼？"

**角色**: 任何人看到一個複雜的 job，有多個分支。

**現狀體驗**:
```
driver
├── fetch_user_data      (x1000)  ■■■■■■■■■■
├── fetch_product_data   (x500)   ■■■■■■■■■■
├── fetch_behavior_data  (x2000)  ■■■■■■■□□□
├── join_features        (x1000)  ■■■□□□□□□□
├── train                (x100)   □□□□□□□□□□
└── export               (x1)     □□□□□□□□□□
```
看不出 join_features 是在 join 哪些 data，train 在等什麼。

**有 DAG 後**:
```
fetch_user_data ────────┐
                         │
fetch_product_data ─────├──→ join_features ──→ train ──→ export
                         │
fetch_behavior_data ────┘
```
- 清楚看到 3 個 fetch 是並行的 fan-out
- `join_features` 是 fan-in，需要等 3 個 fetch 都完成
- `fetch_behavior_data` 只有 70% → 這是 `join_features` 進度慢的原因
- **DAG 讓 fan-out/fan-in 的結構和因果關係變得直觀**

---

### 7. A/B 對比跑法效能 — "新版 preprocess 有比較快嗎？"

**角色**: Data Scientist，改了 preprocess 的邏輯，想知道有沒有改善。

**現狀體驗**:
1. 跑舊版，手動記下各 stage 的完成時間
2. 跑新版，再手動記一次
3. 自己做 spreadsheet 比較

**有 DAG 後**:
打開兩個 job 的 DAG view 並排比較：

Job A (舊版):
```
read (done, 2min) ──→ preprocess_v1 (done, 15min) ──→ train (done, 10min)
```

Job B (新版):
```
read (done, 2min) ──→ preprocess_v2 (done, 8min) ──→ train (done, 10min)
```

每個 node 顯示 duration，直接看出 preprocess 從 15min → 8min，改善了 47%。

---

### 8. 失敗重試的影響範圍 — "這個 stage 失敗了，下游會怎樣？"

**角色**: SRE，半夜被 alert 叫起來，job 的某個 stage 有 task 失敗正在重試。

**現狀體驗**:
1. 看到 alert: "train_step has 5 failed tasks"
2. 不知道這會影響哪些下游 stage
3. 不知道下游是已經開始跑了還是還在等
4. 翻 code 看 train_step 的 output 被誰用

**有 DAG 後**:
```
preprocess ──→ train_step (🔴 5 failed, retrying) ──→ evaluate ──→ save_checkpoint
                                                       (blue:       (blue:
                                                        blocked)     blocked)
```
- 立刻看到 `evaluate` 和 `save_checkpoint` 都是藍色（blocked on dependency）
- 知道影響範圍：失敗的 train_step 重試成功前，下游全部卡住
- 如果 retry 成功，downstream 會自動恢復
- **不需要翻 code 就能判斷 blast radius**

---

### 9. 動態 DAG 生長 — "我的 job 在 runtime 會長出新 task"

**角色**: 用 Ray 寫了一個 recursive decomposition 的 workload，task 數量不是預先確定的。

**現狀體驗**:
1. Ray Core Overview 的 total task count 一直在漲
2. 不知道新 task 是從哪裡來的
3. 不知道整體進度到底多少（因為 total 不斷變化）

**有 DAG 後**:
```
decompose (100/100 ✓) ──→ solve_subproblem (500/??? running)
                                    │
                                    ├──→ solve_subproblem (遞迴，動態生成)
                                    │
                                    └──→ merge_results (0/??? waiting)
```
- Node 的 total 會即時更新（4 秒 polling）
- 看到 `solve_subproblem` 的 total 在持續增加 = task 在動態生成
- `merge_results` 顯示 waiting = 還沒開始，因為 subproblem 還沒解完
- **即使是動態 DAG，資料流方向仍然清楚**

---

### 10. 跨團隊溝通 — "跟 PM 解釋 job 為什麼慢"

**角色**: Tech Lead，需要向非技術的 PM 解釋為什麼 nightly pipeline 延遲了。

**現狀體驗**:
1. 打開 dashboard，截一張 Task Table 的圖
2. PM 看不懂 120K rows 的 task table
3. 自己畫一張簡化的流程圖，手動標上數字
4. 花 30 分鐘準備一張有意義的截圖

**有 DAG 後**:
直接截圖 DAG view：
```
read (✓) → transform (✓) → inference (🟡 30%, bottleneck) → write (blocked)
```
- PM 一看就懂："inference 這步卡住了，只完成了 30%"
- 不需要任何額外解釋
- **DAG 是自帶 context 的視覺化，不需要解讀就能理解**

---

### User Story 總結

| # | 角色 | 問題 | 現在要花多久 | 有 DAG 後 |
|---|------|------|-------------|----------|
| 1 | 新人 | 理解 job 結構 | 讀 code 30min+ | 2 秒看 DAG |
| 2 | Data Eng | 找瓶頸 | 15 min 手動 filter | 5 秒看顏色 |
| 3 | ML Eng | 找失敗 root cause | 20 min 翻 error | 看 DAG 方向 + 顏色 |
| 4 | Data Eng | 偵測 backpressure | 加 logging 重跑 | 看 waterfall 比例 |
| 5 | MLOps | 判斷 GPU 夠不夠 | 交叉比對 table | 看 pending 原因 |
| 6 | 任何人 | 理解 fan-out/fan-in | 讀 code | 看 DAG 形狀 |
| 7 | Data Sci | A/B 比較效能 | 手動記時間 | 並排看 DAG duration |
| 8 | SRE | 判斷失敗影響範圍 | 翻 code 看依賴 | 看下游顏色 |
| 9 | Developer | 理解動態 DAG | 不斷刷新看 total | 看 node 即時更新 |
| 10 | Tech Lead | 跟 PM 解釋 | 畫圖 30min | 截圖 DAG 2 秒 |

---

## 各 Workload 適用性分析

DAG view 是建在 Ray Core 的 task/actor 抽象上，但不同 Ray library 產生的 task graph 結構差異很大。
這裡誠實分析每種 workload 的適用程度。

### Ray Data — 完美適用 (★★★★★)

**產生的 task graph:**
```
ReadParquet ──→ MapBatches(preprocess) ──→ MapBatches(inference) ──→ Write
  _map_task        _map_task                  _map_task              _map_task
  (x50000)         (x50000)                   (x50000)              (x50000)
```

**為什麼適用:**
- Ray Data 的 operator 模型天然就是 pipeline：read → map → filter → write
- 每個 operator 對應一個 `func_or_class_name`（通常是 `_map_task`，但帶有 operator context）
- Operator 之間透過 `ObjectRef[Block]` 傳遞資料 → 有明確的 ObjectRef 依賴
- 跟 Spark 的 stage DAG 最接近，用戶期待也最高

**DAG 長相:**
```
ReadParquet ──→ SplitBlocks ──→ MapBatches(tokenize) ──→ MapBatches(embed) ──→ WriteParquet
  50K/50K ✓      50K/50K ✓       36K/50K                   8K/50K 🟡           2K/50K
```

**注意:** Ray Data 已經有自己的 progress bar（per-operator），但只在 driver stdout，不在 dashboard。
DAG view 等於把這個搬到 dashboard 並加上依賴關係。

---

### Ray Serve (Deployment Graph) — 高度適用 (★★★★☆)

**產生的 task graph:**
```
HTTPProxy ──→ Router ──→ Preprocessor ──→ Model_A ──→ Postprocessor
                                      └──→ Model_B ──┘
```

**為什麼適用:**
- Serve 的 deployment composition（`A.bind(B.bind())`）本身就是一個 DAG
- 每個 deployment 是一個 actor class，每個 request 產生 actor task
- Deployment 之間透過 `ServeHandle.remote()` 傳遞 ObjectRef → 有依賴

**但跟 Data pipeline 不同的地方:**
- 不是 batch 式的「50K task 逐步完成」，而是 streaming 式的「每秒處理 N 個 request」
- 每個 request 是獨立的 fan-out，不是整體的 pipeline
- 更適合顯示 **throughput** 和 **latency** 而不是 finished/total

**DAG 長相（adapted for Serve）:**
```
╭──────────────────╮     ╭──────────────────╮     ╭──────────────────╮
│ Preprocessor     │────▷│ LLMModel         │────▷│ Postprocessor    │
│ 3 replicas 🟢   │     │ 8 replicas 🟢   │     │ 3 replicas 🟢   │
│ QPS: 150         │     │ QPS: 120  🟡     │     │ QPS: 145         │
│ p50: 2ms         │     │ p50: 180ms       │     │ p50: 5ms         │
│ queue: 12        │     │ queue: 847 🔴    │     │ queue: 3         │
╰──────────────────╯     ╰──────────────────╯     ╰──────────────────╯
```

**價值:** Queue depth 在 DAG 裡一看就知道 LLMModel 是瓶頸（queue 最長）。
目前 Serve dashboard 有 deployment 列表，但沒有 DAG 視覺化。

**額外考量:**
- Serve 已有自己的 dashboard tab，DAG view 可能更適合放在 Serve 頁面而不是 Jobs 頁面
- Node 的 metric 應該是 QPS / latency / queue depth，而不是 finished/total
- 需要跟 Serve 團隊討論是否整合

---

### Ray Core (自定義 task graph) — 高度適用 (★★★★☆)

**產生的 task graph:** 取決於用戶寫的程式，可能是：
```python
# Pipeline
a = read.remote()
b = process.remote(a)
c = write.remote(b)

# Fan-out / Fan-in
futures = [process.remote(x) for x in data]
result = aggregate.remote(*futures)

# Recursive
def solve(problem):
    if small(problem): return base.remote(problem)
    a, b = split(problem)
    return merge.remote(solve(a), solve(b))
```

**為什麼適用:**
- 用戶自己寫的 `@ray.remote` function 就是 DAG 的 node
- ObjectRef 依賴是顯式的 → 可以直接提取
- 這是 DAG view 最通用的場景

**DAG 長相:** 完全取決於用戶的程式，但 aggregation by `func_or_class_name` 總是有意義的。

---

### Ray Train — 部分適用 (★★★☆☆)

**產生的 task graph:**
```
TrainWorker.__execute  (x8 actors, 每個跑一個 long-running training loop)
│
├── (iteration 1) train_step → allreduce → checkpoint
├── (iteration 2) train_step → allreduce → checkpoint
├── ...
└── (iteration N) train_step → allreduce → checkpoint
```

**為什麼只是部分適用:**
- Ray Train 的核心模式是 **iterative loop**，不是 pipeline
- 8 個 TrainWorker 是長期存活的 actor，每個 iteration 內部 submit actor task
- Actor task 之間的依賴是 **allreduce**（集體通信），不是 ObjectRef chain
- DAG 會長這樣：

```
╭────────────────────╮
│ TrainWorker (x8)   │
│ 🟢 8 alive         │
│ executed: 12000    │
│ pending: 0         │
╰────────────────────╯
```

就一個 node，沒有邊。因為所有 work 都在 actor 內部完成。

**但 data loading 部分有用:**
如果 Train 搭配 Ray Data 做 data loading：
```
ReadParquet ──→ MapBatches(preprocess) ──→ TrainWorker (x8)
  _map_task        _map_task                actor tasks
```

這時 DAG 可以看出 data pipeline 和 training 之間的關係。

**結論:** DAG view 對純 Train workload 價值有限，但對 Train + Data 組合有用。

---

### Ray Tune — 價值有限 (★★☆☆☆)

**產生的 task graph:**
```
TuneController
├── Trial_1 (Trainable actor) ── 獨立
├── Trial_2 (Trainable actor) ── 獨立
├── Trial_3 (Trainable actor) ── 獨立
├── ...
└── Trial_100 (Trainable actor) ── 獨立
```

**為什麼價值有限:**
- Tune 的核心模式是 **fan-out 獨立 trials** — 每個 trial 互不依賴
- DAG 退化成一個 flat 的 fan-out：

```
╭────────────────────╮
│ Trainable (x100)   │
│ 🟢 30 alive        │
│ FINISHED: 70       │
│ RUNNING: 30        │
╰────────────────────╯
```

一個 node，沒有邊。跟現有的 progress bar 資訊量一樣。

**例外 — PBT (Population Based Training):**
PBT 的 trial 之間有 mutation 依賴（一個 trial 的 checkpoint 會被另一個 trial 用）。
但這個依賴不是透過 ObjectRef 傳遞的，而是透過 checkpoint storage，所以 DAG 也抓不到。

**結論:** DAG view 對 Tune 幾乎沒有額外價值。不需要特別支援。

---

### Ray RLlib — 部分適用 (★★★☆☆)

**產生的 task graph:**
```
EnvRunner.sample (x16 actors, 並行 rollout)
    │
    ▼
Learner.update (x1 actor, centralized training)
    │
    ▼
(sync weights back to EnvRunners, 開始下一個 iteration)
```

**為什麼只是部分適用:**
- RLlib 有明確的兩階段結構：sample → learn
- 但這是一個 **iterative loop**，每個 iteration 重複同樣的兩階段
- sample 到 learn 之間有 ObjectRef 依賴（experience batch）

**DAG 長相:**
```
╭──────────────────╮     ╭──────────────────╮
│ EnvRunner (x16)  │────▷│ Learner (x1)     │
│ sample tasks     │     │ update tasks     │
│ 🟢 16 alive      │     │ 🟢 1 alive       │
│ executed: 4800   │     │ executed: 300    │
╰──────────────────╯     ╰──────────────────╯
```

比 Train 好一點，因為有 sample → learn 的方向性。
但本質上只有 2 個 node，資訊量仍然有限。

---

### 適用性總結

| Workload | Task Graph 結構 | DAG 適用度 | 核心價值 |
|----------|----------------|-----------|---------|
| **Ray Data** | Pipeline (A→B→C) | ★★★★★ | 瓶頸定位、backpressure、結構理解 |
| **Ray Serve** | Deployment DAG | ★★★★☆ | 請求流路徑、queue 瓶頸 |
| **Ray Core** | 用戶自定義 | ★★★★☆ | 通用 task graph 視覺化 |
| **Ray Train** | Iterative loop | ★★★☆☆ | 搭配 Data 時有用 |
| **RLlib** | Sample→Learn loop | ★★★☆☆ | 2 階段結構可視 |
| **Ray Tune** | Fan-out 獨立 trials | ★★☆☆☆ | 幾乎退化成 flat view |

### 設計啟示

1. **Phase 1 目標用戶: Ray Data + Ray Core** — 這兩個場景 DAG 的 ROI 最高
2. **Serve 需要額外適配** — metric 從 finished/total 換成 QPS/latency/queue
3. **Train/Tune/RLlib 不需要特別優化** — DAG view 會自動退化成合理的 aggregated view，不會壞掉，只是資訊量有限
4. **兩個 view 並存，collapsible sections** — 跟 Task Table / Actor Table 一樣，做成可展開的 section。
   用戶按需展開，不佔空間。

```
Job Detail 頁面 layout:

Ray Core Overview          ← 現有的總 progress bar，always visible
│
├── ▶ Dataflow DAG         ← 新增，collapsible，展開顯示 DAG
├── ▶ Task Lineage         ← 現有的 AdvancedProgressBar，collapsible
├── ▶ Task Table           ← 現有，collapsible
├── ▶ Actor Table          ← 現有，collapsible
└── ▶ Placement Group Table ← 現有，collapsible
```

```typescript
// JobDetail.tsx — 跟其他 section 一樣的 pattern
<CollapsibleSection title="Dataflow DAG" startExpanded={false}>
  <DAGProgressBar jobId={jobId} />
</CollapsibleSection>

<CollapsibleSection title="Task Lineage" startExpanded={false}>
  <AdvancedProgressBar progressGroups={lineageGroups} />
</CollapsibleSection>

<CollapsibleSection title="Task Table" startExpanded={false}>
  <TaskTable jobId={jobId} />
</CollapsibleSection>
```

好處：
- 跟頁面現有 pattern 一致，不需要學新的 UI
- 兩個 view 可以同時展開並排看
- 不需要的 view 收起來不佔空間
- DAG fetch 可以 lazy load（收起來時不打 API）

---

## 實作優先級

| 優先級 | 項目 | 改動 | 價值 |
|--------|------|------|------|
| **P0** | Proto + C++: 加 dependency/return object IDs | ~15 行 C++, ~5 行 proto | 所有後續功能的基礎 |
| **P0** | Python: TaskState + protobuf_to_task_state_dict | ~15 行 Python | 暴露到 State API |
| **P0** | Python: to_summary_by_dataflow() | ~120 行 Python | DAG 聚合邏輯 |
| **P1** | 前端: DAGProgressBar + React Flow | ~400 行 TypeScript | DAG 視覺化 |
| **P1** | 前端: 瓶頸顏色 + 動畫邊 | ~50 行 TypeScript | 一眼看到問題 |
| **P2** | 前端: 點擊展開 detail panel | ~200 行 TypeScript | 深入分析 |
| **P2** | 前端: Actor 關聯展示 | ~100 行 TypeScript | Actor ↔ Task 對照 |

**總改動量：**
- C++ / Proto: ~20 行
- Python backend: ~140 行
- TypeScript frontend: ~750 行
- 預估: 一個人 2-3 週可以完成 P0+P1
