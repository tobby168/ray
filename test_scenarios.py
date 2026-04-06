"""
Run each Ray scenario, wait for tasks, then dump task API data.
Usage: python3 test_scenarios.py <scenario>
  scenario: data-pipeline | training | serving | eval
"""
import sys
import time
import json
import requests
import ray


def dump_tasks(label):
    """Fetch task summary and task list from the Ray State API."""
    time.sleep(3)  # wait for task events to propagate

    # Summary by func_name
    rsp = requests.get("http://localhost:8265/api/v0/tasks/summarize").json()
    summary = rsp["data"]["result"]["result"]["node_id_to_summary"]["cluster"]["summary"]
    print(f"\n=== {label}: Task Summary (by func_name) ===")
    for name, info in sorted(summary.items()):
        print(f"  {name}: type={info['type']} states={info['state_counts']}")

    # Detailed task list (first 20)
    rsp2 = requests.get("http://localhost:8265/api/v0/tasks?detail=1&limit=20").json()
    tasks = rsp2["data"]["result"]["result"]
    print(f"\n=== {label}: Sample Tasks (first 5) ===")
    for t in tasks[:5]:
        print(f"  func={t['func_or_class_name']}")
        print(f"    name={t.get('name')} type={t['type']} state={t['state']}")
        print(f"    actor_id={t.get('actor_id')} node_id={t.get('node_id','')[:20]}...")
        print(f"    required_resources={t.get('required_resources', {})}")
        print(f"    start_time_ms={t.get('start_time_ms')} end_time_ms={t.get('end_time_ms')}")
        print(f"    attempt_number={t.get('attempt_number')} call_site={t.get('call_site')}")
        print()


def scenario_data_pipeline():
    """Ray Data ETL pipeline."""
    import ray.data

    # Create a simple dataset and process it
    ds = ray.data.range(1000)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] * 2})
    ds = ds.filter(lambda x: x["val"] > 100)
    ds = ds.map_batches(lambda batch: batch)  # identity transform
    # Materialize
    count = ds.count()
    print(f"Data pipeline result: {count} rows")


def scenario_training():
    """Ray Train distributed training simulation."""
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        import time
        # Simulate training loop
        for epoch in range(3):
            time.sleep(1)
            # In real code: model.train(), loss.backward(), etc.
            from ray.train import report
            report({"loss": 1.0 / (epoch + 1), "epoch": epoch})

    try:
        trainer = TorchTrainer(
            train_func,
            scaling_config=ScalingConfig(num_workers=2),
        )
        result = trainer.fit()
        print(f"Training result: {result.metrics}")
    except Exception as e:
        print(f"Training error (expected if no torch): {e}")
        # Fallback: just use ray.remote tasks to simulate
        print("Falling back to manual training simulation...")

        @ray.remote
        def load_data(shard_id):
            time.sleep(0.5)
            return list(range(shard_id * 100, (shard_id + 1) * 100))

        @ray.remote
        def preprocess(data):
            time.sleep(0.3)
            return [x * 2 for x in data]

        @ray.remote(num_cpus=1)
        def train_step(batch, epoch):
            time.sleep(0.5)
            return {"loss": 1.0 / (epoch + 1), "batch_size": len(batch)}

        @ray.remote
        def validate(model_state):
            time.sleep(0.3)
            return {"accuracy": 0.85}

        @ray.remote
        def save_checkpoint(metrics):
            time.sleep(0.2)
            return "checkpoint_saved"

        # Pipeline: load -> preprocess -> train -> validate -> save
        shards = [load_data.remote(i) for i in range(8)]
        processed = [preprocess.remote(s) for s in shards]
        for epoch in range(3):
            results = [train_step.remote(p, epoch) for p in processed]
            metrics = ray.get(results)
            val = validate.remote(metrics)
            ray.get(val)
            ckpt = save_checkpoint.remote(ray.get(val))
            ray.get(ckpt)
        print("Fallback training done")


def scenario_serving():
    """Ray Serve deployment graph."""
    from ray import serve

    @serve.deployment(num_replicas=2)
    class Preprocessor:
        def __call__(self, request):
            return {"text": request, "processed": True}

    @serve.deployment(num_replicas=2)
    class Model:
        def __call__(self, data):
            return {"prediction": 0.95, "input": data}

    @serve.deployment(num_replicas=1)
    class Postprocessor:
        def __init__(self, preprocessor, model):
            self.preprocessor = preprocessor
            self.model = model

        async def __call__(self, request):
            data = await self.preprocessor.remote(request)
            result = await self.model.remote(data)
            return {"final": result, "status": "ok"}

    preprocessor = Preprocessor.bind()
    model = Model.bind()
    app = Postprocessor.bind(preprocessor, model)

    handle = serve.run(app)

    # Send some requests
    import asyncio

    async def send_requests():
        for i in range(20):
            result = await handle.remote(f"query_{i}")
            if i < 3:
                print(f"  Request {i}: {result}")

    asyncio.get_event_loop().run_until_complete(send_requests())
    print("Serve requests done")


def scenario_eval():
    """Evaluation pipeline."""

    @ray.remote
    def load_model():
        time.sleep(0.5)
        return {"model": "loaded", "params": 1000000}

    @ray.remote
    def load_dataset(split):
        time.sleep(0.3)
        return [{"text": f"sample_{i}", "label": i % 5} for i in range(100)]

    @ray.remote
    def run_inference(model, sample):
        time.sleep(0.1)
        return {"prediction": sample["label"], "confidence": 0.9}

    @ray.remote
    def compute_accuracy(predictions, labels):
        time.sleep(0.2)
        correct = sum(1 for p, l in zip(predictions, labels) if p["prediction"] == l["label"])
        return {"accuracy": correct / len(labels)}

    @ray.remote
    def compute_f1(predictions, labels):
        time.sleep(0.2)
        return {"f1": 0.88}

    @ray.remote
    def compute_latency(timings):
        time.sleep(0.1)
        return {"p50_ms": 100, "p95_ms": 250}

    @ray.remote
    def aggregate_metrics(*metrics):
        time.sleep(0.1)
        result = {}
        for m in metrics:
            result.update(m)
        return result

    @ray.remote
    def generate_report(metrics):
        time.sleep(0.2)
        return f"Report: {json.dumps(metrics)}"

    # Run eval pipeline
    model_ref = load_model.remote()
    test_data_ref = load_dataset.remote("test")
    golden_labels_ref = load_dataset.remote("golden")

    test_data = ray.get(test_data_ref)
    golden_labels = ray.get(golden_labels_ref)

    # Run inference on each sample
    inference_refs = [run_inference.remote(model_ref, sample) for sample in test_data]
    predictions = ray.get(inference_refs)

    # Compute metrics in parallel
    accuracy_ref = compute_accuracy.remote(predictions, golden_labels)
    f1_ref = compute_f1.remote(predictions, golden_labels)
    latency_ref = compute_latency.remote([100, 150, 200, 120, 180])

    # Aggregate
    agg_ref = aggregate_metrics.remote(
        ray.get(accuracy_ref), ray.get(f1_ref), ray.get(latency_ref)
    )
    report_ref = generate_report.remote(ray.get(agg_ref))
    print(f"Eval result: {ray.get(report_ref)}")


if __name__ == "__main__":
    scenario = sys.argv[1] if len(sys.argv) > 1 else "all"

    ray.init()
    print(f"Ray initialized. Dashboard: http://127.0.0.1:8265")

    scenarios = {
        "data-pipeline": scenario_data_pipeline,
        "training": scenario_training,
        "serving": scenario_serving,
        "eval": scenario_eval,
    }

    if scenario == "all":
        for name, fn in scenarios.items():
            print(f"\n{'='*60}")
            print(f"Running scenario: {name}")
            print(f"{'='*60}")
            try:
                fn()
                dump_tasks(name)
            except Exception as e:
                print(f"Error in {name}: {e}")
                import traceback
                traceback.print_exc()
                dump_tasks(name)
    elif scenario in scenarios:
        scenarios[scenario]()
        dump_tasks(scenario)
    else:
        print(f"Unknown scenario: {scenario}")
        print(f"Available: {', '.join(scenarios.keys())}, all")
        sys.exit(1)

    # Keep alive for inspection
    print(f"\nAll done. Keeping alive for 120s for dashboard inspection...")
    print(f"Dashboard: http://127.0.0.1:8265")
    time.sleep(120)
    ray.shutdown()
