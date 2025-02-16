from pathlib import Path

from omnipaxos_cluster import OmnipaxosClusterBuilder
from omnipaxos_configs import FlexibleQuorum, RequestInterval


def example_workload() -> dict[int, list[RequestInterval]]:
    experiment_duration = 10
    read_ratio = 0.50
    high_load = RequestInterval(experiment_duration, 100, read_ratio)
    low_load = RequestInterval(experiment_duration, 10, read_ratio)

    nodes = [1, 2, 3, 4, 5]
    us_nodes = [1, 2, 3]
    workload = {}
    for node in nodes:
        if node in us_nodes:
            requests = [high_load, low_load]
        else:
            requests = [low_load, high_load]
        workload[node] = requests
    return workload


def example_benchmark(num_runs: int = 3):
    # Define workload and cluster
    workload = example_workload()
    cluster = (
        OmnipaxosClusterBuilder("cluster-1")
        .initial_leader(5)
        .server(1, "us-west2-a")
        .server(2, "us-south1-a")
        .server(3, "us-east4-a")
        .server(4, "europe-southwest1-a")
        .server(5, "europe-west4-a")
        .client(1, "us-west2-a", requests=workload[1])
        .client(2, "us-south1-a", requests=workload[2])
        .client(3, "us-east4-a", requests=workload[3])
        .client(4, "europe-southwest1-a", requests=workload[4])
        .client(5, "europe-west4-a", requests=workload[5])
    ).build()
    experiment_log_dir = Path(f"./logs/example-experiment")

    majority_quorum = FlexibleQuorum(read_quorum_size=3, write_quorum_size=3)
    flex_quorum = FlexibleQuorum(read_quorum_size=4, write_quorum_size=2)
    for run in range(num_runs):
        # Run cluster with majority quorum
        cluster.change_cluster_config(initial_flexible_quorum=majority_quorum)
        iteration_dir = Path.joinpath(experiment_log_dir, f"MajorityQuorum/run-{run}")
        print("RUNNING:", iteration_dir)
        cluster.run(iteration_dir)

        # Run same cluster again but with flexible quorum
        flex_quorum = FlexibleQuorum(read_quorum_size=4, write_quorum_size=2)
        cluster.change_cluster_config(initial_flexible_quorum=flex_quorum)
        iteration_dir = Path.joinpath(experiment_log_dir, f"FlexQuorum/run-{run}")
        print("RUNNING:", iteration_dir)
        cluster.run(iteration_dir)

    # Shutdown GCP instances (or not if you want to reuse instances in another benchmark)
    cluster.shutdown()


def main():
    example_benchmark()
    pass


if __name__ == "__main__":
    main()
