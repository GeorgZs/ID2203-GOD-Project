import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


@dataclass
class ExperimentFiles:
    server_files: dict[int, Path]
    client_files: dict[int, tuple[Path, Path]]


def find_experiment_logs(experiment_name: str) -> ExperimentFiles:
    experiment_directory = Path(f"logs/{experiment_name}")
    assert (
        experiment_directory.exists()
    ), f"There is no {experiment_name} expermiment data"
    server_pattern = re.compile(r"^server-(\d+)\.json")
    experiment_files = ExperimentFiles({}, {})
    for file in experiment_directory.rglob("*.json"):
        match = server_pattern.search(file.name)
        if match:
            server_id = int(match.group(1))
            client_output = Path(
                str(file).replace(f"server-{server_id}.json", f"client-{server_id}.csv")
            )
            client_summary = Path(
                str(file).replace(
                    f"server-{server_id}.json", f"client-{server_id}.json"
                )
            )
            experiment_files.server_files[server_id] = file
            if client_output.exists():
                experiment_files.client_files[server_id] = (
                    client_output,
                    client_summary,
                )
    return experiment_files


def parse_server_log(output_file: Path) -> pd.DataFrame:
    print(output_file)
    with open(output_file, "r") as file:
        server_output = json.load(file)
    server_data = pd.json_normalize(server_output)
    return server_data


def parse_client_log(output_file: Path, summary_file: Path) -> pd.DataFrame:
    print(output_file)
    try:
        client_data = pd.read_csv(
            output_file,
            names=["request_time", "write", "response_time"],
            header=0,
            dtype={"write": "bool"},
        )
    except pd.errors.EmptyDataError:
        client_data = pd.DataFrame(columns=["request_time", "write", "response_time"])
    client_data["response_latency"] = (
        client_data.response_time - client_data.request_time
    )
    client_data = client_data.astype(dtype={"request_time": "datetime64[ms]"})
    client_data.set_index("request_time", inplace=True)
    with open(summary_file, "r") as file:
        client_summary = json.load(file)
    if client_summary["sync_time"] is not None:
        assert client_summary["sync_time"] >= 0, "Clients' start not synced"
    client_data.attrs = client_summary
    return client_data


def get_experiment_data(
    experiment_name: str
) -> tuple[dict[int, pd.DataFrame], dict[int, pd.DataFrame]]:
    servers_data = {}
    clients_data = {}
    experiment_files = find_experiment_logs(experiment_name)
    for id, server_log_file in experiment_files.server_files.items():
        server_data = parse_server_log(server_log_file)
        servers_data[id] = server_data
        client_files = experiment_files.client_files.get(id)
        if client_files is not None:
            client_log_file, client_summary_file = client_files
            clients_data[id] = parse_client_log(client_log_file, client_summary_file)
    return clients_data, servers_data


def location_name(location: str) -> str:
    if location.startswith("local"):
        return location
    name_mapping = {
        "us-west2-a": "Los Angeles",
        "us-south1-a": "Dallas",
        "us-east4-a": "N. Virginia",
        "us-east5-a": "Columbus",
        "europe-west2-a": "London",
        "europe-west4-a": "Netherlands",
        "europe-west10-a": "Berlin",
        "europe-southwest1-a": "Madrid",
        "europe-central2-a": "Warsaw",
    }
    name = name_mapping.get(location)
    if name is None:
        raise ValueError(f"Don't have name for location {location}")
    return name


def location_color(location: str):
    color_mapping = {
        "us-west2-a": "#FDBB3B",
        "us-south1-a": "#4CA98F",
        "us-east4-a": "#9A6DB8",
        "europe-west2-a": "#4CA98F",
        "europe-west4-a": "#FF6478",
        "europe-west10-a": "#9A6DB8",
        "europe-southwest1-a": "tab:blue",
        "local-1": "tab:blue",
        "local-2": "tab:green",
        "local-3": "tab:red",
    }
    color = color_mapping.get(location)
    if color is None:
        raise ValueError(f"Don't have color for location {location}")
    return color


def create_base_figure(clients_data: dict[int, pd.DataFrame]):
    axis_label_size = 20
    axis_tick_size = 12
    fig, axs = plt.subplots(
        2, 1, sharex=True, gridspec_kw={"height_ratios": [4, 1]}, layout="constrained"
    )
    # fig.suptitle(title, y=0.95)
    # fig.subplots_adjust(hspace=0)
    fig.set_size_inches((12, 6))
    # axs[0].set_facecolor('#f0f0f0')
    # axs[1].set_facecolor('#f0f0f0')
    # axs[0].grid(color='white', linestyle='-', linewidth=0.7)
    # axs[1].grid(color='white', linestyle='-', linewidth=0.7)
    # axs[0].yaxis.grid(color='lightgrey', linestyle='-', linewidth=0.7)
    # axs[1].grid(color='lightgrey', linestyle='--', linewidth=0.7)
    # Axis labels
    axs[0].set_ylabel("Request Latency\n(ms)", fontsize=axis_label_size)
    axs[1].set_xlabel("Experiment Time", fontsize=axis_label_size)
    axs[1].set_ylabel("Request Rate\n(%)", fontsize=axis_label_size)
    fig.align_ylabels()
    # Splines
    axs[0].spines["top"].set_visible(False)
    axs[0].spines["right"].set_visible(False)
    # axs[0].spines['bottom'].set_color("#DDDDDD")
    axs[0].spines["bottom"].set_visible(False)
    axs[1].spines["top"].set_visible(False)
    axs[1].spines["right"].set_visible(False)
    # axs[1].spines['bottom'].set_visible(False)
    # Axis ticks
    # axs[0].autoscale(axis='y')
    # Set y-axis limit to be just above the max client latency
    top = 0
    for df in clients_data.values():
        client_max = df["response_latency"].max()
        if client_max > top:
            top = client_max
    axs[0].set_ylim(bottom=0, top=math.ceil(top / 10) * 10)
    # axs[0].set_yticks(axs[0].get_yticks()[1:])
    axs[0].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="y", labelsize=axis_tick_size)
    axs[1].tick_params(axis="x", labelsize=axis_tick_size)
    myFmt = mdates.DateFormatter("%M:%S")
    fig.gca().xaxis.set_major_formatter(myFmt)
    axs[0].tick_params(bottom=False)
    axs[1].tick_params(bottom=False)
    graph_relative_request_rate_subplot(axs[1], clients_data)
    # graph_request_rate_subplot(axs[1], clients_data)
    # plt.tight_layout()
    return fig, axs

def graph_relative_request_rate_subplot(fig, clients_data):
    fig.set_ylim(bottom=0, top=1)
    fig.set_yticks([0.0, 0.5, 1.0])
    total_request_rate = pd.DataFrame()
    for requests in clients_data.values():
        request_rate = requests.resample("1s").count()
        total_request_rate = total_request_rate.add(request_rate, fill_value=0)
    total_request_rate = total_request_rate[
        (total_request_rate["response_latency"] != 0)
    ]
    fig.set_xlim(
        left=total_request_rate.index.min(), right=total_request_rate.index.max()
    )
    for id, requests in clients_data.items():
        # request_rate = requests.resample("3s").count()
        request_rate = requests.resample("1s").count() / total_request_rate
        request_rate.fillna(0, inplace=True)
        ma = request_rate.ewm(alpha=0.9).mean()
        color = location_color(requests.attrs["location"])
        label = location_name(requests.attrs["location"])
        fig.plot(
            request_rate.index,
            ma.response_latency,
            linestyle="-",
            color=color,
            linewidth=1,
        )
        fig.fill_between(
            request_rate.index, ma.response_latency, color=color, alpha=0.3
        )


def graph_request_rate_subplot(fig, clients_data):
    for requests in clients_data.values():
        request_rate = requests.resample("1s").count()
        ma = request_rate.ewm(alpha=0.9).mean()
        color = location_color(requests.attrs["location"])
        label = location_name(requests.attrs["location"])
        fig.plot(
            request_rate.index,
            ma["response_latency"],
            linestyle="-",
            label=label,
            color=color,
        )


def graph_client_data_individual(experiment_name: str, specific_server: int | None = None):
    clients_data, servers_data = get_experiment_data(experiment_name)
    for id, server_data in servers_data.items():
        if specific_server is not None and specific_server != id:
            continue
        fig, axs = create_base_figure(clients_data)
        location = location_name(server_data.location[0])
        title = f"Server {id} ({location}) Metrics"
        fig.suptitle(title, y=0.95)

        # Graph request latencies
        client_requests = clients_data.get(id)
        if client_requests is not None:
            read_requests = client_requests.loc[client_requests["write"] == False]
            axs[0].scatter(
                read_requests.index,
                read_requests["response_latency"],
                marker="o",
                linestyle="-",
                label="Read Latency",
                color="pink",
            )
            write_requests = client_requests.loc[client_requests["write"]]
            axs[0].scatter(
                write_requests.index,
                write_requests["response_latency"],
                marker="o",
                linestyle="-",
                label="Write latency",
                color="red",
            )
        axs[0].legend(
            title="Configurations", bbox_to_anchor=(1.01, 1), loc="upper left"
        )  # Legend outside plot to right
        plt.show()


def graph_average_latency_comparison_all(
    experiment_dir: str,
    other_experiments: list[tuple[str, str]], # (name, dir)
    experiment_labels: dict[str, str],
    legend_args: dict,
):
    clients_data, _ = get_experiment_data(experiment_dir)
    # Go from UTC time to experiment time
    epoch_start = pd.Timestamp("20180606")
    all_requests = pd.concat(clients_data.values())
    start = min(all_requests.index)
    for client_data in clients_data.values():
        client_data.index = epoch_start + (client_data.index - start)
    all_requests = pd.concat(clients_data.values())

    # Plot AutoQuorum data
    fig, axs = create_base_figure(clients_data)
    # Moving average latency
    average_latency = all_requests["response_latency"].resample("1s").mean()
    axs[0].plot(
        average_latency.index,
        average_latency.values,
        linestyle="-",
        label="MajorityQuorum",
        # marker=strat_markers["AutoQuorum"],
        color=strat_colors["MajorityQuorum"],
        linewidth=2,
    )

    # Plot other experiment data
    for (experiment_name, experiment_dir) in other_experiments:
        experiment_clients_data, _ = get_experiment_data(experiment_dir)
        all_requests_other = pd.concat(experiment_clients_data.values())
        start = min(all_requests_other.index)
        all_requests_other.index = epoch_start + (all_requests_other.index - start)
        average_latency = all_requests_other["response_latency"].resample("1s").mean()
        axs[0].plot(
            average_latency.index,
            average_latency,
            linestyle="-",
            label=experiment_labels[experiment_name],
            color=strat_colors[experiment_name],
            # marker=strat_markers[experiment_name],
            linewidth=2,
            # alpha=0.5,
        )
    fig.legend(**legend_args)
    fig.tight_layout()


def graph_example_bench():
    experiment_directory = "example-experiment"
    majority_dir = experiment_directory + "/MajorityQuorum/run-0"
    flexible_dir = experiment_directory + "/FlexQuorum/run-0"

    labels = {"MajorityQuorum": "Majority Quorum", "FlexQuorum": "Flexible Quorum"}
    legend_args = {
        "loc": "upper left",
        "bbox_to_anchor": (0.099, 0.99),
        "fontsize": 12,
        "ncols": 1,
        "frameon": False,
    }
    graph_average_latency_comparison_all(
        majority_dir,
        [("FlexQuorum", flexible_dir)],
        labels,
        legend_args,
    )
    plt.show()
    plt.close()

    graph_client_data_individual(flexible_dir)



def main():
    graph_example_bench()
    pass


if __name__ == "__main__":
    strat_colors = {
        "MajorityQuorum": "tab:orange",
        "FlexQuorum": "tab:blue",
    }
    strat_markers = {
        "MajorityQuorum": "s",
        "FlexQuorum": "D",
    }
    strat_hatches = {
        "MajorityQuorum": "x",
        "FlexQuorum": "-",
    }

    main()
