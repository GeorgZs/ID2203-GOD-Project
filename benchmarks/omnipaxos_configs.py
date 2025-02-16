from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace

import toml

from gcp_cluster import InstanceConfig


@dataclass(frozen=True)
class ClusterConfig:
    cluster_name: str
    nodes: list[int]
    initial_leader: int
    initial_flexible_quorum: FlexibleQuorum | None
    server_configs: dict[int, ServerConfig]
    client_configs: dict[int, ClientConfig]
    client_image: str
    server_image: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        if self.initial_flexible_quorum:
            read_quorum = self.initial_flexible_quorum.read_quorum_size
            write_quorum = self.initial_flexible_quorum.write_quorum_size
            if read_quorum < 2:
                raise ValueError(f"Read quorum must be greater than 2")
            if write_quorum < 2:
                raise ValueError(f"Write quorum must be greater than 2")
            if read_quorum + write_quorum <= len(self.nodes):
                raise ValueError(
                    f"Flexible quorum {(read_quorum, write_quorum)} must guarantee overlap"
                )

        for client_id in self.client_configs.keys():
            if client_id not in self.server_configs.keys():
                raise ValueError(f"Client {client_id} has no server to connect to")

        for server_id, server_config in self.server_configs.items():
            client_configs = self.client_configs.values()
            server_id_matches = sum(
                1 for _ in filter(lambda c: c.server_id == server_id, client_configs)
            )
            total_matches = server_id_matches
            if server_config.num_clients != server_id_matches:
                raise ValueError(
                    f"Server {server_id} has {server_config.num_clients} clients but found {total_matches} references among client configs"
                )

        if self.initial_leader not in self.server_configs.keys():
            raise ValueError(
                f"Initial leader {self.initial_leader} must be one of the server nodes"
            )

        server_ids = sorted(self.server_configs.keys())
        if self.nodes != server_ids:
            raise ValueError(
                f"Cluster nodes {self.nodes} must match defined server ids {server_ids}"
            )

    def with_updated(self, **kwargs) -> ClusterConfig:
        new_config = replace(self, **kwargs)
        new_config.validate()
        return new_config


@dataclass(frozen=True)
class ServerConfig:
    instance_config: InstanceConfig
    server_id: int
    num_clients: int
    output_filepath: str
    rust_log: str

    @dataclass(frozen=True)
    class AutoQuorumServerToml:
        location: str
        server_id: int
        num_clients: int
        output_filepath: str
        # Cluster-wide config
        cluster_name: str
        nodes: list[int]
        initial_leader: int
        initial_flexible_quorum: FlexibleQuorum | None

    def __post_init__(self):
        self.validate()

    def validate(self):
        if self.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {self.server_id}. It must be greater than 0."
            )

        if self.num_clients < 0:
            raise ValueError(
                f"Invalid num_clients: {self.num_clients}. It must be a positive number."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def with_updated(self, **kwargs) -> ServerConfig:
        new_config = replace(self, **kwargs)
        return new_config

    def generate_server_toml(self, cluster_config: ClusterConfig) -> str:
        toml_fields = {f.name for f in fields(ServerConfig.AutoQuorumServerToml)}
        shared_fields = {k: v for k, v in asdict(self).items() if k in toml_fields}
        cluster_shared_fields = {
            k: v for k, v in asdict(cluster_config).items() if k in toml_fields
        }
        server_toml = ServerConfig.AutoQuorumServerToml(
            location=self.instance_config.zone,
            **shared_fields,
            **cluster_shared_fields,
        )
        server_toml_str = toml.dumps(asdict(server_toml))
        return server_toml_str


@dataclass(frozen=True)
class ClientConfig:
    instance_config: InstanceConfig
    server_id: int
    requests: list[RequestInterval]
    summary_filepath: str
    output_filepath: str
    rust_log: str = "info"

    @dataclass(frozen=True)
    class AutoQuorumClientToml:
        cluster_name: str
        location: str
        server_id: int
        requests: list[RequestInterval]
        summary_filepath: str
        output_filepath: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        if self.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {self.server_id}. It must be greater than 0."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def with_updated(self, **kwargs) -> ClientConfig:
        new_config = replace(self, **kwargs)
        return new_config

    def generate_client_toml(self, cluster_config: ClusterConfig) -> str:
        toml_fields = {f.name for f in fields(ClientConfig.AutoQuorumClientToml)}
        shared_fields = {k: v for k, v in asdict(self).items() if k in toml_fields}
        cluster_shared_fields = {
            k: v for k, v in asdict(cluster_config).items() if k in toml_fields
        }
        client_toml = ClientConfig.AutoQuorumClientToml(
            location=self.instance_config.zone,
            **shared_fields,
            **cluster_shared_fields,
        )
        client_toml_str = toml.dumps(asdict(client_toml))
        return client_toml_str


@dataclass(frozen=True)
class FlexibleQuorum:
    read_quorum_size: int
    write_quorum_size: int


@dataclass(frozen=True)
class RequestInterval:
    duration_sec: int
    requests_per_sec: int
    read_ratio: float
