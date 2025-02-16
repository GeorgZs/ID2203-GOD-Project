import subprocess
from pathlib import Path

from gcp_cluster import GcpCluster, InstanceConfig
from gcp_ssh_client import GcpClusterSSHClient
from omnipaxos_configs import *


class OmnipaxosCluster:
    """
    Orchestration class for managing a Omnipaxos cluster on GCP.

    This class automates the setup, deployment, and management of Omnipaxos servers and clients
    on Google Cloud Platform (GCP) instances. It abstracts the steps required to push Docker images,
    start instances, configure and launch Omnipaxos containers, and manage logs and shutdown operations.

    Deployment Steps:
    1.   Configure project settings (See `./scripts/project_env.sh`). Configure gcloud authentication (see `./scripts/auth.sh`).
    2.   Push Omnipaxos server and client Docker images to GCR (Google Cloud Registry).
         See `./scripts/push-server-image.sh` and `./scripts/push-client-image.sh` for details.
    3-4. `__init__` Initializes the cluster by creating GCP instances (using the GcpCluster class) for Omnipaxos servers and clients.
         The instances will run startup scripts (passed via ClusterConfig) to configure Docker for the gcloud OS login user and assign
         DNS names to the servers.
    5-6. Use `run()` to SSH into client and server instances, pass configuration files,
         and run Docker containers from the artifact registry. This also waits for client processes to finish
         and then pulls logs from the server and client GCP instances
    7.   Use `shutdown()` to shut down the GCP instances (or leave them running for reuse).
    """

    _cluster_config: ClusterConfig
    _gcp_cluster: GcpCluster
    _gcp_ssh_client: GcpClusterSSHClient

    def __init__(self, project_id: str, cluster_config: ClusterConfig):
        self._cluster_config = cluster_config
        instance_configs = [
            c.instance_config for c in cluster_config.server_configs.values()
        ]
        instance_configs.extend(
            [c.instance_config for c in cluster_config.client_configs.values()]
        )
        self._gcp_cluster = GcpCluster(project_id, instance_configs)
        self._gcp_ssh_client = GcpClusterSSHClient(self._gcp_cluster)


    def run(self, logs_directory: Path, pull_images: bool = False):
        """
        Starts servers and clients but only waits for client processes to exit before
        pulling logs.
        """
        server_process_ids = self._start_servers(pull_images=pull_images)
        client_process_ids = self._start_clients(pull_images=pull_images)
        self._gcp_ssh_client.await_processes_concurrent(client_process_ids)
        self._gcp_ssh_client.stop_processes(server_process_ids)
        self._get_logs(logs_directory)
        self._gcp_ssh_client.clear()


    def change_cluster_config(self, **kwargs):
        self._cluster_config = self._cluster_config.with_updated(**kwargs)


    def change_server_config(self, server_id: int, **kwargs):
        server_config = self._get_server_config(server_id)
        self._cluster_config.server_configs[server_id] = server_config.with_updated(
            **kwargs
        )


    def change_client_config(self, client_id: int, **kwargs):
        client_config = self._get_client_config(client_id)
        self._cluster_config.client_configs[client_id] = client_config.with_updated(
            **kwargs
        )


    def shutdown(self):
        instance_names = [
            c.instance_config.name for c in self._cluster_config.server_configs.values()
        ]
        client_names = [
            c.instance_config.name for c in self._cluster_config.client_configs.values()
        ]
        instance_names.extend(client_names)
        self._gcp_cluster.shutdown_instances(instance_names)


    def _start_servers(self, pull_images: bool = False) -> list[str]:
        process_ids = []
        for id, config in self._cluster_config.server_configs.items():
            process_id = f"server-{id}"
            instance_name = config.instance_config.name
            ssh_command = self._start_server_command(id, pull_image=pull_images)
            self._gcp_ssh_client.start_process(process_id, instance_name, ssh_command)
            process_ids.append(process_id)
        return process_ids


    def _start_clients(self, pull_images: bool = False):
        process_ids = []
        for id, config in self._cluster_config.client_configs.items():
            process_id = f"client-{id}"
            instance_name = config.instance_config.name
            ssh_command = self._start_client_command(id, pull_image=pull_images)
            self._gcp_ssh_client.start_process(process_id, instance_name, ssh_command)
            process_ids.append(process_id)
        return process_ids


    def _get_logs(self, dest_directory: Path):
        # Make sure destination directory exists
        subprocess.run(["mkdir", "-p", dest_directory])
        instance_results_dir = "./results"
        processes = []
        for config in self._cluster_config.server_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, instance_results_dir, dest_directory
            )
            processes.append(scp_process)
        for config in self._cluster_config.client_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, instance_results_dir, dest_directory
            )
            processes.append(scp_process)
        successes = 0
        for process in processes:
            process.wait()
            if process.returncode == 0:
                successes += 1
        print(f"Collected logs from {successes} instances")


    def _get_server_config(self, server_id: int) -> ServerConfig:
        server_config = self._cluster_config.server_configs.get(server_id)
        if server_config is None:
            raise ValueError(f"Server {server_id} doesn't exist")
        return server_config


    def _get_client_config(self, client_id: int) -> ClientConfig:
        client_config = self._cluster_config.client_configs.get(client_id)
        if client_config is None:
            raise ValueError(f"Client {client_id} doesn't exist")
        return client_config


    def _start_server_command(self, server_id: int, pull_image: bool = False) -> str:
        config = self._get_server_config(server_id)
        container_name = "server"
        image_path = self._cluster_config.server_image
        instance_config_location = "~/server-config.toml"
        container_config_location = "/home/$(whoami)/server-config.toml"
        instance_output_dir = "./results"
        container_output_dir = "/app"
        stderr_pipe = f"{instance_output_dir}/xerr-server-{config.server_id}.log"
        server_config_toml = config.generate_server_toml(self._cluster_config)

        # pull_command = f"docker pull {container_image_location} > /dev/null"
        pull_command = f"docker pull {image_path}"
        kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
        gen_config_command = f"mkdir -p {instance_output_dir} && echo -e '{server_config_toml}' > {instance_config_location}"
        docker_command = f"""docker run \\
            --name {container_name} \\
            -p 800{config.server_id}:800{config.server_id} \\
            --env RUST_LOG={config.rust_log} \\
            --env CONFIG_FILE="{container_config_location}" \\
            -v {instance_config_location}:{container_config_location} \\
            -v {instance_output_dir}:{container_output_dir} \\
            --rm \\
            "{image_path}" \\
            2> {stderr_pipe}"""
        if pull_image:
            full_command = f"{kill_prev_container_command}; {pull_command}; {gen_config_command} && {docker_command}"
        else:
            # Add a sleep to help avoid connecting to any currently shutting down servers.
            full_command = f"{kill_prev_container_command}; sleep 1; {gen_config_command} && {docker_command}"
        return full_command


    def _start_client_command(self, client_id: int, pull_image: bool = False) -> str:
        config = self._get_client_config(client_id)
        container_name = "client"
        image_path = self._cluster_config.client_image
        instance_config_location = "~/client-config.toml"
        container_config_location = f"/home/$(whoami)/client-config.toml"
        instance_output_dir = "./results"
        container_output_dir = "/app"
        client_config_toml = config.generate_client_toml(self._cluster_config)

        # pull_command = f"docker pull gcr.io/{container_image_location} > /dev/null"
        # kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
        pull_command = f"docker pull {image_path}"
        kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
        gen_config_command = f"mkdir -p {instance_output_dir} && echo -e '{client_config_toml}' > {instance_config_location}"
        docker_command = f"""docker run \\
        --name={container_name} \\
        --rm \\
        --env RUST_LOG={config.rust_log} \\
        --env CONFIG_FILE={container_config_location} \\
        -v {instance_config_location}:{container_config_location} \\
        -v {instance_output_dir}:{container_output_dir} \\
        {image_path}"""
        if pull_image:
            full_command = f"{kill_prev_container_command}; {pull_command}; {gen_config_command} && {docker_command}"
        else:
            # Add a sleep to help avoid connecting to any currently shutting down servers.
            full_command = f"{kill_prev_container_command}; sleep 1; {gen_config_command} && {docker_command}"
        return full_command


class OmnipaxosClusterBuilder:
    """
    Builder class for defining and validating configurations to start a OmnipaxosCluster.
    Relies on environment variables from `./scripts/project_env.sh` to configure settings.
    """

    def __init__(self, cluster_name: str) -> None:
        env_keys = [
            "PROJECT_ID",
            "SERVICE_ACCOUNT",
            "OSLOGIN_USERNAME",
            "OSLOGIN_UID",
            "CLIENT_DOCKER_IMAGE_NAME",
            "SERVER_DOCKER_IMAGE_NAME",
        ]
        env_vals = {}
        process = subprocess.run(['bash', '-c', 'source ./scripts/project_env.sh && env'], check=True, stdout= subprocess.PIPE, text=True)
        for line in process.stdout.split("\n"):
            (key, _, value) = line.partition("=")
            if key in env_keys:
                env_keys.remove(key)
                env_vals[key] = value
        for key in env_keys:
            raise ValueError(f"{key} env var must be set. Sourcing from ./scripts/project_env.sh")
        self.cluster_name = f"{cluster_name}"
        self._project_id = env_vals["PROJECT_ID"]
        self._service_account = env_vals["SERVICE_ACCOUNT"]
        self._gcloud_ssh_user = env_vals["OSLOGIN_USERNAME"]
        self._gcloud_oslogin_uid = env_vals['OSLOGIN_UID']
        self._server_docker_image_name = env_vals["SERVER_DOCKER_IMAGE_NAME"]
        self._client_docker_image_name = env_vals["CLIENT_DOCKER_IMAGE_NAME"]
        self._server_configs: dict[int, ServerConfig] = {}
        self._client_configs: dict[int, ClientConfig] = {}
        # Cluster-wide settings
        self._initial_leader: int | None = None
        self._initial_quorum: FlexibleQuorum | None = None

    def server(
        self,
        server_id: int,
        zone: str,
        machine_type: str = "e2-standard-8",
        rust_log: str = "info",
    ):
        if server_id in self._server_configs.keys():
            raise ValueError(f"Server {server_id} already exists")
        instance_config = InstanceConfig(
            f"{self.cluster_name}-server-{server_id}-{self._gcloud_oslogin_uid}",
            zone,
            machine_type,
            self._docker_startup_script(self._server_docker_image_name),
            dns_name=f"{self.cluster_name}-server-{server_id}",
            service_account=self._service_account,
        )
        server_config = ServerConfig(
            instance_config=instance_config,
            server_id=server_id,
            num_clients=0,
            output_filepath=f"server-{server_id}.json",
            rust_log=rust_log,
        )
        self._server_configs[server_id] = server_config
        return self

    def client(
        self,
        server_id: int,
        zone: str,
        requests: list[RequestInterval] = [],
        machine_type: str = "e2-standard-2",
        rust_log: str = "info",
    ):
        if server_id in self._client_configs.keys():
            raise ValueError(f"Client {server_id} already exists")
        instance_config = InstanceConfig(
            f"{self.cluster_name}-client-{server_id}-{self._gcloud_oslogin_uid}",
            zone,
            machine_type,
            self._docker_startup_script(self._client_docker_image_name),
            service_account=self._service_account,
        )
        client_config = ClientConfig(
            instance_config=instance_config,
            server_id=server_id,
            requests=requests,
            summary_filepath=f"client-{server_id}.json",
            output_filepath=f"client-{server_id}.csv",
            rust_log=rust_log,
        )
        self._client_configs[server_id] = client_config
        return self

    def initial_leader(self, initial_leader: int):
        self._initial_leader = initial_leader
        return self

    def initial_quorum(self, flex_quorum: FlexibleQuorum):
        self._initial_quorum = flex_quorum
        return self

    def build(self) -> OmnipaxosCluster:
        # Edit server configs based on cluster-wide settings
        for server_id, server_config in self._server_configs.items():
            client_configs = self._client_configs.values()
            server_id_matches = sum(
                1 for _ in filter(lambda c: c.server_id == server_id, client_configs)
            )
            total_matches = server_id_matches
            self._server_configs[server_id] = replace(
                server_config, num_clients=total_matches
            )

        if self._initial_leader is None:
            raise ValueError("Need to set cluster's initial leader")

        cluster_config = ClusterConfig(
            cluster_name=self.cluster_name,
            nodes=sorted(self._server_configs.keys()),
            initial_leader=self._initial_leader,
            initial_flexible_quorum=self._initial_quorum,
            server_configs=self._server_configs,
            client_configs=self._client_configs,
            client_image=self._client_docker_image_name,
            server_image=self._server_docker_image_name,
        )
        return OmnipaxosCluster(self._project_id, cluster_config)

    def _docker_startup_script(
        self,
        image_path: str,
    ) -> str:
        """
        Generates the startup script for a Omnipaxos client on a GCP instance.

        This script is executed during instance creation and configures Docker to use GCR.
        For debugging, SSH into the instance and run `sudo journalctl -u google-startup-scripts.service`.
        """
        user = self._gcloud_ssh_user
        return f"""#! /bin/bash
# Ensure OS login user is setup
useradd -m {user}
mkdir -p /home/{user}
chown {user}:{user} /home/{user}

# Configure Docker credentials for the user
sudo -u {user} docker-credential-gcr configure-docker --registries=gcr.io
sudo -u {user} echo "https://gcr.io" | docker-credential-gcr get
sudo groupadd docker
sudo usermod -aG docker {user}

# Pull the container as user
sudo -u {user} docker pull "{image_path}"
"""
