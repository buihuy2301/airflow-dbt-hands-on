import json
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator


def dbt_docker_run(
    dag,
    dbt_image: str,
    dbt_host_path: str,
    target_mouth_path: str,
    mount_type: str,
    manifest_path: str,
    docker_url: str,
    network_mode: str,
    tag: str = None,
):
    """
    Runs dbt tasks in Docker containers.

    Args:
        dag (_type_): Airflow DAG object.
        dbt_image (str): Docker image to use for dbt.
        dbt_host_path (str): Path on the host to mount.
        target_mount_path (str): Path in the container to mount the host path.
        mount_type (str): Type of mount (e.g., 'bind').
        manifest_path (str): Path to the dbt manifest file.
        docker_url (str): URL of the Docker daemon.
        network_mode (str): Network mode for the Docker containers.
        tag (str, optional): Tag to filter dbt nodes. Defaults to None.

    Returns:
        tuple: A dictionary of dbt tasks and the nodes from the manifest.
    """
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
        nodes = {
            node_id: node_info
            for (node_id, node_info) in manifest["nodes"].items()
            if not node_id.startswith(("source", "analysis"))
        }
    nodes = update_dependencies(nodes)
    return_node = {}
    dbt_tasks = {}
    for node_id, node_info in nodes.items():
        if tag is None or tag in node_info["tags"]:
            return_node[node_id] = node_info
            command = dbt_dynamic_command(node_name=node_info['name'], node_type=node_info['resource_type'])
            dbt_tasks[node_id] = DockerOperator(
                task_id=".".join(
                    [
                        node_info["resource_type"],
                        # node_info["package_name"],
                        node_info["name"],
                    ]
                ),
                image=dbt_image,
                api_version="auto",
                docker_url=docker_url,
                command=command,
                mounts=[
                    Mount(
                        source=dbt_host_path,
                        target=target_mouth_path,
                        type=mount_type,
                    )
                ],
                network_mode=network_mode,
                auto_remove = 'success',
                dag=dag,
            )

    return dbt_tasks, return_node


def dbt_dynamic_command(node_name, node_type):
    """_summary_

    Args:
        node_info (_type_): _description_
    """
    if node_type.startswith("test"):
        return f'dbt test --select "{node_name}"'
    elif node_type.startswith("seed"):
        return f'dbt seed --full-refresh --select "{node_name}"'
    elif node_type.startswith("snapshots"):
        return f"dbt snapshot --select {node_name}"
    else:
        return f"dbt run --models {node_name}"


def update_dependencies(nodes):
    # Create a mapping of table dependencies to their corresponding test items
    table_to_tests = {}

    for key, value in nodes.items():
        if value['config']['materialized'] == 'test':
            for node in value['depends_on']['nodes']:
                if node not in table_to_tests:
                    table_to_tests[node] = []
                table_to_tests[node].append(key)

    # Update the dependencies for items materialized as 'table'
    for key, value in nodes.items():
        if value['config']['materialized'] == 'table':
            updated_nodes = []
            for node in value['depends_on']['nodes']:
                if node in table_to_tests:
                    updated_nodes.extend(table_to_tests[node])
                else:
                    updated_nodes.append(node)
            value['depends_on']['nodes'] = updated_nodes

    return nodes
