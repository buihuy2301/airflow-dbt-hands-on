import json
from pprint import pprint

manifest_path = "/Users/huybq/Documents/dev-dir/dbt/dbt_demo/target/manifest.json"

tag = "loans"

def dbt_docker_run(
    manifest_path: str,
    tag: str = None,
):
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)
        nodes = {node_id : node_info for (node_id, node_info) in manifest["nodes"].items() if not node_id.startswith(("source","analysis"))}

    # nodes = update_dependencies(nodes)
    return_node = {}
    dbt_tasks = {}
    for node_id, node_info in nodes.items():
        if tag in node_info["tags"] or tag is None:
            return_node[node_id] = node_info
            command = dbt_dynamic_command(node_id)
            dbt_tasks[node_id] = command
    return dbt_tasks, return_node, 

def dbt_dynamic_command(node_name, node_type):
    """_summary_

    Args:
        node_info (_type_): _description_
    """
    if node_type.startswith("test"):
        return f"dbt test --full-refresh {node_name}"
    elif node_type.startswith("seed"):
        return f'dbt seed --full-refresh --select "{node_name}"'
    elif node_type.startswith("snapshots"):
        return f"dbt snapshot --select {node_name}"
    else:
        return f"dbt run --models {node_name}"



def update_dependencies(a):
    # Find the test materialized item's dependency node
    test_dependency_nodes = {}
    test_keys = []

    for key, value in a.items():
        if value['config']['materialized'] == 'test':
            for node in value['depends_on']['nodes']:
                test_dependency_nodes[node] = key
            test_keys.append(key)

    # If test materialized items are found, update dependencies for table materialized items
    if test_dependency_nodes:
        for key, value in a.items():
            if value['config']['materialized'] == 'table':
                updated_nodes = []
                for node in value['depends_on']['nodes']:
                    if node in test_dependency_nodes:
                        updated_nodes.append(test_dependency_nodes[node])
                    else:
                        updated_nodes.append(node)
                value['depends_on']['nodes'] = updated_nodes

    return a

dbt_tasks, nodes = dbt_docker_run(manifest_path)
for node_id, node_info in nodes.items():
    if upstream_nodes := node_info["depends_on"].get("nodes"):
        for upstream_node in upstream_nodes:
            print(f"{dbt_tasks.get(upstream_node)} >> {dbt_tasks.get(node_id)}")
    else:
        print(f"{dbt_tasks.get(node_id)}")

pprint(nodes)

