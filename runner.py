import subprocess


def run(node_ip, duration):
    """Runs cassandra-stress against ScyllaDB node"""
    cmd = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "cassandra-stress",
        "scylladb/cassandra-stress",
        "write",
        f"duration={duration}",
        "-rate",
        "threads=10",
        "-node",
        node_ip,
    ]

    result = subprocess.run(
        cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return result.stdout.decode()
