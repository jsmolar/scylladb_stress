import subprocess


class Runner:

    @staticmethod
    def run(node_ip):
        """Runs cassandra-stress against ScyllaDB node"""
        cmd = [
            "docker", "run", "--rm", "--entrypoint", "cassandra-stress",
            "scylladb/cassandra-stress", "write", "duration=10s",
            "-rate", "threads=10", "-node", node_ip
        ]

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode()


def main():
    Runner.run("172.17.0.2")


if __name__ == "__main__":
    main()
