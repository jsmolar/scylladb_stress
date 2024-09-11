import dataclasses
import re
import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass
class StressResult:
    """Data class to store stress test results"""

    op_rate:  Optional[float] = None
    latency_mean: Optional[float] = None
    latency_99th_percentile: Optional[float] = None
    latency_max: Optional[float] = None


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


class Parser:
    """Parser class for stress test results"""

    def __init__(self, out):
        self.out = out
        self._sanitize()

    def _sanitize(self):
        """Removes unused part of output, leaves only Results"""
        self.out = re.sub(r".*Results:", "Results:", self.out, flags=re.DOTALL)

    def parse(self):
        """
        Parses output from stress test. Fields defined in `StressResult` are used as keys in regex
        (underscores are swapped for spaces, to reflect real keys).
        """
        result = StressResult()
        for field in dataclasses.fields(StressResult):
            field_name = field.name.replace("_", " ")
            value = re.search(rf"\n{field_name}\s*:\s*([\d,.]+)", self.out, re.IGNORECASE).group(1)
            value = float(value.replace(",", ""))
            setattr(result, field.name, value)

        return result


def main():
    stress_result = Runner.run("172.17.0.2")
    Parser(stress_result).parse()


if __name__ == "__main__":
    main()
