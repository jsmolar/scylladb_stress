import argparse
import dataclasses
import re
import statistics
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StressResult:
    """Data class to store stress test results"""

    op_rate: Optional[float] = None
    latency_mean: Optional[float] = None
    latency_99th_percentile: Optional[float] = None
    latency_max: Optional[float] = None


class Runner:

    @staticmethod
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

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode()


class Parser:
    """Parser class for stress test results"""

    def __init__(self, data):
        self.data = self._sanitize(data)

    @staticmethod
    def _sanitize(data):
        """Removes unused part of output, leaves only Results"""
        return re.sub(r".*Results:", "Results:", data, flags=re.DOTALL)

    def parse(self):
        """
        Parses output from stress test. Fields defined in `StressResult` are used as keys in regex
        (underscores are swapped for spaces, to reflect real keys).
        """
        result = StressResult()
        for field in dataclasses.fields(StressResult):
            field_name = field.name.replace("_", " ")
            value = re.search(
                rf"\n{field_name}\s*:\s*([\d,.]+)", self.data, re.IGNORECASE
            ).group(1)
            value = float(value.replace(",", ""))
            setattr(result, field.name, value)

        return result


@dataclass
class StressRun:
    """Data class to store information about stress test run"""

    result: StressResult
    start_time: datetime
    end_time: datetime

    @property
    def duration(self) -> float:
        return (self.end_time - self.start_time).total_seconds()


class StressAnalysis:
    """Class that analyse Cassandra Stress Test"""

    def __init__(self):
        self.threads = []
        self.results = []
        self.done = False

    def _start_test(self, node_ip, duration):
        """Calls runner to run cassandra stress test and saves parsed results and time marks"""
        start_time = datetime.now()
        stress_result = Runner.run(node_ip, duration)
        end_time = datetime.now()

        result = StressRun(Parser(stress_result).parse(), start_time, end_time)
        self.results.append(result)

    def run(self, node_ip, nproc, process_runtime):
        """
        Runs stress tests in concurrent threads and waits until all threads terminates
        Args:
            :param node_ip: ip of a node from nodetool output
            :param nproc: number of concurrent stress test runs
            :param process_runtime: specify the time to run, in seconds, minutes or hours
        """
        for i in range(nproc):
            t = threading.Thread(
                target=self._start_test,
                args=(
                    node_ip,
                    process_runtime[i],
                ),
            )
            self.threads.append(t)
            t.start()

        [t.join() for t in self.threads]
        self.done = True

    @property
    def op_rate_sum(self):
        if not self.done:
            raise ValueError(
                "Cannot compute op_rate_sum because results are not available."
            )
        return sum(result.result.op_rate for result in self.results)

    @property
    def avg_latency_mean(self):
        if not self.done:
            raise ValueError(
                "Cannot compute avg_latency_mean because results are not available."
            )
        return statistics.mean(result.result.latency_mean for result in self.results)

    @property
    def avg_latency_99_percentile(self):
        if not self.done:
            raise ValueError(
                "Cannot compute avg_latency_99_percentile because results are not available."
            )
        return statistics.mean(
            result.result.latency_99th_percentile for result in self.results
        )

    @property
    def stdev_latency_max(self):
        if not self.done:
            raise ValueError(
                "Cannot compute stdev_latency_max because results are not available."
            )
        return statistics.stdev(result.result.latency_max for result in self.results)


def arg_parse():
    parse = argparse.ArgumentParser(
        description="Run cassandra-stress in multiple threads and aggregate the results."
    )
    parse.add_argument("node_ip", type=str, help="IP address of the ScyllaDB node")
    parse.add_argument(
        "nproc", type=int, help="Number of concurrent cassandra-stress test"
    )
    parse.add_argument(
        "runtime",
        type=str,
        nargs="*",
        help="Specify the time to run cassandra-stress test, in seconds, minutes or hours",
    )
    arg = parse.parse_args()

    if arg.runtime is None:
        arg.runtime = []
    if arg.nproc != len(arg.runtime):
        print(
            "Runtime args is less than number of required processes. Missing values are supplemented with `10s`"
        )
        while len(arg.runtime) < arg.nproc:
            arg.runtime.append("10s")

    return arg


def main():
    arg = arg_parse()
    anal = StressAnalysis()
    anal.run(arg.node_ip, arg.nproc, arg.runtime)

    print(f"Number of Cassandra Stress tests: {len(anal.threads)}")
    for i, process in enumerate(anal.results, start=1):
        print(
            f"Cassandra Stress test {i} "
            f"started at: {process.start_time}, "
            f"ended at: {process.start_time}, "
            f"and total duration is: {process.duration}"
        )
    print(f"Aggregation of Op rate: {anal.op_rate_sum}")
    print(f"Average of Latency mean: {anal.avg_latency_mean}")
    print(f"Average of Latency 99th percentile: {anal.avg_latency_99_percentile}")
    print(
        f"Standard deviation calculation of all Latency max results: {anal.stdev_latency_max}"
    )


if __name__ == "__main__":
    main()
