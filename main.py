import argparse

from analysis import StressAnalysis


def arg_parse():
    parse = argparse.ArgumentParser(
        description="Run cassandra-stress in multiple threads and aggregate the results."
    )
    parse.add_argument("node_ip", type=str, help="IP address of the ScyllaDB node")
    parse.add_argument(
        "nproc", type=int, help="Number of concurrent cassandra-stress tests to run"
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
            f"ended at: {process.end_time}, "
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
