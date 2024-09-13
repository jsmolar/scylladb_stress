# ScyllaDB Stress Test Analyzer

## Prerequisites

1. **Deploy Scylla Cluster**:
   - Follow the steps to deploy a single-node Scylla cluster:  
     [Scylla on Docker Hub](https://hub.docker.com/r/scylladb/scylla/)  
     Or simply run the following command to start a Scylla container:
     ```bash
     docker run --rm --name some-scylla --hostname some-scylla -d scylladb/scylla --smp 1
     ```

2. **Pull Cassandra-Stress Tool**:
   - We will be using the `cassandra-stress` tool from the ScyllaDB Docker image. Pull the image with the following command:
     ```bash
     docker pull scylladb/cassandra-stress
     ```

## Running the Code

1. **Get the Node IP**:
   - Retrieve the node IP address using `nodetool`:
     ```bash
     docker exec -it some-scylla nodetool status
     ```
   - The node IP will be listed under the "Address" column.

2. **Run Stress Test Analysis**:
   - Run the stress test analysis script by executing:
     ```bash
     python stress_analysis.py {node_ip_from_nodetool_output} {number_of_concurrent_tests} {list_of_stress_command_durations}
     ```
   - **Parameters**:
     - `node_ip_from_nodetool_output`: The IP address retrieved from `nodetool status`.
     - `number_of_concurrent_tests`: The number of concurrent stress tests to run.
     - `list_of_stress_command_durations`: Specify the runtime duration for each stress test (e.g., `10s`, `2m`, `1h`).

3. **Note**:
   - The **duration** must specify the time in seconds (`s`), minutes (`m`), or hours (`h`).
   - If the number of durations provided is fewer than the `number_of_concurrent_tests`, the script will automatically supply `10s` for the missing durations.
   - If more durations are provided than needed, the script will ignore the extra ones.