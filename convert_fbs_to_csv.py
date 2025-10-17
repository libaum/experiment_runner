#!/usr/bin/env python3
import os
import csv
from typing import List
# import argparse
import config as cfg
import utils

# Import the generated Flatbuffers classes.
from PartitionInfo import PartitionLog
# import fbs.PartitionInfo.PartitionLog as PartitionLog


def parse_flatbuffer_file(file_path, alg_name, set_name):
    """
    Reads a Flatbuffers binary file and extracts the desired fields
    to create a row in CSV format.
    """
    with open(file_path, 'rb') as f:
        buf = f.read()

    # Get the root object (PartitionLog) from the buffer.
    partition_log = PartitionLog.PartitionLog.GetRootAsPartitionLog(buf, 0)

    # 1. Algorithm name: Taken from the parameter.
    # (Here, the passed value is used.)

    # 2. Graph: From GraphMetadata.filename, we extract the pure graph name.
    graph_meta = partition_log.GraphMetadata()
    graph_name = graph_meta.Filename().decode('utf-8') if graph_meta.Filename() else ""



    # 3. Seed: From PartitionConfiguration
    config = partition_log.PartitionConfiguration()
    seed = config.Seed()

    # Num partitions:
    k = config.K()

    if not k in cfg.SET_CONFIG[set_name]["k"]:
        # print(f"Warning: k={k} is not in the set configuration for {set_name}. Skipping this entry.")
        return None



    # 4. Runtime: Here we use the total time value from RunTime.total_time.
    runtime_table = partition_log.Runtime()
    runtime = runtime_table.TotalTime()

    # 5. Memory: From MemoryConsumption.max_rss
    mem = partition_log.MemoryConsumption()
    memory = mem.MaxRss()

    # 6. Solution Quality: Here we use the Balance value from PartitionMetrics.
    metrics = partition_log.Metrics()
    edge_cut = metrics.EdgeCut()

    # 7. Edge Cut Ratio (ECR): Calculated as the edge cut divided by the total number of edges.
    ECR = metrics.EdgeCut() / graph_meta.NumEdges()

    return utils.produce_result(
        alg_name=alg_name,
        graph=graph_name,
        seed=seed,
        k=k,
        runtime=runtime,
        memory=memory,
        edge_cut=edge_cut,
        ECR=ECR
    )


def main(tasklist: List[utils.Task], alg_name, set_name, ordering, max_cores):

    # Compose input directory (expand the ~ path)

    utils.fprint(f"Converting Flatbuffers files to CSV for {alg_name} on {set_name} with ordering {ordering} and max cores {max_cores}")

    fbs_dir = utils.get_fbs_output_dir(set_name, ordering, alg_name, max_cores)
    processed_output_dir = utils.get_processed_output_dir(set_name, ordering, max_cores)
    output_csv = os.path.join(processed_output_dir, f"{alg_name}.csv")

    utils.fprint(f"Input directory: {fbs_dir}")
    utils.fprint(f"Output CSV file: {output_csv}")

    os.makedirs(processed_output_dir, exist_ok=True)
    # Define the CSV columns (header)

    data_rows = []

    if not os.path.exists(fbs_dir):
        print(f"The input directory '{fbs_dir}' does not exist!")
        return

    for task in tasklist:
        successful = False
        fbs_target_path = task.fbs_target_path
        if os.path.exists(fbs_target_path):
            try:
                data = parse_flatbuffer_file(fbs_target_path, alg_name, set_name)
                if data is not None:
                    data_rows.append(data)
                    successful = True
            except Exception as e:
                print(f"Error processing {fbs_target_path}: {e}")
                pass

        if not successful:
            data_rows.append(utils.produce_failed_result(alg_name, task.raw_graph_name, task.k))


    # Sort the data by the "k" value.
    data_rows.sort(key=lambda x: x["k"])
    data_rows.sort(key=lambda x: x["graph"])

    # Write the collected data to the CSV file.
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=utils.fieldnames)
        writer.writeheader()
        writer.writerows(data_rows)

    print(f"CSV file '{output_csv}' was created successfully.")

# if __name__ == "__main__":
#         # Parse args
#     parser = argparse.ArgumentParser(description="Script to create a CSV from Flatbuffers files.")
#     parser.add_argument("--alg_name", required=True, help="Name of the algorithm (also used as part of the input path)")
#     parser.add_argument("--set_name", required=True, help="Name of the dataset (also used as part of the input path)")
#     parser.add_argument("--ordering", required=True, help="Ordering to use [natural, random]")
#     parser.add_argument("--max_cores", required=True, help="Max cores")

#     args = parser.parse_args()
#     alg_name = args.alg_name
#     set_name = args.set_name
#     ordering = args.ordering
#     max_cores = args.max_cores

#     main(alg_name, set_name, ordering, max_cores)