import os
import subprocess
import csv
import shutil
import concurrent.futures
import threading
import time
import tempfile
from functools import partial
from typing import List, Dict, Callable, Optional
import psutil
import utils as utils

import config as cfg
import convert_fbs_to_csv

def get_runtime_and_memory_limit_prefix(max_cores):
    limit_prefix = ""
    if cfg.RUNTIME_LIMIT_ACTIVE:
        limit_prefix += f"timeout --signal=TERM --kill-after=5s {cfg.RUNTIME_LIMIT_IN_HOURS}h "
    if cfg.MEMORY_LIMIT_ACTIVE:
        limit_prefix += f'prlimit --as=$(({int(cfg.MEMORY_ON_MACHINE / max_cores)}*1024*1024*1024)) -- '
    return limit_prefix


class AlgorithmRunner:
    # Configuration parameters for GNU parallel
    MAX_PARALLEL_JOBS = 4  # Leave one core for system

    def __init__(self, algo_type):
        """
        Initializes the AlgorithmRunner.
        
        Args:
            algo_type (str): Type of the algorithm ('cuttana', 'heistream', or another algorithm name)
        """
        self.algo_type = algo_type
        self.program = os.path.expanduser(f"~/deploy/{algo_type}")


    def run(self, set_name, ordering, hyperparam_dict, param_dict, alg_name, max_cores: Optional[int] = MAX_PARALLEL_JOBS, quick_test=False):
        """
            Runs the algorithm with the given parameters.
            Creates a temporary file with commands for GNU parallel.
            
        Args:
                set_name (str): Name of the graph set
                ordering (str): Type of ordering (e.g. 'natural', 'random')
                hyperparam_dict (dict): Dictionary of hyperparameters
                param_dict (dict): Dictionary of parameters
                alg_name (str): Name of the algorithm for output files
                quick_test (bool, optional): Run a quick test with limited data. Defaults to False.
                parallel (bool, optional): Run in parallel mode. Defaults to False.
        """
        if self.algo_type.startswith("cuttana"):
            return self._run_cuttana_parallel(set_name, ordering, hyperparam_dict, param_dict, alg_name, max_cores, quick_test)
        else:
            # Für HEIStream und andere Algorithmen die kombinierte Parallelisierungsfunktion
            return self._run_algo_parallel(set_name, ordering, hyperparam_dict, param_dict, alg_name, max_cores, quick_test)

    def _create_command_file(self, commands: List[str], prefix: str = "parallel_cmds") -> str:
        """
        Creates a temporary file with commands for GNU parallel.
        
        Args:
            commands: List of command strings to execute
            prefix: Prefix for the temporary file name
            
        Returns:
            Path to the created temporary file
        """
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, prefix=prefix, suffix='.txt')
        for cmd in commands:
            temp_file.write(f"{cmd}\n")
        temp_file.close()
        return temp_file.name

    def _run_parallel_commands(self, commands: List[str], file_moves, max_jobs) -> bool:
        """
        Execute commands using GNU parallel.
        
        Args:
            commands: List of command strings to execute
            max_jobs: Maximum number of parallel jobs (defaults to MAX_PARALLEL_JOBS)
            
        Returns:
            True if all commands succeeded, False otherwise
        """
        if not commands:
            return True

        # Create temporary file with commands
        cmd_file = self._create_command_file(commands)

        try:
            # Construct GNU parallel command
            parallel_cmd = [
                "parallel",
                f"--jobs={max_jobs}",
                "--line-buffer",  # Preserve line order in output
                "--tag",          # Tag output with job number
                "--halt", "never", # Continue even if some jobs fail
                ":::"              # Separator for arguments
                # "--arg-file", cmd_file  # Read commands from file instead of command line

            ]

            # Add commands from file
            with open(cmd_file, 'r') as f:
                command_lines = [line.strip() for line in f.readlines() if line.strip()]

            parallel_cmd.extend(command_lines)

            if cfg.DEBUG:
                utils.fprint(f"Running GNU parallel with {len(command_lines)} commands, max {max_jobs} jobs")
                # utils.fprint(f"{' '.join(parallel_cmd)}")

            # Execute GNU parallel
            ret = subprocess.run(parallel_cmd, text=True)

            if ret.returncode != 0:
                utils.fprint(f"WARNING: Some parallel jobs failed (return code: {ret.returncode})")
                return False

            return True

        finally:
            # Clean up temporary file
            if os.path.exists(cmd_file):
                os.unlink(cmd_file)

    def _run_algo_parallel(self, set_name, ordering, hyperparam_dict, param_dict, alg_name, max_cores, quick_test):
        """Generalized parallel implementation using GNU parallel"""

        # Flags für algorithmenspezifisches Verhalten
        is_heistream = "heistream" in self.algo_type

        # Determine the output folder.
        fbs_output_folder = utils.get_fbs_output_dir(set_name, ordering, alg_name, max_cores)
        # output_folder = os.path.expanduser(os.path.join(f"{cfg.BASE_DIR_OUTPUT}/fbs_{set_name}/{ordering}", f"{max_cores}core{'' if max_cores == 1 else 's'}", alg_name))
        os.makedirs(fbs_output_folder, exist_ok=True)
        os.chdir(fbs_output_folder)

        utils.fprint(f"Output folder: {fbs_output_folder}")

        graph_set = utils.read_graph_set(set_name)

        # Set v_k and additional_args based on the set name.
        set_config = cfg.SET_CONFIG.get(set_name, cfg.SET_CONFIG["default"])
        v_k = set_config["k"]

        # Nur für new_algo notwendig
        additional_args_str = "" if is_heistream else " ".join(set_config["additional_args"])

        if quick_test:
            graph_set = graph_set[:1]
            v_k = v_k[:1]

        # Prepare all commands for GNU parallel
        commands_to_execute = []
        file_moves = []  # Store file move operations for after execution


        tasklist: list[utils.Task] = []

        for k in v_k:
            for graph in graph_set:
                raw_graph_name = utils.get_graph_name(graph, ordering) # Add ordering if not natural
                stream_buffer = hyperparam_dict.get('stream_buffer', '32768' if is_heistream else '16384')
                max_pq_size = hyperparam_dict.get('max_pq_size', '131072')

                if hyperparam_dict.get('bb_ratio', None) is not None and not is_heistream:
                    stream_buffer = str(int((int(max_pq_size) / int(hyperparam_dict['bb_ratio']))))

                if is_heistream:
                    fbs_filename = f"{raw_graph_name}_{k}_{stream_buffer}.bin"
                    old_fbs_filename = fbs_filename
                else:
                    fbs_filename = f"{raw_graph_name}_{k}_{stream_buffer}_{max_pq_size}.bin"
                    old_fbs_filename = f"{raw_graph_name}_{k}_{stream_buffer}.bin"


                old_target_path = os.path.join(fbs_output_folder, old_fbs_filename)
                fbs_target_path = os.path.join(fbs_output_folder, fbs_filename)

                # utils.fprint(f"FBS filename: {fbs_filename}")

                graph_path = os.path.expanduser(os.path.join(cfg.BASE_DIR_GRAPHS, f"{raw_graph_name}.graph"))
                task = utils.Task(
                    k=k,
                    raw_graph_name=raw_graph_name,
                    graph_path=graph_path,
                    stream_buffer=stream_buffer,
                    max_pq_size=max_pq_size,
                    old_target_path=old_target_path,
                    fbs_target_path=fbs_target_path
                )

                tasklist.append(task)

        for task in tasklist:
            # utils.fprint(f"Task: {task}")

            fbs_target_path = task.fbs_target_path
            old_target_path = task.old_target_path
            raw_graph_name = task.raw_graph_name

            if cfg.OVERWRITE:
                if os.path.exists(fbs_target_path):
                    utils.fprint(f"File {fbs_target_path} already exists, removing it.")
                    os.remove(fbs_target_path)
                if os.path.exists(old_target_path):
                    utils.fprint(f"File {old_target_path} already exists, removing it.")
                    os.remove(old_target_path)
            else:
                # Check if the file already exists in the output folder
                if os.path.exists(fbs_target_path):
                    utils.fprint(f"File {fbs_target_path} already exists, skipping.")
                    continue
                elif os.path.exists(old_target_path):
                    utils.fprint(f"File {old_target_path} already exists, skipping.")
                    # If the old file exists, we can rename it to the new name
                    os.rename(old_target_path, fbs_target_path)
                    utils.fprint(f"Renamed {old_target_path} to {fbs_target_path}")
                    continue

            # Build command

            base_command = [
                self.program,
                task.graph_path,
                f"--k={task.k}",
                "--write_log",
            ]

            # Füge additional_args_str nur für new_algo hinzu
            if not is_heistream and additional_args_str:
                base_command.append(f"{additional_args_str}")

            for param_name, param_value in param_dict.items():
                if param_value != "":
                    base_command.append(f"--{param_name}={param_value}")
                else:
                    base_command.append(f"--{param_name}")

            for hyperparam_name, hyperparam_value in hyperparam_dict.items():
                base_command.append(f"--{hyperparam_name}={hyperparam_value}")

            command_str = " ".join(base_command)
            commands_to_execute.append(f"{get_runtime_and_memory_limit_prefix(max_cores)}{command_str}")

        if commands_to_execute:
            # Execute all commands using GNU parallel
            utils.fprint(f"Executing {len(commands_to_execute)} commands in parallel...")
            success = self._run_parallel_commands(commands_to_execute, file_moves, max_jobs=max_cores)

            if not success:
                utils.fprint("Some commands failed during parallel execution")

        # Abschluss
        if not quick_test:
            utils.fprint(f"Done running experiments for {alg_name}")
            convert_fbs_to_csv.main(tasklist, alg_name, set_name, ordering, max_cores)

        os.chdir("/home/lbaumgaertner/scripts")
        return True

    def _run_cuttana_parallel(self, set_name, ordering, hyperparam_dict, param_dict, alg_name, max_cores, quick_test):
        """Implementation of run_cuttana with GNU parallel"""

        graph_set = utils.read_graph_set(set_name)

        # Set v_k based on the set name.
        set_config = cfg.SET_CONFIG.get(set_name, cfg.SET_CONFIG["default"])
        v_k = set_config["k"]
        if quick_test:
            graph_set = graph_set[:1]
            v_k = v_k[:1]


        processed_output_dir = utils.get_processed_output_dir(set_name, ordering, max_cores)
        output_csv_path = os.path.join(processed_output_dir, f"{alg_name}.csv")

        utils.fprint(f"Output file: {output_csv_path}")

        # Create output directory if it doesn't exist
        os.makedirs(processed_output_dir, exist_ok=True)

        # Check if file exists and read existing data
        existing_data_rows = []
        if os.path.exists(output_csv_path):
            if not cfg.OVERWRITE:
                with open(output_csv_path, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        row["k"] = int(row["k"])
                        if row["k"] in v_k:
                            existing_data_rows.append(row)
            else:
                utils.fprint(f"Overwriting existing CSV file: {output_csv_path}")
                os.remove(output_csv_path)

        # Create temp_results directory structure
        temp_results_dir = os.path.join(processed_output_dir, "temp_results")
        alg_temp_dir = os.path.join(temp_results_dir, alg_name)
        os.makedirs(alg_temp_dir, exist_ok=True)

        # Prepare commands for GNU parallel
        commands_to_execute = []
        task_infos = [] # Store task information for result processing

        for graph in graph_set:
            for k in v_k:
                raw_graph_name = utils.get_graph_name(graph, ordering) # Add ordering if not natural

                # Check if results already exist in existing_data_rows
                if not cfg.OVERWRITE and any(row["graph"] == raw_graph_name and row["k"] == k for row in existing_data_rows):
                    utils.fprint(f"Results for {raw_graph_name} with k={k} already exist in CSV, skipping.")
                    continue

                # Check if results already exist in temp_results/alg_name/graph_k.txt
                temp_result_file = os.path.join(alg_temp_dir, f"{raw_graph_name}_{k}.txt")
                if os.path.exists(temp_result_file) and not cfg.OVERWRITE:
                    try:
                        with open(temp_result_file, 'r') as f:
                            content = f.read().strip()
                            if content:  # Only skip if file has actual content
                                # Parse format: "runtime memory edge_cut edge_cut_ratio"
                                parts = content.split()
                                if len(parts) >= 4:
                                    runtime, memory, edge_cut, edge_cut_ratio = parts[:4]
                                    # Append to existing_data_rows
                                    result = utils.produce_result(
                                        alg_name=alg_name,
                                        graph=raw_graph_name,
                                        seed="0",
                                        k=k,
                                        runtime=runtime,
                                        memory=memory,
                                        edge_cut=edge_cut,
                                        ECR=edge_cut_ratio
                                    )

                                    existing_data_rows.append(result)

                                    utils.fprint(f"Results for {raw_graph_name} with k={k} loaded from temp file, skipping.")
                                    continue
                                else:
                                    utils.fprint(f"WARNING: Invalid temp result format in {temp_result_file}, will re-run")
                                    os.remove(temp_result_file)
                            else:
                                # File exists but is empty - remove it and re-run
                                utils.fprint(f"Empty temp result file {temp_result_file}, will re-run")
                                os.remove(temp_result_file)
                    except Exception as e:
                        utils.fprint(f"WARNING: Failed to read temp result file {temp_result_file}: {e}, will re-run")
                        if os.path.exists(temp_result_file):
                            os.remove(temp_result_file)



                # CutTana verwendet .cut-Dateien
                graph_path = os.path.expanduser(os.path.join(cfg.BASE_DIR_GRAPHS, f"{raw_graph_name}.cut"))

                # Build command
                base_command = [
                    self.program,
                    "-d",
                    graph_path,
                    f"-p",
                    f"{k}"
                ]

                # Parameter hinzufügen
                for param_name, param_value in param_dict.items():
                    if param_value != "":
                        base_command.extend([f"-{param_name}", f"{param_value}"])
                    else:
                        base_command.append(f"-{param_name}")

                is_set_konect_cc = set_name in ["konect_cc_set", "konect_cc_set_light"]
                is_twitter = raw_graph_name in ["twitter-konect", "twitter-konect_r1"]
                for hyperparam_name, hyperparam_value in hyperparam_dict.items():
                    if is_set_konect_cc and is_twitter:
                        if hyperparam_name == "subp" and hyperparam_value == "4096": # Special case for twitter
                        # Für konect_cc_set und konect_cc_set_light immer subp=256 verwenden
                            base_command.extend([f"-{hyperparam_name}", "256"])
                        elif hyperparam_name == "dmax":
                            base_command.extend([f"-{hyperparam_name}", "100"])
                    else:
                        base_command.extend([f"-{hyperparam_name}", f"{hyperparam_value}"])

                utils.fprint(f"Running: \"{' '.join(base_command)}\"")

                # Add output redirection to temp file using tee to show output and save to file
                suffix = f' | tee {temp_result_file}'
                command_str = get_runtime_and_memory_limit_prefix(max_cores) + " ".join(base_command) + suffix
                commands_to_execute.append(command_str)
                task_infos.append({'graph_name': raw_graph_name, 'k': k, 'temp_file': temp_result_file})

        if commands_to_execute:
            utils.fprint(f"Executing {len(commands_to_execute)} Cuttana commands in parallel...")

            # Execute commands with GNU parallel
            success = self._run_parallel_commands(commands_to_execute, [], max_jobs=max_cores)

            # Read results from temp files and add to existing_data_rows
            for task in task_infos:
                parsed_result_file_successfully = False
                temp_file = task['temp_file']
                if os.path.exists(temp_file):
                    try:
                        with open(temp_file, 'r') as f:
                            line = f.readline().strip()
                            if line:
                                # Parse format: "runtime memory edge_cut edge_cut_ratio"
                                parts = line.split()
                                if len(parts) >= 4:
                                    runtime, memory, edge_cut, solution_quality = parts[:4]
                                    task_successful = "1" if str(edge_cut) != "0" else "0"
                                    existing_data_rows.append({
                                        "alg_name": alg_name,
                                        "graph": task['graph_name'],
                                        "seed": "0",
                                        "k": task['k'],
                                        "runtime": runtime,
                                        "memory": memory,
                                        "solution_quality": solution_quality,
                                        "edge_cut": edge_cut,
                                        "success": task_successful
                                    })
                                    utils.fprint(f"[SUCCESS] {task['graph_name']} k={task['k']}: Runtime={runtime}, Memory={memory}, EdgeCut={edge_cut}, Quality={solution_quality}")
                                    parsed_result_file_successfully = True
                                else:
                                    utils.fprint(f"[ERROR] Invalid result format for {task['graph_name']} k={task['k']}: '{line}'")
                    except Exception as e:
                        utils.fprint(f"[ERROR] Failed to read result file {temp_file}: {e}")
                else:
                    utils.fprint(f"[WARNING] Result file {temp_file} not found for {task['graph_name']} k={task['k']}")

                if not parsed_result_file_successfully:
                    with open(temp_file, "w") as f:
                        f.write("0 0 0 0")

                    failed_result = utils.produce_failed_result(alg_name=alg_name, graph=task['graph_name'], k=task['k'])
                    existing_data_rows.append(failed_result)
                    utils.fprint(f"[FAILURE] {task['graph_name']} k={task['k']}: No valid result found")

        # Write final consolidated CSV
        if not quick_test and existing_data_rows:
            existing_data_rows.sort(key=lambda x: (x["graph"], x["k"]))

            with open(output_csv_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=utils.fieldnames)
                writer.writeheader()
                writer.writerows(existing_data_rows)

            utils.fprint(f"CSV-file '{output_csv_path}' was successfully created for {alg_name}")


        return True
