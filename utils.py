import json
import os
import math
import sys
import commentjson
import config as cfg
from datetime import datetime

''' This module contains utility functions used by the experiment scripts. '''

def fprint(*args, **kwargs):
    ''' Print function that flushes the output. '''
    print(*args, **kwargs, flush=True)

def read_graph_set(set_name) -> list:
    ''' Reads the graph set from the file: graph_sets/<set_name> '''

    base_path = os.path.expanduser("~/scripts/exp/")
    graph_set_file = os.path.join(base_path, "graph_sets", set_name)
    try:
        with open(graph_set_file, "r") as f:
            graph_set = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Graph set file not found: {graph_set_file}")
        sys.exit(1)
    return graph_set

def get_graph_name(graph, ordering) -> str:
    ''' Returns the graph name based on the ordering. '''
    if ordering == "natural":
        return graph
    elif ordering == "random":
        return f"{graph}_r1"
    elif ordering == "random2":
        return f"{graph}_r2"
    elif ordering == "random3":
        return f"{graph}_r3"
    else:
        print(f"Invalid ordering: {ordering}")
        sys.exit(1)

def get_abbr(value) -> str:
    ''' Returns a short abbreviation for a number. '''
    try:
        num = float(value)
    except ValueError:
        return value

    if num < 1000:
        # For small numbers, show as an integer if possible.
        return str(int(num)) if num.is_integer() else str(num)
    elif True: #num < 1_000_000:
        # For numbers between 1000 and 999,999, convert to thousands.
        return f"{int(num // 1000)}k"
    else:
        # For numbers 1,000,000 and above, convert to millions.
        m = num / 1_000_000
        # If m is an exact integer, return without a decimal.
        if m.is_integer():
            return f"{int(m)}m"
        else:
            # Truncate to one decimal place.
            truncated = math.floor(m * 10) / 10
            # If the truncated value is an integer, don't show the decimal.
            if truncated.is_integer():
                return f"{int(truncated)}m"
            else:
                return f"{truncated:.1f}m"


def get_algo_name(algo, conf) -> str:
    ''' Returns the algorithm name based on the configuration. '''
    if algo.starswith("heistream"):
        stream_buffer = conf.strip()
        return f"{algo}_{get_abbr(stream_buffer)}"
    else:
        parts = conf.split()
        if len(parts) < 3:
            return None
        FPBS, SPBS, MQS = parts[:3]
        return f"{algo}_1p{get_abbr(FPBS)}_2p{get_abbr(SPBS)}_mqs{get_abbr(MQS)}"

def get_algo_name_new(algo, conf, hyp_params, param_dict) -> str:
    ''' Returns the algorithm name based on the configuration and the defined. '''
    # Split the configuration string into individual parameter values
    param_values = conf.split()

    # Take the order from the hyp_params keys (insertion order is preserved in Python 3.7+)
    hyp_param_names = list(hyp_params.keys())

    if len(param_values) < len(hyp_param_names):
        return None

    parts = []
    for i, key in enumerate(hyp_param_names):
        prefix = hyp_params[key]  # Use the mapping from hyp_params
        value = param_values[i]
        parts.append(f"{prefix}{get_abbr(value)}")

    # Add the additional parameters from params
    for param_name, param_value in param_dict.items():
        if param_name in hyp_param_names:
            continue  # Skip if already included

        if param_value != "":
            parts.append(f"{param_name}{get_abbr(param_value)}")
        else:
            parts.append(param_name)

    return f"{algo}_" + "_".join(parts)

def read_config_file(config_file) -> dict:
    config_data = {}
    try:
        with open(config_file, "r") as f:
            config_data = commentjson.load(f)
    except Exception as e:
        fprint(f"Error reading config file: {e}")
        sys.exit(1)
    return config_data

def print_configuration_new(config_data, theme) -> None:

    today = datetime.today().strftime('%Y-%m-%d')
    # Append to this file
    output_file = os.path.expanduser(f"~/outputs/{today}.json")

    # if exists read it and append
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            output_config = json.load(f)
    else:
        output_config = {
            "cfgs": []
        }

    algo = config_data.get("algo")
    # sets = config_data.get("set", {})
    orderings = config_data.get("orderings", {})
    configurations = config_data.get("configurations", [])


    for set_name, _ in orderings["natural"].items():

        # Extract enabled orderings for the current set_name
        enabled_orderings = []
        for ord_name, ord_sets in orderings.items():
            if set_name in ord_sets and ord_sets[set_name]:
                enabled_orderings.append(ord_name)

        if len(enabled_orderings) == 0:
            # If no orderings are enabled, skip this set_name
            continue

        # enabled_orderings = [ordering for ordering, ordering_enabled in orderings.items() if ordering_enabled]
        output_config["cfgs"].append({
            "folder": set_name,
            "theme": theme,
            "server": cfg.SERVER,
            "set": set_name,
            "orderings": enabled_orderings,
            "algorithms": []
        })

        for configuration in configurations:
            param_dict = configuration.get("params", {})
            hyperparams = configuration.get("hyperparams", {})
            for i, conf in enumerate(configuration["to_run"]):
                algo_conf = get_algo_name_new(algo, conf, hyperparams, param_dict)
                output_config["cfgs"][-1]["algorithms"].append(f"{algo_conf} {algo_conf.replace('_', '-')}")

    # for set_name, set_enabled in sets.items():
    #     if not set_enabled:
    #         continue

    #     enabled_orderings = [ordering for ordering, ordering_enabled in orderings.items() if ordering_enabled]
    #     output_config["cfgs"].append({
    #         "folder": set_name,
    #         "theme": theme,
    #         "server": cfg.SERVER,
    #         "set": set_name,
    #         "orderings": enabled_orderings,
    #         "algorithms": []
    #     })

    #     for configuration in configurations:
    #         hyperparams = configuration.get("hyperparams", {})
    #         for i, conf in enumerate(configuration["to_run"]):
    #             algo_conf = get_algo_name_new(algo, conf, hyperparams)
    #             output_config["cfgs"][-1]["algorithms"].append(f"{algo_conf} {algo_conf.replace('_', '-')}")


    # Write the updated configuration to the output file
    with open(output_file, "w") as f:
        json.dump(output_config, f, indent=2)
    print(f"Configuration written to {output_file}", flush=True)

    # Example of the output configuration file format:
    #     {
    #     "cfgs": [
    #         {
    #             "folder": "konect_cc",
    #             "theme": "all",
    #             "server": "109",
    #             "set": "konect_cc_set",
    #             "orderings": ["natural"],
    #             "algorithms": [
    #                 "heistream_32k",
    #                 "heistream_65k",
    #                 "heistream_131k",
    #                 "cuttana_mbs1m_subp16",
    #                 "cuttana_mbs1m_subp4k",
    #                 "PQv7_NBS3_16k_mbs131k_haa_2",
    #                 "PQv7_NBS3_8k_mbs65k_haa_2",
    #                 "PQv7_NBS3_1_mbs65k",
    #                 "PQv7_NBS3_1_mbs131k",
    #                 "PQv7_NBS3_1_mbs262k"
    #             ]
    #         }
    #     ]
    # }

def print_configuration_newest(config_data, theme) -> None:
    """Generiert eine JSON-Konfigurationsdatei basierend auf den Eingabedaten."""
    today = datetime.today().strftime('%Y-%m-%d')
    # Ausgabedatei
    output_file = os.path.expanduser(f"~/outputs/{today}.json")

    # Falls die Datei existiert, einlesen und erweitern
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            output_config = json.load(f)
    else:
        output_config = {
            "cfgs": []
        }

    # Konfigurationsdaten auslesen
    orderings = config_data.get("orderings", {})
    configurations = config_data.get("configurations", [])


    # Für jeden Set-Namen in den Orderings
    for set_name, _ in orderings["natural"].items():
        # Aktivierte Orderings extrahieren
        enabled_orderings = []
        for ord_name, ord_sets in orderings.items():
            if set_name in ord_sets and ord_sets[set_name]:
                enabled_orderings.append(ord_name)

        if len(enabled_orderings) == 0:
            # Wenn keine Orderings aktiviert sind, diesen Set überspringen
            continue

        # Neue Konfiguration hinzufügen
        output_config["cfgs"].append({
            "folder": set_name,
            "theme": theme,
            "server": cfg.SERVER,
            "set": set_name,
            "orderings": enabled_orderings,
            "algorithms": []
        })

        # Für jede Konfiguration die Algorithmen erstellen
        for configuration in configurations:
            # Algorithmus aus der Konfiguration holen (neu)
            algo = configuration.get("algo")
            hyperparams = configuration.get("hyperparams", {})
            param_dict = configuration.get("params", {})

            max_cores = configuration.get("max_cores")


            # Für jeden definierten Lauf in to_run
            for i, conf in enumerate(configuration["to_run"]):
                algo_conf = get_algo_name_new(algo, conf, hyperparams, param_dict)
                if algo_conf:  # Nur hinzufügen, wenn gültiger Name generiert wurde
                    output_config["cfgs"][-1]["algorithms"].append(f"{max_cores} {algo_conf} {algo_conf.replace('_', '-')}")

    # Aktualisierte Konfiguration in die Ausgabedatei schreiben
    with open(output_file, "w") as f:
        json.dump(output_config, f, indent=2)
    print(f"Configuration written to {output_file}", flush=True)


def print_configurations(algo, set_name, configurations, ordering, theme) -> None:
    """
    Generates a configuration JSON file at ~/current_exp_config.json.
    The file uses a fixed template except for the "algorithms" list, which is built
    from the provided algo and configuration. The data_path for each algorithm is built
    as: data/<set_name>/<algorithm_name>.csv.
    
    For the "heistream" algorithm, each configuration is a single number,
    and the algorithm name is constructed as: <algo>_<abbreviation>.
    
    For all other algorithms, each configuration is a string with three numbers
    (separated by whitespace), and the algorithm name is:
      <algo>_1p<abbr(FPBS)>_2p<abbr(SPBS)>_mqs<abbr(MQS)>
    """
    OUTPUT_FILE = f"~/outputs/{theme}_{ordering}.json"
    algorithms_list = []

    colors = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
        "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5"
    ]

    for configuration in configurations:
        param_dict = configuration.get("params", {})
        hyperparams = configuration.get("hyperparams", {})
        for i, conf in enumerate(configuration["to_run"]):

            algo_conf = get_algo_name_new(algo, conf, hyperparams, param_dict)
            data_path = f"data/{cfg.SERVER}/{set_name}/{ordering}/{algo_conf}.csv"
            algorithms_list.append({
                "name": algo_conf,
                "data_path": data_path,
                # "color": colors[i % len(colors)],
                "legend_label": "\\textsc{" + algo_conf.replace('_', '-') + "}"
            })

    # Build the complete configuration using the provided template.
    exp_config = {
        "algorithms": algorithms_list,
        "not_used": [],
        "r_script_path": "perf_profile.r",
        "output_csv": "combined_results.csv",
        "metrics": ["runtime", "memory", "solution_quality"],
        "plots": [
            {
                "metric": "runtime",
                "name": "a) Running Time",
                "optimization": "minimization"
            },
            {
                "metric": "memory",
                "name": "b) Memory Consumption",
                "optimization": "minimization"
            },
            {
                "metric": "solution_quality",
                "name": "c) Edge cut",
                "optimization": "minimization"
            }
        ],
        "boxplot_script_path": "boxplot.r",
        "boxplots": [
            { "metric": "runtime", "y_label": "Running Time (s)", "y_scale": "log" },
            { "metric": "memory", "y_label": "Memory Consumption (GB)", "y_scale": "log" },
            { "metric": "solution_quality", "y_label": "Edge cut", "y_scale": "log" }
        ],
        "filters": {
            "seed": 0
        },
        "plot_settings": {
            "title": "Performance Profile",
            "x_label": "Ratio to Best",
            "y_label": "Fraction of Instances",
            "x_scale": "log"
        }
    }

    output_file = os.path.expanduser(OUTPUT_FILE)
    with open(output_file, "w") as f:
        json.dump(exp_config, f, indent=2)
    print(f"Configuration written to {output_file}", flush=True)

def get_fbs_output_dir(set_name, ordering, alg_name, max_cores) -> str:
    """
    Returns the output directory for FlatBuffers files for a given set name, ordering, and algorithm.
    The directory is constructed as: ~/results/fbs_<set_name>/<ordering>/<algo>
    """
    return os.path.expanduser(os.path.join(f"{cfg.BASE_DIR_OUTPUT}/fbs_{set_name}/{ordering}", f"{max_cores}core{'' if max_cores == 1 else 's'}", alg_name))

def get_processed_output_dir(set_name, ordering, max_cores) -> str:
    """
    Returns the processed output directory for a given set name, ordering, and algorithm.
    The directory is constructed as: ~/results/processed_results/<server>/<set_name>/<ordering>/<max_cores>/
    """
    return os.path.expanduser(f"{cfg.BASE_DIR_PROCESSED_OUTPUT}/{set_name}/{ordering}/{max_cores}core{'' if max_cores == 1 else 's'}")




fieldnames = ["alg_name", "graph", "seed", "k", "runtime", "memory", "edge_cut", "solution_quality", "success"]

def produce_result(alg_name, graph, seed, k, runtime, memory, edge_cut, ECR):
    task = {
        "alg_name": alg_name,
        "graph": graph,
        "seed": seed,
        "k": k,
        "runtime": runtime,
        "memory": memory,
        "edge_cut": edge_cut,
        "solution_quality": ECR,
        "success": "1" if str(edge_cut) != "0" else "0"
    }
    return task

def produce_failed_result(alg_name, graph, k):
    failed_task = {
        "alg_name": alg_name,
        "graph": graph,
        "seed": "0",
        "k": k,
        "runtime": "0",
        "memory": "0",
        "edge_cut": "0",
        "solution_quality": "0",
        "success": "0"
    }
    return failed_task


class Task:
    def __init__(self,  k, raw_graph_name, graph_path, stream_buffer, max_pq_size, old_target_path, fbs_target_path):
        self.raw_graph_name = raw_graph_name
        self.graph_path = graph_path
        self.k = k
        self.stream_buffer = stream_buffer
        self.max_pq_size = max_pq_size
        self.old_target_path = old_target_path
        self.fbs_target_path = fbs_target_path




