#!/usr/bin/env python3

import commentjson
import argparse
import subprocess
import sys
import os

from datetime import datetime
from utils import get_algo_name_new, print_configuration_newest, fprint, read_config_file, print_configuration_new
import config as cfg
from algo_runner import AlgorithmRunner


def main():
    parser = argparse.ArgumentParser(description="Run experiments based on a JSON config file.")
    parser.add_argument("--config", required=True, help="Path to the JSON config file")
    parser.add_argument("--clean_config", default=False, action="store_true", help="Clean the configuration and exit")
    parser.add_argument("--create_config", default=False, action="store_true", help="Print the configuration and exit")
    parser.add_argument("--quick_test", default=False, action="store_true", help="Perform a quick test over all algorithms")
    args = parser.parse_args()

    if args.clean_config:
        today = datetime.today().strftime('%Y-%m-%d')
        output_config = os.path.expanduser(f"~/outputs/{today}.json")
        if os.path.exists(output_config):
            os.remove(output_config)
        return

    # Load the configuration file
    config_data = read_config_file(args.config)

    # Extract configuration values
    # algo = config_data.get("algo")
    # sets = config_data.get("set", {})
    orderings = config_data.get("orderings", {})
    configurations = config_data.get("configurations", [])
    theme = os.path.splitext(os.path.basename(args.config))[0]



    if args.create_config:
        print_configuration_newest(config_data, theme)
        return

        # Print configuration for each enabled set and ordering.
        # for ordering, ordering_enabled in orderings.items():
        #     if not ordering_enabled:
        #         continue
        #     for set_name, set_enabled in sets.items():
        #         if not set_enabled:
        #             continue
        #         print_configurations(algo, set_name, configurations, ordering, theme)
        # return
    # params = config_data.get("hyperparams", [])

    # Führe Experimente für jede Konfiguration aus
    for config in configurations:
        # Extrahiere den Algorithmus aus jeder Konfiguration
        algo = config.get("algo")

        if args.quick_test and algo.startswith("cuttana"):
            fprint("No Quick Test with Cuttana.")
            continue

        if args.quick_test:
            fprint(f"\n################ STARTING QUICK TEST for {algo} ################")
            fprint("####### ----- Running quick test for all algorithms ----- #######\n")
        else:
            fprint(f"\n################ RUNNING EXPERIMIMENTS for {algo} ################\n")

        # Create an instance of AlgorithmRunner with the algorithm type
        runner = AlgorithmRunner(algo)

        # Wir übergeben nur eine Konfiguration und nicht die gesamte Liste
        run_exp_for_ordering_and_set(algo, runner, orderings, [config], quick_test=args.quick_test)

        if args.quick_test:
            fprint(f"\n################ FINISHED QUICK TEST for {algo} ################\n")
        else:
            fprint(f"\n#################### FINISHED for {algo} ####################\n")




def run_exp_for_ordering_and_set(algo: str, runner: AlgorithmRunner, orderings: dict, configurations: list, quick_test=False):
    # Run experiments for each enabled set and ordering.
    for ordering, sets in orderings.items():

        for set_name, set_enabled in sets.items():

            if not set_enabled:
                continue

            for configuration in configurations:
                hyperparams = configuration.get("hyperparams", {})
                param_dict = configuration.get("params", {})
                hyperparam_names = list(hyperparams.keys())
                max_cores = configuration.get("max_cores", 4)

                for conf in configuration["to_run"]:
                    alg_name = get_algo_name_new(algo, conf, hyperparams, param_dict)
                    if alg_name is None:
                        continue

                    fprint(f"\n# ----- Running \'{alg_name}\' with ordering \'{ordering}\' for set \'{set_name}\' ----- #")

                    # Generate dict from param names and values
                    hyperparam_values = conf.split()
                    hyperparam_dict = dict(zip(hyperparam_names, hyperparam_values))

                    # fprint(algo, set_name, ordering, hyperparam_dict, param_dict, alg_name)
                    runner.run(set_name, ordering, hyperparam_dict, param_dict, alg_name, max_cores, quick_test)


if __name__ == "__main__":
    main()