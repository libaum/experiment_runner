# Experiment Runner

This project provides a framework to run various experiments using different algorithms and configurations. It leverages JSON configuration files to define what experiments to run. Additionally, the `config.py` file sets up basic configurations common to all experiments.

The script handles BuffCut, HeiStream and Cuttana. The default handling refers to BuffCut, if the algo name (defined in the JSON file) contains "heistream" or "cuttana" it handles the programs accordingly. For BuffCut and HeiStream the FBS generated output files are used to extract the results. For Cuttana it reads the results from the output, hereby, it is assumed that the output of the Cuttana program corresponds to `<runtime> <memory usage> <edgecut> <edges cut ratio>`.

The resulting CSV files are by default stored in "~/results/processed_results/<SERVER>/<ordering>/<cores>/<algo>_<params>.csv". They contain the evaluation metrics runtime, memory usage and edge cut for all graphs and k values. This path can be changed in `config.py`.


## How to Use

1. **Prepare Your Configuration File**
   Create or modify a configuration file in the `exp/cfg/` folder. For example, `example.json` might define the algorithm, dataset sets, orders, and other experiment parameters.

2. **Execution**
   To run the experiments using a configuration file, execute the following command:

```bash
  python3 run_exp_for_config.py --config=cfg/example.json
```



