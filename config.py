#!/usr/bin/env python3

import socket

DEBUG = True
OVERWRITE = False


# Specify directory where graphs can be found
BASE_DIR_GRAPHS = "~/graphs/all"
SERVER = socket.gethostname()[-3:]

# Output directory contains FBS files -> processed_ouput directory contains CSV files
BASE_DIR_OUTPUT =           f"~/results/{SERVER}"
BASE_DIR_PROCESSED_OUTPUT = f"~/results/processed_results/{SERVER}"

# Runtime and Memory limits (deactivated by default)
RUNTIME_LIMIT_ACTIVE = False
MEMORY_LIMIT_ACTIVE = False
RUNTIME_LIMIT_IN_HOURS = 12
MEMORY_ON_MACHINE = 755 # in GB


SET_CONFIG = {
    "tuning_set": {
        "k": [4, 8, 16, 32, 64, 128, 256],
        "additional_args": []
    },
    "test_set": {
        "k": [4, 8, 16, 32, 64, 128, 256],
        "additional_args": []
    },
        "tuning_set_fast": {
        "k": [4, 32, 128],
        "additional_args": []
    },
    "konect_cc_set": {
        # "k": [4, 8, 16, 32, 64],
        "k": [4, 8, 16, 32, 64, 128, 256],
        "additional_args": [
            # "--imbalance=5"
        ]
    },
    "konect_cc_set_light": {
        "k": [8],
        "additional_args": [
            # "--imbalance=5"
        ]
    },
    "local_cc_set": {
        "k": [8],
        "additional_args": [
            # "--imbalance=5"
        ]
    },
    "default": {
        "k": [4, 8, 16, 32, 64, 128, 256],
        "additional_args": []
    }
}




