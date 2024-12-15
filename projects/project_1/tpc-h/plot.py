import pickle
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import os

def get_df(file_path):
    with open(file_path, "rb") as file:
        data = pickle.load(file)
    for wh, schema_dict in data.items():
        for schema, query_dict in schema_dict.items():
            for query_name, lst in query_dict.items():
                query_dict[query_name] = pd.DataFrame(lst, columns=["total_elapsed_time", "compilation_time", "execution_time"])
                print(query_dict[query_name])
    return data

data = get_df("results_tpc_h.pkl")

def plot_avg_time(data, path, column, scaling_factor, y_label="Execution time (ms)"):
    warehouses = list(data.keys())

    avg_times = {}
    std_devs = {}
    
    for warehouse, scales in data.items():
        if scaling_factor in scales:
            avg_times[warehouse] = {}
            std_devs[warehouse] = {}
            for query, df in scales[scaling_factor].items():
                avg_times[warehouse][query] = df[column].mean()
                std_devs[warehouse][query] = df[column].std()
    
    queries = ["q1", "q5", "q18"]
    x = np.arange(len(warehouses))
    bar_width = 0.15
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    warehouse_labels = {
        "xsmall": "XS",
        "small": "S",
        "medium": "M",
        "large": "L"
    }
    
    custom_warehouse_labels = [warehouse_labels.get(warehouse, warehouse) for warehouse in warehouses]
    
    for i, query in enumerate(queries):
        query_avg_times = [avg_times[warehouse].get(query, 0) for warehouse in warehouses]
        query_std_devs = [std_devs[warehouse].get(query, 0) for warehouse in warehouses]
        
        bars = ax.bar(x + i * bar_width, query_avg_times, bar_width, label=query)
        ax.errorbar(x + i * bar_width, query_avg_times, yerr=query_std_devs, fmt="none", color="black", capsize=5)
    
    ax.set_xticks(x + bar_width * (len(queries) - 1) / 2)
    ax.set_xticklabels(custom_warehouse_labels)
    
    ax.set_xlabel("Warehouse Size")
    ax.set_ylabel(y_label)
    ax.legend(title="Query")
    ax.set_ylim(bottom=0)
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    # ax.set_yscale("log")
    
    plt.tight_layout()
    plt.savefig(path)






def plot_avg_time_fix_wh(data, path, column, fixed_warehouse, y_label):
    scaling_factors = list(data[fixed_warehouse].keys())
    
    avg_times = {}
    std_devs = {}
    
    for scaling_factor in scaling_factors:
        avg_times[scaling_factor] = {}
        std_devs[scaling_factor] = {}
        
        for query, df in data[fixed_warehouse][scaling_factor].items():
            avg_times[scaling_factor][query] = df[column].mean()
            std_devs[scaling_factor][query] = df[column].std()
    
    queries = ["q1", "q5", "q18"]
    x = np.arange(len(scaling_factors))
    bar_width = 0.2
    
    fig, ax = plt.subplots(figsize=(12, 6))

    scaling_factor_labels = {
        "TPCH_SF1": "1",
        "TPCH_SF10": "10",
        "TPCH_SF100": "100",
        "TPCH_SF1000": "1000"
    }
    
    custom_labels = [scaling_factor_labels.get(str(factor), str(factor)) for factor in scaling_factors]
    
    for i, query in enumerate(queries):
        query_avg_times = [avg_times[scaling_factor].get(query, 0) for scaling_factor in scaling_factors]
        query_std_devs = [std_devs[scaling_factor].get(query, 0) for scaling_factor in scaling_factors]
        
        bars = ax.bar(x + i * bar_width, query_avg_times, bar_width, label=query)
        ax.errorbar(x + i * bar_width, query_avg_times, yerr=query_std_devs, fmt="none", color="black", capsize=5)
    
    ax.set_xticks(x + bar_width * (len(queries) - 1) / 2)
    ax.set_xticklabels(custom_labels)
    
    ax.set_xlabel("Scaling Factor")
    ax.set_ylabel(y_label)
    ax.set_yscale("log")
    ax.set_ylim(bottom=1)
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    ax.legend(title="Query")
    
    plt.tight_layout()
    plt.savefig(path)

plot_avg_time_fix_wh(data, "./plots/fix_wh.pdf", column="execution_time", fixed_warehouse="xsmall", y_label="Execution time (ms)")
plot_avg_time_fix_wh(data, "./plots/fix_wh_large.pdf", column="execution_time", fixed_warehouse="large", y_label="Execution time (ms)")

plot_avg_time(data, "./plots/fix_sf.pdf", "execution_time", "TPCH_SF100", y_label="Execution time (ms)")
