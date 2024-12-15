import numpy as np
import matplotlib.pyplot as plt
import re
import glob
from pathlib import Path

def read_benchmark_file(filename):
    with open(filename, "r") as f:
        lines = [float(line.strip())*1000 for line in f.readlines()]
    
    return {
        "q1_1": np.array(lines[0:5]),
        "q3_1": np.array(lines[5:10]),
        "q4_1": np.array(lines[10:15])
    }

def parse_filename(filename):
    pattern = r"sf_(\d+(?:\.\d+)?)_(\d+)\.out"
    match = re.match(pattern, Path(filename).name)
    if match:
        return int(match.group(1)), int(match.group(2))
    raise ValueError(f"Invalid filename format: {filename}")

def plot_scaling_factor_comparison(filenames, num_threads, path):
    relevant_files = []
    scaling_factors = []
    
    for filename in filenames:
        sf, threads = parse_filename(filename)
        if threads == num_threads:
            relevant_files.append(filename)
            scaling_factors.append(sf)
    scaling_factors.sort()
    relevant_files.sort(reverse=True)

    benchmarks = ["q1_1", "q3_1", "q4_1"]
    means = {b: [] for b in benchmarks}
    stds = {b: [] for b in benchmarks}
    
    for filename in relevant_files:
        data = read_benchmark_file(filename)
        for bench in benchmarks:
            means[bench].append(np.mean(data[bench]))
            stds[bench].append(np.std(data[bench]))
    
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(scaling_factors))
    width = 0.25
    
    for i, bench in enumerate(benchmarks):
        ax.bar(x + i*width, means[bench], width, 
               label=bench,
               yerr=stds[bench], 
               capsize=5)
    
    ax.set_xlabel("Scaling Factor")
    ax.set_ylabel("Time (ms)")
    ax.set_xticks(x + width)
    ax.set_xticklabels(scaling_factors)
    ax.set_yscale("log")
    ax.set_ylim(bottom=1)
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(path)

def plot_thread_comparison(filenames, scaling_factor, path):
    relevant_files = []
    thread_counts = []
    
    for filename in filenames:
        sf, threads = parse_filename(filename)
        if sf == scaling_factor:
            relevant_files.append(filename)
            thread_counts.append(threads)
    thread_counts.sort()
    relevant_files.sort()
    
    benchmarks = ["q1_1", "q3_1", "q4_1"]
    means = {b: [] for b in benchmarks}
    stds = {b: [] for b in benchmarks}
    
    for filename in relevant_files:
        data = read_benchmark_file(filename)
        for bench in benchmarks:
            means[bench].append(np.mean(data[bench]))
            stds[bench].append(np.std(data[bench]))
    
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(thread_counts))
    width = 0.25
    
    for i, bench in enumerate(benchmarks):
        ax.bar(x + i*width, means[bench], width, 
               label=bench,
               yerr=stds[bench], 
               capsize=5)
    
    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Time (ms)")
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    ax.set_xticks(x + width)
    ax.set_xticklabels(thread_counts)
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(path)

filenames = glob.glob("./duckdb/sf_*.out")
plot_scaling_factor_comparison(filenames, 1, "./plots/fix_t_1.pdf")
plot_scaling_factor_comparison(filenames, 2, "./plots/fix_t_2.pdf")
plot_scaling_factor_comparison(filenames, 4, "./plots/fix_t_4.pdf")
plot_scaling_factor_comparison(filenames, 8, "./plots/fix_t_8.pdf")
plot_thread_comparison(filenames, 1, "./plots/fix_sf_1.pdf")
plot_thread_comparison(filenames, 10, "./plots/fix_sf_10.pdf")
plot_thread_comparison(filenames, 100, "./plots/fix_sf_100.pdf")
