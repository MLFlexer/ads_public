import numpy as np
import matplotlib.pyplot as plt
import re

def parse_timing_file(filepath, col_type):
    timings = {}
    
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            match = re.search(col_type + r"_(\d+)_10000000\.(?:parquet|csv)", line)
            if match:
                variant = int(match.group(1))
                file_type = "Parquet" if ".parquet" in line else "CSV"
                timing = int(line.split(", ")[-1]) / 1e9
                
                if variant not in timings:
                    timings[variant] = {"Parquet": [], "CSV": []}
                
                timings[variant][file_type].append(timing)
    
    return timings

def plot_timing_comparison(timings, output_path):
    variants = sorted(timings.keys())
    file_types = ["Parquet", "CSV"]
    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(variants))
    width = 0.4
    
    for i, file_type in enumerate(file_types):
        means = [np.mean(timings[variant][file_type]) for variant in variants]
        stds = [np.std(timings[variant][file_type]) for variant in variants]
        
        ax.bar(x + i*width, means, width, 
               yerr=stds, 
               capsize=5,
               label=file_type)
    
    ax.set_xlabel("Number of columns")
    ax.set_ylabel("Time (s)")
    ax.set_xticks(x + width/2)
    ax.set_xticklabels([str(variant) for variant in variants])
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    ax.legend()
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()

def main(filepath, col_type, output_path):
    timings = parse_timing_file(filepath, col_type)
    plot_timing_comparison(timings, output_path)

col_type = "i32"
filepath = f"../results/{col_type}_10000000.csv"
main(filepath, col_type, "./plots/i32_10000000.pdf")
col_type = "i64"
filepath = f"../results/{col_type}_10000000.csv"
main(filepath, col_type, "./plots/i64_10000000.pdf")
col_type = "str"
filepath = f"../results/{col_type}_10000000.csv"
main(filepath, col_type, "./plots/str_10000000.pdf")
