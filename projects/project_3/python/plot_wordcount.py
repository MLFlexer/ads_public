import numpy as np
import matplotlib.pyplot as plt

def parse_timing_file(filepath):
    timings = {
        700: {"Parquet": [], "Parquet w/o Projection": [], "CSV": []},
        7000: {"Parquet": [], "Parquet w/o Projection": [], "CSV": []},
        70000: {"Parquet": [], "Parquet w/o Projection": [], "CSV": []},
        700000: {"Parquet": [], "Parquet w/o Projection": [], "CSV": []}
    }
    
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            for size in timings.keys():
                if f"yelp_reviews_{size}." in line:
                    timing = int(line.split(", ")[-1]) / 1e9
                    
                    if "parquet" in line.lower():
                        if "No Projection" in line:
                            timings[size]["Parquet w/o Projection"].append(timing)
                        else:
                            timings[size]["Parquet"].append(timing)
                    elif ".csv" in line:
                        timings[size]["CSV"].append(timing)
    
    return timings

def plot_timing_comparison(timings, output_path):
    sizes = list(timings.keys())
    file_types = ["Parquet", "Parquet w/o Projection", "CSV"]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(sizes))
    width = 0.25
    
    for i, file_type in enumerate(file_types):
        means = [np.mean(timings[size][file_type]) for size in sizes]
        stds = [np.std(timings[size][file_type]) for size in sizes]
        
        ax.bar(x + i*width, means, width, 
               yerr=stds, 
               capsize=5,
               label=file_type)
    
    ax.set_xlabel("Number of rows")
    ax.set_ylabel("Time (s)")
    ax.set_xticks(x + width)
    ax.set_xticklabels([str(size) for size in sizes])
    ax.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    ax.legend()
    plt.tight_layout()
    
    plt.savefig(output_path)
    plt.close()

def main(filepath, output_path="./plots/word_count.pdf"):
    timings = parse_timing_file(filepath)
    plot_timing_comparison(timings, output_path)

filepath = "../results/wordcount.csv"
main(filepath)
