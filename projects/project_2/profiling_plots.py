import json
import os
from collections import defaultdict, deque
from typing import Dict, List, Tuple
from dataclasses import dataclass
import matplotlib.pyplot as plt
import numpy as np

@dataclass
class OperatorInfo:
    type: str
    timing: float
    path: str

def extract_operator_timings(node, timings, current_path = ""):
    queue = deque([(node, current_path)])
    
    while queue:
        current_node, current_path = queue.popleft()
        
        if "operator_type" in current_node:
            timings.append(OperatorInfo(
                type=current_node["operator_type"],
                timing=current_node["operator_timing"],
                path=f"{current_path}/{current_node["operator_type"]}"
            ))
        
        for i, child in enumerate(current_node.get("children", [])):
            queue.append((child, f"{current_path}/child{i}"))

def parse_filename(filename):
    parts = filename.replace(".json", "").split("_")
    query_id = parts[1]
    scaling_factor = int(parts[2])
    num_threads = int(parts[3])
    return query_id, scaling_factor, num_threads

def analyze_profiles(directory):
    timings = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    for filename in os.listdir(directory):
        if not filename.startswith("profile_") or not filename.endswith(".json"):
            continue
            
        query_id, scale_factor, num_threads = parse_filename(filename)
        with open(os.path.join(directory, filename), "r") as f:
            data = json.load(f)
            
        operator_timings = []
        extract_operator_timings(data, operator_timings)
        timings[query_id][scale_factor][num_threads] = operator_timings

    return timings

def plot_operators_by_threads(timings, query_id, scale_factor, path):
    thread_counts = sorted([t for t in timings[query_id][scale_factor].keys() if t in [1, 2, 4, 8]])
    all_operators = set()
    for threads in thread_counts:
        all_operators.update([op.path for op in timings[query_id][scale_factor][threads]])
    
    operator_names = sorted(list(all_operators))
    plt.figure(figsize=(15, 8))
    bar_width = 0.2
    index = np.arange(len(operator_names))
    
    for i, threads in enumerate(thread_counts):
        timings_for_threads = []
        for op_name in operator_names:
            matching_ops = [op for op in timings[query_id][scale_factor][threads] if op.path == op_name]
            if matching_ops:
                timings_for_threads.append((matching_ops[0].timing * 1000) / threads)
            else:
                timings_for_threads.append(0)
        
        plt.bar(index + i*bar_width, timings_for_threads, bar_width, 
                label=f"{threads} Threads")
    
    plt.xlabel("Operators")
    plt.ylabel("Time per Thread (ms)")
    plt.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    plt.xticks(index + bar_width*1.5, [op.split("/")[-1] for op in operator_names], rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path)

def plot_operators_by_scale(timings, query_id, thread_count, path):
    scale_factors = sorted([s for s in timings[query_id].keys() if s in [1, 10, 100]])
    all_operators = set()
    for scale in scale_factors:
        all_operators.update([op.path for op in timings[query_id][scale][thread_count]])
    
    operator_names = sorted(list(all_operators))
    plt.figure(figsize=(15, 8))
    bar_width = 0.25
    index = np.arange(len(operator_names))
    
    for i, scale in enumerate(scale_factors):
        timings_for_scale = []
        for op_name in operator_names:
            matching_ops = [op for op in timings[query_id][scale][thread_count] if op.path == op_name]
            if matching_ops:
                timings_for_scale.append((matching_ops[0].timing*1000) / thread_count)
            else:
                timings_for_scale.append(0)
        
        plt.bar(index + i*bar_width, timings_for_scale, bar_width, 
                label=f"Scale {scale}")
    
    plt.xlabel("Operators")
    plt.ylabel("Time per Thread (ms)")
    plt.yscale("log")
    plt.ylim(bottom=1)
    plt.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    plt.xticks(index + bar_width, [op.split("/")[-1] for op in operator_names], rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path)


directory = "./profiling/"
timings = analyze_profiles(directory)


img_dir = "./profiling/img/"


threads = [1, 2, 4, 8]
sf_lst = [1, 10, 100]
query_lst = ["q1.1", "q3.1", "q4.1"]
for t in threads:
    for query in query_lst:
        plot_operators_by_scale(timings, query, t, f"{img_dir}{query}_t_{t}.pdf")
for sf in sf_lst:
    for query in query_lst:
        plot_operators_by_threads(timings, query, sf, f"{img_dir}{query}_sf_{sf}.pdf")
