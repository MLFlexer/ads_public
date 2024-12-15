import pickle
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import os

def get_df(file_path):
    with open(file_path, "rb") as file:
        data = pickle.load(file)
    res = {}
    for q_name, lst in data:
        res[q_name] = pd.DataFrame(lst, columns=["total_elapsed_time", "compilation_time", "execution_time"])
    return res

def plot_avg_std(df_dict, column_name, y_label, path, is_udtf=False, show_full_height=False):
    query_names = []
    averages = []
    std_devs = []

    for q_name, df in df_dict.items():
        query_names.append(q_name)
        averages.append(df[column_name].mean())
        std_devs.append(df[column_name].std())

    plt.figure(figsize=(10, 6))
    bars = plt.bar(query_names, averages, yerr=std_devs, capsize=5)
    plt.xlabel("Query")
    plt.ylabel(y_label)
    plt.xticks(rotation=45, ha="right")
    #plt.yscale("log")
    #plt.ylim(bottom=1)
    if not show_full_height:
        plt.ylim(bottom=0, top=13000)
    plt.tight_layout()
    plt.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)
    if is_udtf:
        plt.text(bars[3].get_x() + bars[3].get_width() / 2, 13000,
                    f"{averages[3]:.2f}", ha="center", va="bottom", fontsize=10, color="red")

    plt.savefig(path)

sql_df = get_df("time_sql.pkl")
plot_avg_std(sql_df, "execution_time", "Execution Time (ms)", "./plots/sql_avg.pdf")

udtf_df = get_df("time_udtf.pkl")
plot_avg_std(udtf_df, "execution_time", "Execution Time (ms)", "./plots/udtf_avg.pdf", True)
plot_avg_std(udtf_df, "execution_time", "Execution Time (ms)", "./plots/udtf_full_avg.pdf", False, True)

# udtf
udtf_queries = {
"test": [
"def_get_priori",
"get_priori",
"def_get_word_label_count",
"get_word_label_count",
"def_get_denominator",
"get_labels"
],
"train": [
"get_test_w_id",
"def_get_tokenized_test",
"get_test_word_counts",
"def_get_likelyhoods",
"get_likelyhoods",
"def_get_prediction",
"get_prediction"
],
"metrics": [
"def_get_metrics_bin",
"get_metrics_bin"
]
}

# sql
sql_queries = {
"test": [
"create_test",
"compute_priori",
"get_labels",
"clean_tokenize_train",
"label_word_cross_product",
"label_word_counts",
"vocabulary_size",
"compute_denominator_per_label"
],
"train": [
"create_train",
"clean_tokenize_test",
"compute_likelyhoods",
"compute_scores",
"compute_predictions"
],
"metrics": [
"compute_metrics",
]
}

def plot_category_comparison(sql_df, udtf_df, sql_queries, udtf_queries, column_name, y_label, path):
    categories = ["train", "test", "metrics"]
    
    sql_averages = []
    sql_std_devs = []
    udtf_averages = []
    udtf_std_devs = []

    for category in categories:
        sql_category_averages = []
        sql_category_std_devs = []
        sums = [0]*5
        for i in range(5):
            for query in sql_queries[category]:
                sums[i] += sql_df[query][column_name][i]

        sql_averages.append(np.mean(sums))
        sql_std_devs.append(np.std(sums))
        
        udtf_category_averages = []
        udtf_category_std_devs = []
        sums = [0]*5
        for i in range(5):
            for query in udtf_queries[category]:
                sums[i] += udtf_df[query][column_name][i]
        
        udtf_averages.append(np.mean(sums))
        udtf_std_devs.append(np.std(sums))

    fig, ax = plt.subplots(figsize=(10, 6))

    index = np.arange(len(categories))
    bar_width = 0.35

    ax.bar(index - bar_width / 2, sql_averages, bar_width, yerr=sql_std_devs, label="SQL", capsize=5)

    ax.bar(index + bar_width / 2, udtf_averages, bar_width, yerr=udtf_std_devs, label="UDTF", capsize=5)

    print(sql_averages)
    print(udtf_averages)

    ax.set_xlabel("Category")
    ax.set_ylabel(y_label)
    ax.set_xticks(index)
    ax.set_xticklabels(categories, rotation=45, ha="right")
    #ax.set_ylim(bottom=0, top=14000)
    ax.legend()
    plt.grid(True, linestyle="--", linewidth=0.7, alpha=0.6)

    plt.tight_layout()
    plt.savefig(path)

plot_category_comparison(sql_df, udtf_df, sql_queries, udtf_queries, "execution_time", "Execution Time (ms)", "./plots/comp.pdf")

plot_category_comparison(sql_df, udtf_df, sql_queries, udtf_queries, "compilation_time", "Compilation Time (ms)", "./plots/compilation_comp.pdf")

