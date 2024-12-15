import snowflake.connector
import time
import os
from collections import defaultdict
import pickle

import sql_queries as q

password = os.getenv("SNOWFLAKE_PASSWORD")

if not password:
    print("Could not find password in environment variables")
    exit(1)

DATABASE = "camel_db_sql_3"
WAREHOUSE = "ANIMAL_TASK_WH"

config = {
    "account": "sfedu02-gyb58550",
    "user": "CAMEL",
    "password": password,
    "database": DATABASE,
    "session_parameters" : {
        "USE_CACHED_RESULT": False,
    }
}

conn = snowflake.connector.connect(**config)
cur = conn.cursor()

def benchmark_preample():
    cur.execute(f"use warehouse {WAREHOUSE};")
    cur.fetchall()
    cur.execute(f"use schema {DATABASE}.public;")
    cur.fetchall()
    
def get_timestamp():
    cur.execute("SELECT CURRENT_TIMESTAMP;")
    return cur.fetchall()[0][0]

def run_query(query, title, result_dict):
    print(f"-- {title}")
    cur.execute(query)
    query_id = cur.sfqid
    print(query_id)
    if title == "compute_metrics":
        print(f"metrics for binary classification: (TP, FP, TN, FN, precision, recall, accuracy)")
        print(cur.fetchone())
    result_dict[title].append(query_id)

RUNS = 5
pkl_dict = {}
pkl_dict["start"] = get_timestamp()

res_dict = defaultdict(list)
benchmark_preample()
for i in range(RUNS):
    print(f"-- run: #{i}")
    
    run_query(q.create_test, "create_test", res_dict)
    run_query(q.create_train, "create_train", res_dict)

    run_query(q.compute_priori, "compute_priori", res_dict)
    run_query(q.get_labels, "get_labels", res_dict)
    run_query(q.clean_tokenize_train, "clean_tokenize_train", res_dict)
    run_query(q.label_word_cross_product, "label_word_cross_product", res_dict)
    run_query(q.label_word_counts, "label_word_counts", res_dict)
    run_query(q.vocabulary_size, "vocabulary_size", res_dict)
    run_query(q.compute_denominator_per_label, "compute_denominator_per_label", res_dict)

    run_query(q.clean_tokenize_test, "clean_tokenize_test", res_dict)
    run_query(q.compute_likelyhoods, "compute_likelyhoods", res_dict)
    run_query(q.compute_scores, "compute_scores", res_dict)
    run_query(q.compute_predictions, "compute_predictions", res_dict)

    run_query(q.compute_metrics, "compute_metrics", res_dict)
    

pkl_dict["q_ids"] = res_dict
time.sleep(1)
pkl_dict["end"] = get_timestamp()

print(pkl_dict)

with open(f"results_sql.pkl", 'wb') as file:
    pickle.dump(pkl_dict, file)

def get_benchmark_averages(start, end, q_ids):
    q_ids_tbl = "(" + ", ".join(f"'{q_id}'" for q_id in q_ids) + ")"
    query = f"""
        select total_elapsed_time, compilation_time, execution_time
        from table(information_schema.query_history())
        where user_name = 'CAMEL' and START_TIME >= '{start.strftime('%Y-%m-%d %H:%M:%S')}'
        and end_time <= '{end.strftime('%Y-%m-%d %H:%M:%S')}'
        and query_id in {q_ids_tbl};
    """
    cur.execute(query)
    res = cur.fetchall()
    return res

def get_averages(data):
    queries = []
    for query_name, q_ids in data["q_ids"].items():
        res = get_benchmark_averages(data["start"], data["end"], q_ids)
        print(res)
        queries.append((query_name, res))

    return queries

with open(f"time_sql.pkl", 'wb') as file:
    pickle.dump(get_averages(pkl_dict), file)

cur.close()
conn.close()
