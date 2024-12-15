import snowflake.connector
import time
import os
from collections import defaultdict
import pickle

import queries as q

password = os.getenv("SNOWFLAKE_PASSWORD")

if not password:
    print("Could not find password in environment variables")
    exit(1)

DATABASE = "SNOWFLAKE_SAMPLE_DATA"

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

def change_warehouse(wh):
    cur.execute(f"create or replace warehouse camel_wh WAREHOUSE_SIZE = {wh}, AUTO_SUSPEND = 10;")
    cur.fetchall()

def change_schema(schema):
    cur.execute(f"USE SCHEMA SNOWFLAKE_SAMPLE_DATA.{schema};")
    cur.fetchall()
    
def get_timestamp():
    cur.execute("SELECT CURRENT_TIMESTAMP;")
    return cur.fetchall()[0][0]

def run_query(query, title, result_dict):
    print(f"-- {title}")
    cur.execute(query)
    query_id = cur.sfqid
    print(query_id)
    result_dict[title].append(query_id)

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


WAREHOUSE_SIZES = ["xsmall", "small", "medium", "large"]
SCHEMAS = ["TPCH_SF1", "TPCH_SF10", "TPCH_SF100", "TPCH_SF1000"]
QUERIES = [("q1", q.q1), ("q5", q.q5), ("q18", q.q18)]

RUNS = 5

res_dict = {}
for wh in WAREHOUSE_SIZES:
    change_warehouse(wh)
    res_dict[wh] = {}
    for schema in SCHEMAS:
        print(f"wh: {wh}, sf: {schema}")
        res_dict[wh][schema] = defaultdict(list)
        start = get_timestamp()
        time.sleep(1)

        change_schema(schema)
        for q_name, q in QUERIES:
            for i in range(RUNS):
                run_query(q, q_name, res_dict[wh][schema])
        time.sleep(1)
        end = get_timestamp()

        for query_name, q_ids in res_dict[wh][schema].items():
            res = get_benchmark_averages(start, end, q_ids)
            res_dict[wh][schema][query_name] = res

with open(f"results_tpc_h.pkl", "wb") as file:
    pickle.dump(res_dict, file)


cur.close()
conn.close()
