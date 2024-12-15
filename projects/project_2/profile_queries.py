import duckdb
import queries as q

threads = [1, 2, 4, 8]
sf_lst = [1, 10, 100]
query_lst = [("q1.1", q.q1_1), ("q3.1", q.q3_1), ("q4.1", q.q4_1)]
for sf in sf_lst:
    con = duckdb.connect(f"./duckdb/duckdb_benchmark_data/ssb_sf_{sf}.duckdb")
    con.sql("SET enable_profiling = 'json'")
    for t in threads:
        con.sql(f"SET threads = {t}")
        for query_id, query in query_lst:
            print(f"{sf}, {t}, {query_id}")
            con.sql(f"SET profile_output = '/home/mlflexer/repos/ADS/assignments/duckdb/profiling/profile_{query_id}_{sf}_{t}.json'")
            con.sql(query).show()
            con.sql(query).show()
            con.sql(query).show()
    con.close()
