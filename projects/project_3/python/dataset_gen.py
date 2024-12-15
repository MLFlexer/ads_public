import pandas as pd
import numpy as np
from faker import Faker

def generate_dataset(num_rows, column_specs):
    faker = Faker()
    data = {}
    int32_col = np.random.randint( np.iinfo(np.int32).min, np.iinfo(np.int32).max, num_rows, dtype=np.int32)
    int64_col = np.random.randint( np.iinfo(np.int64).min, np.iinfo(np.int64).max, num_rows, dtype=np.int64)
    for column_name, column_type in column_specs.items():
        print(f"Generating column: {column_name} as {column_type}")
        if column_type == "i32":
            data[column_name] = int32_col
        elif column_type == "i64":
            data[column_name] = int64_col
        elif column_type == "str":
            data[column_name] = [faker.word() for _ in range(num_rows)]
    
    df = pd.DataFrame(data)
    return df

columns = {
    "i32": "i32",
    "str": "str",
    "i64": "i64"
}


def generate_dataset_w_type(col_type, df_subset):
    df = df_subset[[col_type]]
    df = df.rename(columns={col_type: "col_1"})
    df.to_parquet(f"data/{col_type}_1_{size}.parquet", index=False)
    df.to_csv(f"data/{col_type}_1_{size}.csv", index=False, header=False)
    column_names = ["col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8", "col_9", "col_10"]

    for n_cols, column in enumerate(column_names):
        print(f"column: {column} - {col_type}")
        n_cols += 1
        df[column] = df_subset[col_type]
        df.to_parquet(f"data/{col_type}_{n_cols}_{size}.parquet", index=False)
        df.to_csv(f"data/{col_type}_{n_cols}_{size}.csv", index=False, header=False)


df = generate_dataset(10_000_000, columns)


sizes = [10_000, 100_000, 1_000_000, 10_000_000]
for size in sizes:
    print(f"making size: {size}")
    
    if size != len(df):
        df_subset = df.head(size)
    else:
        df_subset = df
    df_subset.to_parquet(f"data/full_{size}.parquet", index=False)
    df_subset.to_csv(f"data/full_{size}.csv", index=False, header=False)

    generate_dataset_w_type("i32", df_subset)
    generate_dataset_w_type("i64", df_subset)
    generate_dataset_w_type("str", df_subset)

