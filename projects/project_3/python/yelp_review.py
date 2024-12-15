import pandas as pd
import json
import re

splits = {"train": "yelp_review_full/train-00000-of-00001.parquet", 
          "test": "yelp_review_full/test-00000-of-00001.parquet"}

df = pd.read_parquet("hf://datasets/Yelp/yelp_review_full/" + splits["train"])
df_2 = pd.read_parquet("hf://datasets/Yelp/yelp_review_full/" + splits["test"])
df = pd.concat([df, df_2])

def count_tokens(text):
    tokens = re.split(r"\W+", text)
    tokens = [t for t in tokens if t]
    return len(tokens)

average_length = df["text"].apply(count_tokens).mean()
print(f"Average length of text (split by \\W+): {average_length}")

sizes = [700, 7000, 70000, 700000]
for size in sizes:
    df_subset = df.head(size)
    df_subset.to_parquet(f"data/yelp_reviews_{size}.parquet", index=False)
    df_subset.to_csv(f"data/yelp_reviews_{size}.csv", index=False, header=False)
