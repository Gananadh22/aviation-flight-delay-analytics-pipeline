import pandas as pd
import os

print("Loading streaming dataset...")

df = pd.read_csv("data/processed/flights_stream.csv")

print("Rows:", len(df))

# create streaming folder
os.makedirs("data/streaming", exist_ok=True)

batch_size = 50000
batch_number = 1

for start in range(0, len(df), batch_size):

    end = start + batch_size
    batch = df.iloc[start:end]

    file_name = f"data/streaming/stream_batch_{batch_number}.csv"

    batch.to_csv(file_name, index=False)

    print(f"Created {file_name}")

    batch_number += 1

print("Streaming batches created successfully")