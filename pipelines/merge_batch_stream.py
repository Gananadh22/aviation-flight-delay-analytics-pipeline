import pandas as pd

print("Merging Batch + Streaming data...")

# Load batch dataset
batch_df = pd.read_csv("data/gold/fact_flights.csv")

print("Batch rows:", len(batch_df))

# Load streaming dataset
stream_df = pd.read_csv("data/gold/streaming_updates/streaming_updates.csv")

print("Streaming rows:", len(stream_df))

# Merge both
full_df = pd.concat([batch_df, stream_df], ignore_index=True)

print("Total rows after merge:", len(full_df))

# Save final dataset
full_df.to_csv("data/gold/fact_flights_full_year.csv", index=False)

print("Full year dataset created successfully.")