import pandas as pd

input_file = "data/processed/flights_batch.csv"
output_file = "data/processed/flights_clean.csv"

print("Starting cleaning process...")

chunksize = 200000  # process 200k rows at a time
first_chunk = True
total_rows = 0

for chunk in pd.read_csv(input_file, chunksize=chunksize):

    # keep all rows (do NOT drop cancelled flights)
    df_clean = chunk

    total_rows += len(df_clean)

    if first_chunk:
        df_clean.to_csv(output_file, index=False, mode="w")
        first_chunk = False
    else:
        df_clean.to_csv(output_file, index=False, header=False, mode="a")

    print(f"{total_rows} rows processed...")

print("Cleaning finished.")
print(f"Clean dataset saved at: {output_file}")