import pandas as pd

parquet_file = './dataset/products.parquet'
csv_output_file = './dataset/metadata.csv'

df = pd.read_parquet(parquet_file)
df.to_csv(csv_output_file, index=False)

print(f"Converted {parquet_file} to {csv_output_file}")