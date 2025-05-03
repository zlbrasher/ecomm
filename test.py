import pandas as pd

# Read the parquet file
df = pd.read_parquet('./dataset/products.parquet')

# Display all data
print(df)