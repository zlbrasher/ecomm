import pandas as pd
from dotenv import load_dotenv

load_dotenv()
# create the combined dataset with all the parquet files

# read the 5 parquet files
df1 = pd.read_parquet('./dataset/test-00000-of-00005.parquet')
df2 = pd.read_parquet('./dataset/test-00001-of-00005.parquet')
df3 = pd.read_parquet('./dataset/test-00002-of-00005.parquet')
df4 = pd.read_parquet('./dataset/test-00003-of-00005.parquet')
df5 = pd.read_parquet('./dataset/test-00004-of-00005.parquet')

# select 10k random rows and print the title field

# concatenate the dataframes
df = pd.concat([df1, df2, df3, df4, df5], ignore_index=True)

df.to_parquet('dataset/combined.parquet', index=False)