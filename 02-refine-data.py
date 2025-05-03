import pandas as pd
import random
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

prompt = """
You are a product name refinement assistant. Your job is to refine product names to be clear, structured, and very simple, and only output the name of the product with no formatting or extra text.

Refine the following product names, and remove unnecessary words/formatting/brands. Return with just newlines between each product name:
"""

# select 10k random product names
df = pd.read_parquet('./dataset/shopping_queries_dataset_products.parquet')
# filter out the product names that are not in English
df = df[df['product_locale'] == 'us']

# select 10k random rows
df = df.sample(10000, random_state=random.randint(0, 2000))
df = df[['product_title']]

# write the sample to a file
df.to_csv('./dataset/sample.csv', index=False)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# refine & save using OpenAI API in batches
# Process each item individually
r = open('./dataset/refined.txt', 'w')

for i, item in enumerate(df['product_title']):
    item_prompt = prompt + item
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": item_prompt}]
    )
    
    refined_product = response.choices[0].message.content.strip()
    
    if refined_product != "":
        r.write(refined_product + "\n")
        r.flush()
    
    print(f"refined item {i+1}/{len(df)}: {refined_product}")

r.close()