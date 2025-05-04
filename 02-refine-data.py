import pandas as pd
import random
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

prompt = """
You are an e-commerce search expert. 
Input: A seed product query (e.g., "Nike shoes").
Task: Produce 3 varied, realistic search queries by adding 1-3 shopping attributes. 
• Do NOT change the brand or core product type.
• Attributes can include color, material, price filter, style, size, discount, etc.
• Keep language concise and natural.
• Avoid gender or cultural bias.
Return plain text with just the query, no numbering, explanations.
Seed query:
"""

# select 10k random english product names
df = pd.read_parquet('./dataset/shopping_queries_dataset_products.parquet')
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
        messages=[{"role": "user", "content": item_prompt}], 
        max_tokens=100,
        temperature=0.7,
    )
    
    refined_product = response.choices[0].message.content.strip().replace("\n\n", "\n") # remove whitespae/excessive newlines
    
    if refined_product != "":
        r.write(refined_product + "\n")
        r.flush()
    
    print(f"refined item {i+1}/{len(df)}: {refined_product}")

r.close()