import pandas as pd
import random
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
batch_size = 20
r = open('./dataset/refined.txt', 'w')
    
for i in range(0, len(df), batch_size):
    # get the batch of product names
    batch = df.iloc[i:i+batch_size]['product_title'].tolist()
    batch_prompt = prompt + "\n".join(batch)
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": batch_prompt}]
    )
    
    # parse, print, and write the response
    refined_products = response.choices[0].message.content.strip().split('\n')
    
    # write the refined products to the file
    for refined in refined_products:
        if refined != "":
            r.write(refined + "\n")
            
    r.flush()
    
    print(f"refined batch {i//batch_size + 1}/{len(df)//batch_size + 1}: {refined_products}")
                
