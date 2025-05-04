import pandas as pd
from serpapi import GoogleSearch
from dotenv import load_dotenv
import os
import json
import time
import uuid
import requests

load_dotenv()

# read the product names from the refined file
df = pd.read_csv('./dataset/refined-final.txt', sep='\t', header=None)
df.columns = ['product_title']
df['product_title'] = df['product_title'].str.strip() # remove whitespace
df = df.drop_duplicates()
df = df.dropna()
df = df[df['product_title'] != '']

# create a new parquest dataset
# fields: query, product_title, price, image_urls, product_url, source, and save the image data as well
dataset = pd.DataFrame(columns=['product_title', 'image_file_name', 'price', 'image_url', 'product_url', 'product_id', 'source'])
dataset.to_parquet('./dataset/products.parquet', index=False)

totalSearches = 0

# get serper data
def serp(query):
    payload = json.dumps({
      "q": query,
      "num": 100,
    })
    headers = {
      'X-API-KEY': os.getenv("SERPER_API_KEY"),
      'Content-Type': 'application/json'
    }
    response = requests.request("POST", "https://google.serper.dev/shopping", headers=headers, data=payload)

    if response.status_code == 200:
        data = response.json()
        if 'shopping' in data:
            return data['shopping']
        else:
            print("No shopping results", query, data)
            return []
    
def parse_price(price_str):
    price_str = price_str.replace("$", "").replace(",", "").split(" ")[0]
    try:
        return float(price_str)
    except ValueError:
        return 0


# for every product name, query serpapi for 100 results
for query in df['product_title']:
    if totalSearches > 10000:
        print("Reached 10000 searches, stopping.")
        break
    
    totalSearches += 1
    print(f"Searching for {query} {totalSearches}/10000")
    
    results = serp(query)
        
    for item in results:
        print(item['title'])
    
    for item in results:
        # add to jsonl for backup
        id = uuid.uuid4().hex  # generate a unique id for the image file
        
        item['query'] = query
        item['id'] = id
        with open('./dataset/products.jsonl', 'a') as f:
            json.dump(item, f)
            f.write('\n')
            
            
        # add to the dataset
        new_row = {
            'product_title': item.get('title', ''),
            'image_file_name': id + '.jpg',
            'price': parse_price(item.get('price', '')),
            'image_url': item.get('imageUrl', ''),
            'product_url': item.get('link', ''),
            'source': item.get('source', '')
        }
        
        dataset.loc[len(dataset)] = new_row
        print(f"Added product: {new_row['product_title']} - {new_row['price']}")
            

    # save after every iteration
    dataset.to_parquet('./dataset/products.parquet', index=False)
    
# save the dataset to a parquet file
dataset.to_parquet('./dataset/products.parquet', index=False)
