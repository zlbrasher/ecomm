import pandas as pd
from serpapi import GoogleSearch
import json
import time
from dotenv import load_dotenv

load_dotenv()

# read the product names from the refined file
df = pd.read_csv('./dataset/sample.csv')
df['product_title'] = df['product_title'].str.strip() # remove whitespace
df = df.drop_duplicates() # remove duplicates
df = df.dropna() # remove empty rows
df = df[df['product_title'] != ''] # remove empty rows

# create a new parquest dataset
# fields: query, product_title, price, image_urls, product_url, source, and save the image data as well
dataset = pd.DataFrame(columns=['query', 'product_title', 'price', 'image_urls', 'product_url', 'source'])
dataset.to_parquet('./dataset/products.parquet', index=False)

# for every product name, query serpapi for 100 results
for product_title in df['product_title']:
    params = {
      "api_key": os.getenv("SERPAPI_KEY"),
      "engine": "google",
      "q": product_title,
      "google_domain": "google.com",
      "gl": "us",
      "hl": "en",
      "tbm": "shop",
      "num": "100`"
    }
    
    search = GoogleSearch(params)
    results = search.get_dict()
    
    if "shopping_results" in results:
        for item in results["shopping_results"]:
            new_row = {
                'query': product_title,
                'product_title': item.get('title', ''),
                'price': item.get('price', ''),
                'image_urls': item.get('thumbnails', '')[0],
                'product_url': item.get('product_link', ''),
                'source': item.get('source', '')
            }
            dataset.loc[len(dataset)] = new_row
            print(f"Added product: {new_row['product_title']} - {new_row['price']}")

    # save after every iteration
    dataset.to_parquet('./dataset/products.parquet', index=False)
    
# save the dataset to a parquet file
dataset.to_parquet('./dataset/products.parquet', index=False)
