import os
import json
import requests

input_file = './dataset/products.jsonl'
output_dir = './dataset/images'

os.makedirs(output_dir, exist_ok=True)

# synchronously download every image
with open(input_file, 'r') as file:
    for line in file:
        try:
            # Parse the JSON line
            product = json.loads(line)
            product_id = product.get('productId')
            image_url = product.get('imageUrl')

            # Skip if required fields are missing
            if not product_id or not image_url:
                continue

            # Define the output file path
            output_path = os.path.join(output_dir, f"{product_id}.jpg")

            # Download the image
            response = requests.get(image_url, stream=True)
            if response.status_code == 200:
                with open(output_path, 'wb') as img_file:
                    for chunk in response.iter_content(1024):
                        img_file.write(chunk)
                print(f"Downloaded: {output_path}")
            else:
                print(f"Failed to download {image_url} (status code: {response.status_code})")
        except Exception as e:
            print(f"Error processing line: {line.strip()} - {e}")