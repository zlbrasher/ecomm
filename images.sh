#!/bin/bash

mkdir -p dataset/images

while IFS= read -r line; do
    # Extract productId and imageUrl using jq
    id=$(echo "$line" | jq -r '.id')
    imageUrl=$(echo "$line" | jq -r '.imageUrl')

    # Check if both productId and imageUrl are not null
    if [[ "$id" != "null" && "$imageUrl" != "null" ]]; then
        # Download the image using wget
        wget -O "dataset/images/${id}.jpg" "$imageUrl"
    fi
done < ./dataset/products.jsonl