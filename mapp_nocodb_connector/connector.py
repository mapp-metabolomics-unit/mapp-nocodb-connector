import requests
import os
import argparse
import pandas as pd
from pandas import json_normalize
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# NocoDB API information
base_url = os.getenv("NOCODB_URL")  # NocoDB instance URL from .env file
api_token = os.getenv("NOCODB_API_TOKEN")  # API token from .env file
headers = {
    "accept": "application/json",
    "xc-auth": api_token,
    "xc-token": api_token
}

# Function to flatten nested dictionaries while preserving project keys
def flatten_project(project):
    flattened = {}
    for key, value in project.items():
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            sub_dict = flatten_project(value)
            for sub_key, sub_value in sub_dict.items():
                flattened[key + '_' + sub_key] = sub_value
        elif isinstance(value, list):
            # Join list elements into a string
            flattened[key] = ', '.join(map(str, value))
        else:
            flattened[key] = value
    return flattened

# GET request to fetch data from a specific table
def get_data(table_id, view_id, limit=25, shuffle=0, offset=0):
    url = f"{base_url}/api/v2/tables/{table_id}/records"
    params = {
        "viewId": view_id,
        "limit": limit,
        "shuffle": shuffle,
        "offset": offset
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        json_data = response.json()
        if 'list' in json_data:
            return json_data['list']
        else:
            return json_data
    else:
        error_message = f"Error: {response.status_code} - Response: {response.text}"
        raise Exception(error_message)

# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch data from a specific NocoDB table and save as CSV.")
    parser.add_argument("table_id", type=str, help="ID of the table to fetch data from")
    parser.add_argument("view_id", type=str, help="View ID for the table")
    parser.add_argument("csv_filename", type=str, help="Filename for the output CSV")
    parser.add_argument("--limit", type=int, default=25, help="Limit of records to fetch")
    parser.add_argument("--shuffle", type=int, default=0, help="Shuffle records")
    parser.add_argument("--offset", type=int, default=0, help="Offset for records")
    args = parser.parse_args()

    try:
        json_data = get_data(args.table_id, args.view_id, args.limit, args.shuffle, args.offset)
        flattened_data = [flatten_project(project) for project in json_data]
        df = pd.DataFrame(flattened_data)
        df.to_csv(args.csv_filename, index=False)
        print(f"Data saved as {args.csv_filename}")
    except Exception as e:
        print(f"Error occurred: {e}")
