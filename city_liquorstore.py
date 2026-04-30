# imports
import requests
import concurrent.futures
from tqdm import tqdm
import numpy as np
import pandas as pd
import os


# API Configs
base_url = "https://api.nyc.gov/geoclient/v2/search"
app_key = 'c14f342384e644c882575ea890dbfe99' # In a real project, this would be stored securely rather than hardcoded into our script. However, for the sake of our project, it is here to help streamline the process. Please don't use this key for any other projects or purposes, as it is meant solely for this project and may have usage limits. If you need to use the API for your own projects, please sign up for your own key at https://https://api-portal.nyc.gov. 
app_id = 'BigData636Project'

# Mapper Function to get the longitude and latitude of the stores
def geocode_row_v2(row):
    index, address = row
    session = requests.Session()
    headers = {
        'Ocp-Apim-Subscription-Key': app_key,
    }
    session.headers.update(headers)

    params = { # Address for geocoding
        'input': address + 'Manhattan,NY',
        'app_id': app_id,
    }
    
    try: # Attempt to get geocode data
        response = session.get(base_url, params=params, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes and stops script.
        data = response.json()
        # v2 search endpoint typically is an array of results.
        addr = data['results'][0].get('response',{}) # This is a list NOT a dictionary, so we take the first element (aka results) and then get the 'response' key which has all the data we want to get.          
        return {
                'index': index,
                'latitude': addr.get('latitude') or addr.get('lat'),
                'longitude': addr.get('longitude') or addr.get('lon'),
                'status': 'success'
            }
       
    except Exception as e:
        return {
            'index': index,
            'latitude': None,
            'longitude': None,
            'status': 'error',
            'error': str(e)
        }

def datasetDL():
    # Loading Dataset
    print("Loading Manhattan Liquor Store Dataset...")
    url = 'https://data.ny.gov/resource/ghy4-6tfh.csv/?$limit=54000'
    df = pd.read_csv(url)
    print("Original Dataset Head:\n", df.head(10)) # Used to check the head of the dataset before processing.

    # Filtering for Manhattan stores only
    manhattan_df = df[df['county'] == 'NEW YORK'] # NEW YORK county is the "official"

    # Prepare our addresses for geocoding
    addresses = list(manhattan_df['premise_address'].items()) # Get the index and address as a list of tuples for geocoding

    # Geocode addresses in parallel
    print("Starting Geocoding...")
    geocode_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        geocode_results = list(tqdm(executor.map(geocode_row_v2, addresses), total=len(addresses)))

    # Convert Geocode results to DataFrame
    geocode_df = pd.DataFrame(geocode_results).set_index('index') # Set index to match original DataFrame for merging
    # Merge geocode results back to original DataFrame
    manhattan_df_geocoded = manhattan_df.join(geocode_df)

    # Checking Results to ensure geocoding worked correctly
    if 'latitude' in manhattan_df_geocoded.columns and 'longitude' in manhattan_df_geocoded.columns:
        print ("Geocoding completed successfully.\n Sample of geocoded data:\n")
        print(manhattan_df_geocoded[['premise_address', 'latitude', 'longitude']].head(10))
    else:
        print("Geocoding failed. Latitude and Longitude columns are missing.")
        print(manhattan_df_geocoded['error'].head(10)) 

# Save Our Output to a CSV file for later use in our analysis and visualization steps.
    output_file = "manhattan_liquor_stores_geocoded.csv"
    manhattan_df_geocoded.to_csv(output_file, index=False)
    print(f"Geocoded data saved to {os.path.abspath(output_file)}")

# Visualization on the heatmap
def visualization():
    # Implementation for visualization step    
    df = pd.read_csv("manhattan_liquor_stores_geocoded.csv") # Load our dataset
    df = df.dropna(subset=['latitude', 'longitude']) # Drop rows where geocoding failed (i.e., missing latitude or longitude) to ensure our visualization only includes valid data points.
    df = df['premise_address latitude longitude'.split()] # Select only the columns we need for visualization to streamline our data and reduce memory usage when loading into our visualization script.
    df.to_json("manhattan_stores.json", orient='records') # Save the geocoded data to a JSON file for use in our visualization step. This way we can easily load the geocoded data into our visualization script without having to run the geocoding step again, which can be time-consuming and resource-intensive. By saving the geocoded data to a JSON file, we can quickly and efficiently load the data into our visualization script and generate our heatmap without any delays.
    print (f"Visualization saved to {os.path.abspath('manhattan_stores.json')}")

def main(): 
    if not os.path.exists("manhattan_liquor_stores_geocoded.csv"):
        datasetDL() # Only run the geocoding and dataset download if we don't already have a geocoded dataset saved. This way we can save time and resources by not having to geocode every time we have to run this script to generate the coordinates for visualization.
        visualization() # After geocoding is complete, we can run the visualization step to generate our heatmap. This will use the geocoded data we just generated to create a heatmap of liquor store locations in Manhattan.

    elif not os.path.exists("manhattan_stores.json"):
        print("Geocoded dataset already exists. Skipping geocoding step.")
        visualization()
    else:
        print("Geocoded dataset and JSON file already exist. Skipping geocoding and visualization steps.")
        print("You can now run the visualization script to generate the heatmap using the existing JSON file.") 
if __name__ == "__main__":
    main()