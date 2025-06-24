import pandas as pd
import requests
from sqlalchemy import create_engine
import sqlalchemy
import os
from dotenv import load_dotenv
import psycopg2
import logging

# Setup basic logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(r"logs\etl_log.log"),
        logging.StreamHandler()
    ]
)

def extract_record(url, headers):
    logging.info("Extracting data from API...")
    try:
        property_records = requests.get(url, headers=headers)
        property_records.raise_for_status()
        logging.info("Data extraction successful!")
        return property_records.json()
    except Exception as e:
        logging.error(f"Error during data extraction: {e}")
        raise



def transform_data(property_records):
    logging.info("Transforming data...")
    try:
        # Transformations...
        property_main = [
        {
            'id' : item.get('id', 'NaN'), 'addressLine1' : item.get('addressLine1', 'NaN') , 'addressLine2' : item.get('addressLine2', 'NaN'), \
            'city' : item.get('city', 'NaN'), 'state' : item.get('state', 'NaN'), 'zipCode' : item.get('zipCode', 'NaN'), \
            'county' : item.get('county', 'NaN'), 'latitude' : item.get('latitude', 'NaN'), 'longitude' : item.get('longitude', 'NaN'), \
            'propertyType' : item.get('propertyType', 'NaN'), 'bedrooms' : item.get('bedrooms', 'NaN'), 'bathrooms' : item.get('bathrooms', 'NaN'), \
            'ownerOccupied' : item.get('ownerOccupied', 'NaN')
        } for item in property_records
        ]
        
        property_main_df = pd.DataFrame(property_main)

        # Get property features information
        property_features = [
        {'propertyId': property['id'], **property['features']} # '**' to unpack the 'features' dict to merge with property_id in a new dict
        for property in property_records if 'features' in property
        ]
        logging.info(f"Transformed {len(property_main_df)} property_main_df records.")

        property_features = [
        {'propertyId': property['id'], **property['features']} # unpack 'features' and merge with property_id in new dict
        for property in property_records if 'features' in property
        ]

        property_features_df = pd.DataFrame(property_features).drop(
            columns=['pool', 'fireplaceType', 'poolType', 'fireplace'],
            errors='ignore'
        )
        logging.info(f"Transformed {len(property_features_df)} property_main_df records.")

        property_features_df.drop_duplicates(subset=['propertyId'], inplace=True)

        property_owner = [
        {'ownerNames': property['owner']['names'], 'type': property['owner'].get('type', 'NaN'), 'propertyId': property['id'], \
        **property['owner'].get('mailingAddress', {})} for property in property_records if 'owner' in property # Unpack customer mailingAddress dict to merge with new dict
        ]
        
        property_owner_df = pd.DataFrame(property_owner).drop(columns=['formattedAddress'])
        logging.info(f"Transformed {len(property_owner_df)} property_main_df records.")

        property_owner_df['ownerNames'] = property_owner_df['ownerNames'].apply(lambda x: ', '.join(map(str, x))) # Convert list to string

        property_owner_df.drop_duplicates(subset=['ownerNames', 'propertyId', 'addressLine1'], inplace=True)

        logging.info(f"Completed data transformation.")

        return property_main_df, property_features_df, property_owner_df
    
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise



def load_data(property_main_df, property_features_df, property_owner_df):
    logging.info("Loading data into PostgreSQL...")

    try:
        USERNAME = os.environ['user']
        PASSWORD = os.environ['db-passwrd']
        SERVER = os.environ['server']
        DATABASE = os.environ['database']
        PORT = "5432"

        db_url = sqlalchemy.URL.create(
            drivername="postgresql+psycopg2",
            username=USERNAME,
            password=PASSWORD,  # plain (unescaped) text
            host=SERVER,
            database=DATABASE,
            port=PORT
        )
        engine = create_engine(db_url, echo=True)

        # engine = create_engine(f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{SERVER}:5432/{DATABASE}")
        con = engine.connect()

        property_main_df.to_sql('property', con, schema=None, if_exists='replace', index=False)
        property_features_df.to_sql('propertyFeatures', con, schema=None, if_exists='replace', index=True, index_label='id')
        property_owner_df.to_sql('propertyOwner', con, schema=None, if_exists='replace', index=True, index_label='ownerId')

        logging.info("Data successfully loaded into database!")
        con.close()
    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        raise

if __name__ == "__main__":
    logging.info("Starting ETL pipeline...")
    try:
        load_dotenv()
        API_KEY = os.environ['api-key']
        headers = {"Accept": "application/json", "X-Api-Key": API_KEY}
        url = 'https://api.rentcast.io/v1/properties?limit=500'

        property_records = extract_record(url, headers)
        property_main_df, property_features_df, property_owner_df = transform_data(property_records)
        load_data(property_main_df, property_features_df, property_owner_df)

        logging.info("ETL pipeline completed successfully!")
    except Exception as e:
        logging.critical(f"ETL pipeline failed: {e}")

