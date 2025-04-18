import argparse
import configparser
import os
from pyiceberg.catalog import load_catalog
import logging


### This script retrieves Iceberg metadata paths from a Unity Catalog in Databricks.
### It uses the PyIceberg library to connect to the catalog and list tables.
### It filters out excluded schemas and logs the process.
# Prerequisites:
# - PyIceberg library installed
# - You need a config.ini file with the necessary credentials and necessary profile
### Config.ini example:
# [E2DEMO]
# CATALOG_TYPE = UC
# CATALOG_NAMESPACE = aka_interop
# CATALOG_URL = e2-demo-field-eng.cloud.databricks.com
# CATALOG_CREDENTIAL = dapixxxxxxxxxxxxxx




# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Select configuration profile.")
parser.add_argument("-p", "--profile", required=True, help="Configuration profile to use (e.g., SANDBOX or E2DEMO).")
args = parser.parse_args()

logger.info(f"Using profile: {args.profile}")

# Load configuration from a config file
config = configparser.ConfigParser()
config.read('pyiceberg/config.ini')

# Read the profile type from the configuration
profile_type = config[args.profile]['CATALOG_TYPE']
logger.info(f"Profile type: {profile_type}")

if profile_type == "GLUE":
    logger.info("Configuring AWS environment for Glue catalog.")
    # Set AWS environment variables from the profile
    os.environ['AWS_DEFAULT_REGION'] = config[args.profile]['AWS_DEFAULT_REGION']
    os.environ['AWS_ACCESS_KEY_ID'] = config[args.profile]['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config[args.profile]['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_SESSION_TOKEN'] = config[args.profile]['AWS_SESSION_TOKEN']
    catalog = load_catalog("glue", **{"type": "glue"})
    logger.info("Glue catalog configured successfully.")

elif profile_type == "UC":
    # Load Unity Catalog configuration from the profile
    CATALOG_NAMESPACE = config[args.profile]['CATALOG_NAMESPACE']
    CATALOG_URL = config[args.profile]['CATALOG_URL']
    CATALOG_CREDENTIAL = config[args.profile]['CATALOG_CREDENTIAL']

    # Connect to Unity Catalog
    uc_catalog_properties = {
        'type': 'rest',
        'uri': f"https://{CATALOG_URL}/api/2.1/unity-catalog/iceberg-rest",
        'token': CATALOG_CREDENTIAL,
        'warehouse': CATALOG_NAMESPACE,
    }
    catalog = load_catalog(**uc_catalog_properties)
    logger.info(
        "Unity Catalog configured successfully.\n"
        f"CATALOG_URL: {CATALOG_URL}\n"
        f"CATALOG_NAMESPACE: {CATALOG_NAMESPACE}"
    )

else:
    logger.error(f"Unsupported CATALOG_TYPE: {profile_type}")
    raise ValueError(f"Unsupported CATALOG_TYPE: {profile_type}")

def get_iceberg_metadata_paths(catalog, excluded_schemas=None):
    if excluded_schemas is None:
        excluded_schemas = set()  # e.g., {"default", "information_schema"}
    
    results = {}
    
    # List all namespaces (schemas)
    for namespace in catalog.list_namespaces():
        schema_name = '.'.join(namespace)
        logger.info(f"Checking schema: {schema_name}")

        if schema_name in excluded_schemas:
            logger.info(f"Skipping excluded schema: {schema_name}")
            continue
        
        # List tables in namespace
        tables = catalog.list_tables(namespace)
        
        for table_id in tables:
            table_name = '.'.join(table_id)
            try:
                table = catalog.load_table(table_id)
                
                # Verify Iceberg table through metadata properties
                if table.metadata and table.metadata_location:
                    results[table_name] = table.metadata_location
                    
            except Exception as e:
                logger.error(f"Error processing {table_name}: {str(e)}")
    
    return results

if __name__ == "__main__":
    metadata_paths = get_iceberg_metadata_paths(catalog,
                                                excluded_schemas={"default", "information_schema"}
                                                )
    print("Iceberg Metadata Paths:")
    for table, path in metadata_paths.items():
        print(f"{table}: {path}")
