"""
File: m_create_table_bulkload_data.py
Purpose: Converted from the SAS macro m_create_table_bulkload_data.
         This script prepares data for bulk loading by cleaning and aligning columns with a target table.
Logic Overview:
    1. Optionally cleans data by removing non-printable special characters.
    2. Gets column metadata from both source and target tables.
    3. Identifies columns that exist in the target but not in the source.
    4. Creates default values for missing columns (empty strings for character, nulls for numeric).
    5. Keeps only columns that exist in the target table.
Notes:
    - This code uses global variables loaded from a YAML file.
    - The function is designed to work with BigQuery instead of Teradata.
    - It handles column type differences between source and target tables.
"""

import yaml
import logging
import os
import pandas as pd
import m_data_operations as mdo
import m_clean_table_bulkload_data
from google.cloud import bigquery

# Load shared variables from YAML file
with open('shared_variable.txt', 'r') as f:
    shared_variables = yaml.safe_load(f)

# Configure logging
macro_test_flag = shared_variables.get('macro_test_flag', 'no')
script_name = os.path.splitext(os.path.basename(__file__))[0]
developer = "Karthik"

if macro_test_flag.lower() == "yes":
    log_filename = f"{script_name}_{developer}.logs"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler(log_filename)  # Log to file
        ]
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()  # Log to console only
        ]
    )

def m_create_table_bulkload_data(libname=None, data_set=None, db_table=None):
    """
    Prepares data for bulk loading by cleaning and aligning columns with a target table.
    
    Parameters:
        libname (str): The library/dataset name for the source data.
        data_set (str): The table name for the source data.
        db_table (str): The target table name.
        
    Returns:
        pandas.DataFrame: The processed DataFrame ready for loading.
    """
    logging.info(f"Starting m_create_table_bulkload_data with libname={libname}, data_set={data_set}, db_table={db_table}")
    
    try:
        # Step 1: Clean data if campaign ID is not 99
        c_s_dqi_campaign_id = int(shared_variables.get('c_s_dqi_campaign_id', 0))
        
        if c_s_dqi_campaign_id != 99:
            logging.info("Cleaning data to remove non-printable special characters")
            m_clean_table_bulkload_data.m_clean_table_bulkload_data(
                libname=libname,
                data_set=data_set
            )
        
        # Step 2: Get column metadata from target table
        logging.info(f"Getting column metadata from target table: {db_table}")
        
        # Use BigQuery client to get schema information
        client = bigquery.Client()
        
        # Extract dataset and table name for target table
        if '.' in db_table:
            target_project_dataset, target_table = db_table.split('.')
            target_project, target_dataset = target_project_dataset.split(':') if ':' in target_project_dataset else (None, target_project_dataset)
        else:
            target_dataset, target_table = None, db_table
        
        # Get target table schema
        if target_dataset:
            target_table_ref = client.dataset(target_dataset).table(target_table)
            target_table_obj = client.get_table(target_table_ref)
            target_schema = target_table_obj.schema
            
            # Create db_variables DataFrame with column metadata
            db_variables = pd.DataFrame([
                {
                    'name': field.name.lower(),
                    'varnum': i,
                    'type': 2 if field.field_type in ['STRING', 'BYTES', 'DATE', 'DATETIME', 'TIME', 'TIMESTAMP'] else 1,
                    'format': field.field_type,
                    'length': field.mode
                }
                for i, field in enumerate(target_schema)
            ])
        else:
            # If no dataset specified, fetch the DataFrame and get its schema
            target_df = mdo.fetch_bigquery_dataframe(f"SELECT * FROM {db_table} LIMIT 0")
            
            # Create db_variables DataFrame with column metadata
            db_variables = pd.DataFrame([
                {
                    'name': col.lower(),
                    'varnum': i,
                    'type': 2 if target_df[col].dtype == 'object' else 1,
                    'format': str(target_df[col].dtype),
                    'length': 0
                }
                for i, col in enumerate(target_df.columns)
            ])
        
        # Step 3: Get column names from source table
        logging.info(f"Getting column names from source table: {libname}.{data_set}")
        
        # Extract dataset and table name for source table
        if '.' in data_set:
            source_project_dataset, source_table = data_set.split('.')
            source_project, source_dataset = source_project_dataset.split(':') if ':' in source_project_dataset else (None, source_project_dataset)
        else:
            source_dataset, source_table = libname, data_set
        
        # Get source table schema
        source_table_ref = client.dataset(source_dataset).table(source_table)
        source_table_obj = client.get_table(source_table_ref)
        source_schema = source_table_obj.schema
        
        # Create ds_variables DataFrame with column names
        ds_variables = pd.DataFrame([
            {
                'name': field.name.lower()
            }
            for field in source_schema
        ])
        
        # Step 4: Find matching and non-matching columns
        logging.info("Finding matching and non-matching columns")
        
        # Merge on column name
        merged_df = pd.merge(db_variables, ds_variables, on='name', how='left', indicator=True)
        
        # Split into matching and non-matching columns
        match_variables = merged_df[merged_df['_merge'] == 'both'].drop('_merge', axis=1)
        nomatch_variables = merged_df[merged_df['_merge'] == 'left_only'].drop('_merge', axis=1)
        
        # Step 5: Create assignment statements for non-matching columns
        logging.info("Creating assignment statements for non-matching columns")
        
        nomatch_variable_counts = len(nomatch_variables)
        nomatch_variable_names = ' '.join(nomatch_variables['name'].tolist())
        
        logging.info(f"NOTE: nomatch_variable_counts = {nomatch_variable_counts}")
        logging.info(f"NOTE: nomatch_variable_names = {nomatch_variable_names}")
        
        # Create assignment statements
        assign_nomatch = nomatch_variables.copy()
        assign_nomatch['assign_variable'] = assign_nomatch.apply(
            lambda row: f"{row['name']} = ''" if row['type'] == 2 else f"{row['name']} = None", 
            axis=1
        )
        
        assign_variable = ' '.join(assign_nomatch['assign_variable'].tolist())
        
        # Step 6: Get list of columns to keep
        keep_variables = ' '.join(db_variables['name'].tolist())
        
        # Step 7: Fetch the source data
        logging.info(f"Fetching data from source table: {libname}.{data_set}")
        source_query = f"SELECT * FROM `{source_dataset}.{source_table}`"
        source_df = mdo.fetch_bigquery_dataframe(source_query)
        
        # Step 8: Process the data
        logging.info("Processing the data")
        
        # Convert column names to lowercase
        source_df.columns = [col.lower() for col in source_df.columns]
        
        # Add missing columns with default values
        for _, row in nomatch_variables.iterrows():
            if row['type'] == 2:  # Character
                source_df[row['name']] = ''
            else:  # Numeric
                source_df[row['name']] = None
        
        # Keep only columns in the target table
        keep_cols = [col for col in db_variables['name'] if col in source_df.columns]
        result_df = source_df[keep_cols]
        
        # Step 9: Store the result
        globals()[data_set] = result_df
        
        logging.info(f"Data processing completed. Result stored in {data_set}")
        
        return result_df
        
    except Exception as e:
        logging.error(f"Error in m_create_table_bulkload_data: {e}")
        raise

if __name__ == "__main__":
    try:
        # Get parameters from shared variables
        tdname = shared_variables.get('tdname', '')
        table_out = shared_variables.get('table_out', '')
        
        # Example usage
        result_df = m_create_table_bulkload_data(
            libname="work",
            data_set=f"address_{tdname}",
            db_table=table_out
        )
        
        logging.info(f"Processed DataFrame has {len(result_df)} rows and {len(result_df.columns)} columns")
        
    except Exception as e:
        logging.error(f"Script execution failed with error: {e}")
        raise
