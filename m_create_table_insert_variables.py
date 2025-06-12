"""
File: m_create_table_insert_variables.py
Purpose: Converted from the SAS macro m_create_table_insert_variables.
         This script creates a comma-separated list of column names from a dataset for use in SQL INSERT statements.
Logic Overview:
    1. Extracts column metadata from a source dataset.
    2. Sorts the columns by their original order.
    3. Handles special cases for 'row_gid' and 'dqi_ts' columns.
    4. Creates a comma-separated list of column names.
    5. Stores the result in a global variable for use in SQL statements.
Notes:
    - This code uses global variables loaded from a YAML file.
    - The function is designed to work with BigQuery instead of Teradata.
"""

import yaml
import logging
import os
import pandas as pd
import m_data_operations as mdo
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

def m_create_table_insert_variables(data_set=None, macro_variable=None):
    """
    Creates a comma-separated list of column names from a dataset for use in SQL INSERT statements.
    
    Parameters:
        data_set (str): The name of the dataset to extract column metadata from.
        macro_variable (str): The name of the global variable to store the result.
        
    Returns:
        str: The comma-separated list of column names.
    """
    logging.info(f"Starting m_create_table_insert_variables with data_set={data_set}, macro_variable={macro_variable}")
    
    try:
        # Step 1: Get column metadata from data_set
        logging.info(f"Getting column metadata from {data_set}")
        
        # Use BigQuery client to get schema information
        client = bigquery.Client()
        
        # Extract dataset and table name
        if '.' in data_set:
            project_dataset, table = data_set.split('.')
            project, dataset = project_dataset.split(':') if ':' in project_dataset else (None, project_dataset)
        else:
            dataset, table = None, data_set
            
        # Get table schema
        if dataset:
            table_ref = client.dataset(dataset).table(table)
            table_obj = client.get_table(table_ref)
            schema = table_obj.schema
            
            # Create temp_variables DataFrame with column metadata
            temp_variables = pd.DataFrame([
                {
                    'name': field.name,
                    'varnum': i,
                    'type': 2 if field.field_type in ['STRING', 'BYTES', 'DATE', 'DATETIME', 'TIME', 'TIMESTAMP'] else 1,
                    'format': field.field_type,
                    'length': field.mode
                }
                for i, field in enumerate(schema)
            ])
        else:
            # If no dataset specified, fetch the DataFrame and get its schema
            data_set_df = mdo.fetch_bigquery_dataframe(f"SELECT * FROM {data_set} LIMIT 0")
            
            # Create temp_variables DataFrame with column metadata
            temp_variables = pd.DataFrame([
                {
                    'name': col,
                    'varnum': i,
                    'type': 2 if data_set_df[col].dtype == 'object' else 1,
                    'format': str(data_set_df[col].dtype),
                    'length': 0
                }
                for i, col in enumerate(data_set_df.columns)
            ])
        
        # Step 2: Sort by original column order
        temp_variables = temp_variables.sort_values('varnum')
        
        # Step 3: Process column names
        temp_variables['name'] = temp_variables['name'].str.lower()
        
        # Step 4: Handle special cases
        for i, row in temp_variables.iterrows():
            if row['name'] == 'row_gid':
                temp_variables.at[i, 'name'] = '1'
            elif row['name'] == 'dqi_ts':
                temp_variables.at[i, 'name'] = "CURRENT_TIMESTAMP()"
        
        # Step 5: Create comma-separated list
        logging.info("Creating comma-separated list of column names")
        result = ', '.join(temp_variables['name'].tolist())
        
        # Step 6: Store in global variable
        globals()[macro_variable] = result
        
        logging.info(f"NOTE: macro {macro_variable} = {result[:100]}...")
        
        return result
        
    except Exception as e:
        logging.error(f"Error in m_create_table_insert_variables: {e}")
        raise

if __name__ == "__main__":
    try:
        # Example usage
        data_set = "example_dataset.example_table"
        macro_variable = "insert_columns"
        
        result = m_create_table_insert_variables(
            data_set=data_set,
            macro_variable=macro_variable
        )
        
        print(f"Generated insert columns: {result}")
        
    except Exception as e:
        logging.error(f"Script execution failed with error: {e}")
        raise
