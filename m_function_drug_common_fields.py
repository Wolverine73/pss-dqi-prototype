"""
File: m_function_drug_common_fields.py
Purpose: Converted from the SAS macro m_function_drug_common_fields.
         This script processes drug-related fields with common logic for various applications.
Logic Overview:
    1. Handles age range defaults.
    2. Processes GPI (Generic Product Identifier) fields.
    3. Converts wildcards to BigQuery syntax.
    4. Determines drug level and code based on available identifiers.
    5. Sets SQL join type for wildcard conditions.
    6. Processes retail and mail quantity limits based on campaign ID.
Notes:
    - This code uses global variables loaded from a YAML file.
    - The function is designed to be called from other modules that process drug data.
"""

import yaml
import logging
import os
import inspect
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

# Add BigQuery client to mdo module
def fetch_bigquery_dataframe(query, table_name=None, project_id=None):
    """
    Executes a BigQuery query and returns the results as a pandas DataFrame.
    
    Parameters:
        query (str): The SQL query to execute.
        table_name (str, optional): Name to assign to the resulting DataFrame.
        project_id (str, optional): Google Cloud project ID.
        
    Returns:
        pandas.DataFrame: The query results as a DataFrame.
    """
    try:
        # Use project_id from shared variables if not provided
        if not project_id:
            project_id = shared_variables.get('project_id')
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Execute the query
        logging.info(f"Executing BigQuery query: {query[:100]}...")
        query_job = client.query(query)
        
        # Wait for the query to complete and get results
        results = query_job.result()
        
        # Convert to DataFrame
        df = results.to_dataframe()
        
        # Log success
        logging.info(f"Query executed successfully. Returned {len(df)} rows.")
        
        return df
    
    except Exception as e:
        logging.error(f"Error executing BigQuery query: {e}")
        raise

# Add the function to the mdo module
mdo.fetch_bigquery_dataframe = fetch_bigquery_dataframe

def m_function_drug_common_fields(row):
    """
    Processes drug-related fields with common logic.
    
    Parameters:
        row (dict or pandas.Series): A row of drug data with fields to process.
        
    Returns:
        dict or pandas.Series: The processed row with updated fields.
    """
    # Get the caller's name (parent function)
    parent = inspect.currentframe().f_back.f_code.co_name.upper()
    
    # Get campaign ID from shared variables
    c_s_dqi_campaign_id = int(shared_variables.get('c_s_dqi_campaign_id', 0))
    c_s_clnt_spcfc_cmgpn = shared_variables.get('c_s_clnt_spcfc_cmgpn', 'N')
    
    # Process age range defaults
    if not row.get('age_min') or pd.isna(row['age_min']):
        row['age_min'] = 0
    if not row.get('age_max') or pd.isna(row['age_max']):
        row['age_max'] = 999
    
    # Process GPI fields
    if row.get('gpi') and row['gpi'].strip():
        ast = row['gpi'].find('*')
        if ast > 0:
            gpi_len = ast - 1
            drug_sub = row['gpi'].strip()[:ast-1]
            row['gpi_len'] = gpi_len
        else:
            row['gpi_len'] = len(row['gpi'].strip())
            drug_sub = row['gpi'].strip()
        
        # Set b_g and ms_ss if not in m_intake_form_drug
        if parent != "M_INTAKE_FORM_DRUG":
            if not row.get('b_g') or not row['b_g'].strip():
                row['b_g'] = 'A'
            if not row.get('ms_ss') or not row['ms_ss'].strip():
                row['ms_ss'] = 'A'
    else:
        row['gpi_len'] = 0
        drug_sub = ''
        if not row.get('b_g') or not row['b_g'].strip():
            row['b_g'] = 'A'
        if not row.get('ms_ss') or not row['ms_ss'].strip():
            row['ms_ss'] = 'A'
    
    row['drug_sub'] = drug_sub
    
    # Convert wildcards to BigQuery syntax
    if row.get('ndc11'):
        row['ndc11'] = row['ndc11'].replace('?', '_').replace('*', '%')
    if row.get('gpi'):
        row['gpi'] = row['gpi'].replace('?', '_').replace('*', '%')
    
    # Determine drug level and code
    if row.get('ndc11') and row['ndc11'].strip():
        row['druglvl'] = 'NDC11'
        row['drug_code'] = row['ndc11']
    elif row.get('ndc9') and row['ndc9'].strip():
        row['druglvl'] = 'NDC9'
        row['drug_code'] = row['ndc9']
    elif row.get('gpi') and row['gpi'].strip():
        row['druglvl'] = 'GPI'
        row['drug_code'] = drug_sub
    
    # Set SQL join type for wildcard conditions
    row['sql_join'] = ''
    if (row.get('ndc11') and '_' in row['ndc11'] and row['druglvl'] == 'NDC11') or \
       (row.get('gpi') and '_' in row['gpi'] and row['druglvl'] == 'GPI') or \
       (row.get('ndc11') and '%' in row['ndc11'] and row['druglvl'] == 'NDC11') or \
       (row.get('gpi') and '%' in row['gpi'] and row['druglvl'] == 'GPI'):
        row['sql_join'] = 'WILDCARD'
    
    # Initialize limit flags
    row['_exceed_rlimit'] = 0
    row['_exceed_mlimit'] = 0
    row['drugcat'] = row.get('drug_category', '').strip()
    
    # Process retail and mail quantity limits based on campaign ID
    if c_s_dqi_campaign_id in [67, 68, 554]:
        # Balance Formulary has its own limit logic
        if row.get('retail_qty_limit') or row.get('retail_qty_unit') or row.get('retail_qty_time'):
            row['rlimit'] = str(row.get('retail_qty_limit', '')).strip()
        else:
            row['rlimit'] = ' '
            
        if row.get('mail_qty_limit') or row.get('mail_qty_unit') or row.get('mail_qty_time'):
            row['mlimit'] = str(row.get('mail_qty_limit', '')).strip()
        else:
            row['mlimit'] = ' '
    
    elif parent == "M_INTAKE_FORM_DRUG":
        # Intake form processing
        if row.get('retail_qty_limit') or row.get('retail_qty_unit') or row.get('retail_qty_time'):
            _rlimit = f"{str(row.get('retail_qty_limit', '')).strip()} {str(row.get('retail_qty_unit', '')).strip()}/{str(row.get('retail_qty_time', '')).strip()} days"
        else:
            _rlimit = ' '
            
        if row.get('mail_qty_limit') or row.get('mail_qty_unit') or row.get('mail_qty_time'):
            _mlimit = f"{str(row.get('mail_qty_limit', '')).strip()} {str(row.get('mail_qty_unit', '')).strip()}/{str(row.get('mail_qty_time', '')).strip()} days"
        else:
            _mlimit = ' '
        
        # Check for limit length exceeding 200 characters
        if len(_rlimit) > 200:
            row['_exceed_rlimit'] = 1
        if len(_mlimit) > 200:
            row['_exceed_mlimit'] = 1
            
        row['rlimit'] = _rlimit[:200]
        row['mlimit'] = _mlimit[:200]
        
        # Clean up invalid limit formats
        if row['rlimit'].strip() and row['rlimit'].strip().startswith('./'):
            row['rlimit'] = ' '
        if row['mlimit'].strip() and row['mlimit'].strip().startswith('./'):
            row['mlimit'] = ' '
    
    # BOB Health Exchange doesn't need this logic or BF client specific
    elif not ((c_s_dqi_campaign_id in [20, 558] and c_s_clnt_spcfc_cmgpn == 'N') or c_s_dqi_campaign_id == 66):
        if row.get('retail_qty_limit') or row.get('retail_qty_unit') or row.get('retail_qty_time'):
            row['rlimit'] = f"{str(row.get('retail_qty_limit', '')).strip()} {str(row.get('retail_qty_unit', '')).strip()}/{str(row.get('retail_qty_time', '')).strip()} days"
        else:
            row['rlimit'] = ' '
            
        if row.get('mail_qty_limit') or row.get('mail_qty_unit') or row.get('mail_qty_time'):
            row['mlimit'] = f"{str(row.get('mail_qty_limit', '')).strip()} {str(row.get('mail_qty_unit', '')).strip()}/{str(row.get('mail_qty_time', '')).strip()} days"
        else:
            row['mlimit'] = ' '
        
        # Clean up invalid limit formats
        if row['rlimit'].strip() and row['rlimit'].strip().startswith('./'):
            row['rlimit'] = ' '
        if row['mlimit'].strip() and row['mlimit'].strip().startswith('./'):
            row['mlimit'] = ' '
    
    return row

# Example usage in a DataFrame context
import pandas as pd

def apply_drug_common_fields(df):
    """
    Apply the drug common fields function to each row in a DataFrame.
    
    Parameters:
        df (pandas.DataFrame): DataFrame containing drug data.
        
    Returns:
        pandas.DataFrame: Processed DataFrame with updated fields.
    """
    return df.apply(m_function_drug_common_fields, axis=1)

if __name__ == "__main__":
    # Example usage
    try:
        # Create a sample drug record
        sample_drug = {
            'age_min': None,
            'age_max': None,
            'gpi': '1234*',
            'ndc11': '12345678901',
            'ndc9': '123456789',
            'b_g': '',
            'ms_ss': '',
            'retail_qty_limit': '30',
            'retail_qty_unit': 'tabs',
            'retail_qty_time': '30',
            'mail_qty_limit': '90',
            'mail_qty_unit': 'tabs',
            'mail_qty_time': '90',
            'drug_category': 'ANTIBIOTIC'
        }
        
        # Convert to pandas Series for processing
        drug_series = pd.Series(sample_drug)
        
        # Process the drug record
        processed_drug = m_function_drug_common_fields(drug_series)
        
        # Print the processed record
        for key, value in processed_drug.items():
            print(f"{key}: {value}")
            
    except Exception as e:
        logging.error(f"Error in example: {e}")
        raise
