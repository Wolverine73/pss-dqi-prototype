"""
File: m_dqi_vmacros.py
Purpose: Converted from the SAS macro m_dqi_vmacros.
         This script collects DQI macros within BigQuery tables utilized for DQI campaigns.

Logic Overview:
1. Check if the source dataframe exists
2. If it exists, drop the temporary table if it already exists
3. Read the source data into a DataFrame
4. Insert the data into the appropriate BigQuery table
5. Drop the temporary table
6. Update table statistics

Notes:
- This code makes use of global configuration values from a YAML file.
- Logging and exception handling are implemented.
- The script handles both production and development environments.
"""

import logging
import os
import sys
import pandas as pd
import yaml
import m_data_operations as mdo
from m_table_drop_passthrough import table_drop_passthrough
from m_table_statistics import m_table_statistics

# Load shared_variable.yaml
with open('shared_variable.yaml', 'r') as f:
    shared_variables = yaml.safe_load(f)

# Configure logging
macro_test_flag = shared_variables.get('macro_test_flag', 'no')
script_name = os.path.splitext(os.path.basename(__file__))[0]
developer = "Makkena"
if macro_test_flag.lower() == "yes":
    log_filename = f"{script_name}_{developer}.logs"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_filename)
        ]
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

# Global variables from shared_variable.yaml
c_s_tdtempx = shared_variables['c_s_tdtempx']
tdname = shared_variables['tdname']
c_s_dqi_production = shared_variables['c_s_dqi_production']
dqi_storage_project = shared_variables['dqi_storage_project']

def m_dqi_vmacros():
    """
    Collects DQI macros within BigQuery tables utilized for DQI campaigns.
    """
    logging.info("=========================================================")
    logging.info("Start :: m_dqi_vmacros...")
    logging.info("=========================================================")
    
    try:
        # Check if the source table exists
        check_table_query = f"""
        SELECT table_name 
        FROM `{dqi_storage_project}.saslib.INFORMATION_SCHEMA.TABLES` 
        WHERE table_name = 'mbr_vmacros_{tdname}'
        """
        
        table_exists_df = mdo.fetch_bigquery_dataframe(check_table_query, "table_exists")
        
        if not table_exists_df.empty:
            logging.info(f"Table saslib.mbr_vmacros_{tdname} exists. Processing...")
            
            # Drop temporary table if it exists
            table_drop_passthrough(f"{dqi_storage_project}.{c_s_tdtempx}.mbr_vmacros_{tdname}")
            
            # Fetch data from source table
            source_query = f"SELECT * FROM `{dqi_storage_project}.saslib.mbr_vmacros_{tdname}`"
            mbr_vmacros_df = mdo.fetch_bigquery_dataframe(source_query, "mbr_vmacros")
            
            # Write to temporary table
            mdo.write_df_to_bigquery(mbr_vmacros_df, f"{dqi_storage_project}.{c_s_tdtempx}.mbr_vmacros_{tdname}")
            
            # Determine target table based on production flag
            target_table = "dqi_process_macros" if c_s_dqi_production == "Y" else "dqi_process_macros_dev"
            
            # Insert data into target table
            insert_query = f"""
            INSERT INTO `{dqi_storage_project}.{c_s_tdtempx}.{target_table}`
            SELECT
                1,
                DQI_TICKET,
                DQI_APRIMO_ACTIVITY,
                MBR_PROCESS_ID,
                PHYS_PROCESS_ID,
                DQI_MACRO_SCOPE,
                DQI_MACRO_VARIABLE,
                DQI_MACRO_VALUE,
                DQI_USER_ID,
                DQI_USER_NAME,
                DQI_JOB_ID,
                CAST(CURRENT_DATE AS TIMESTAMP) + 
                CAST(FORMAT_TIMESTAMP('%H:%M:%S', CURRENT_TIMESTAMP) AS INTERVAL)
            FROM `{dqi_storage_project}.{c_s_tdtempx}.mbr_vmacros_{tdname}`
            """
            
            mdo.execute_bigquery_query(insert_query)
            logging.info(f"Data inserted into {target_table} successfully.")
            
            # Drop the temporary table
            table_drop_passthrough(f"{dqi_storage_project}.{c_s_tdtempx}.mbr_vmacros_{tdname}")
            
            # Update table statistics
            m_table_statistics(
                data_in=f"{dqi_storage_project}.{c_s_tdtempx}.{target_table}", 
                index_in="row_gid"
            )
            
        else:
            logging.info(f"Table saslib.mbr_vmacros_{tdname} does not exist. No action taken.")
        
        logging.info("=========================================================")
        logging.info("m_dqi_vmacros completed successfully.")
        logging.info("=========================================================")
        
    except Exception as e:
        logging.error(f"Error in m_dqi_vmacros: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        m_dqi_vmacros()
    except Exception as e:
        logging.error(f"Script execution failed with error: {e}")
        sys.exit(1)