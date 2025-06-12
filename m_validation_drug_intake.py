import yaml  # Updated to use YAML
import logging
import os
import m_data_operations as mdo
import m_abend_handler
import pandasql as psql

# Load from shared_variable.yaml
with open('shared_variable.yaml', 'r') as f:
    shared_variables = yaml.safe_load(f)  # Updated to load YAML

# Configure logging
macro_test_flag = shared_variables['macro_test_flag']  # Set this to "yes" to enable logging to a file
script_name = os.path.splitext(os.path.basename(__file__))[0]
developer = "Makkena"
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

tdname = shared_variables['tdname']
c_s_tdtempx = shared_variables['c_s_tdtempx']
c_s_schema = shared_variables['c_s_schema']
table_out = shared_variables['table_out']
c_s_dqi_campaign_id = shared_variables['c_s_dqi_campaign_id']
c_s_filedir = shared_variables['c_s_filedir']
dqi_storage_project = shared_variables['dqi_storage_project']
def m_validation_drug_intake(tdname, c_s_tdtempx, c_s_schema, table_out, c_s_dqi_campaign_id, c_s_filedir):
    logging.info("=========================================================")
    logging.info("Start :: m_validation_drug_intake...")
    logging.info("=========================================================")
    """
    Validates drug intake by processing data from Teradata.

    Parameters:
        tdname (str): The name of the table.
        c_s_tdtempx (str): Temporary table schema.
        c_s_schema (str): Schema for drug denormalization.
        table_out (str): Output table name.
        c_s_dqi_campaign_id (int): Campaign ID.
        c_s_filedir (str): Directory for output files.
    """
    try:
        # Step 1: Drop previous table
        #logging.info(f"Dropping previous table: {c_s_tdtempx}.drug_validity_{tdname}")
        mdo.table_drop_passthrough(f"{c_s_tdtempx}.drug_validity_{tdname}")
        logging.info("Previous table dropped successfully.")

        # Step 2: Create drug validity table
        logging.info(f"Creating drug validity table: drug_validity_{tdname}")
        create_table_query = f"""
        CREATE TABLE `{dqi_storage_project}.{c_s_tdtempx}.drug_validity_{tdname}` AS
        SELECT DISTINCT
            COALESCE(DRUG.DRUG_PROD_GID, 0) AS DRUG_PROD_GID,
            drug.drug_id,
            drug.BRND_GNRC_CD,
            drug.DRUG_MULTI_SRC_CD,
            drug.PBM_DRUG_MULTI_SRC_CD,
            drug.OTC_DRUG_IND,
            mydrug.rec_type,
            mydrug.drug_code,
            mydrug.druglvl,
            mydrug.ndc9,
            mydrug.ndc11,
            mydrug.gpi,
            mydrug.drug_desc,
            mydrug.b_g,
            mydrug.ms_ss,
            'PBM ' AS DRUG_DEFN
            {', '.join(['mydrug.mony_m', 'mydrug.mony_o', 'mydrug.mony_n', 'mydrug.mony_y', 'mydrug.rx', 'mydrug.otc']) if c_s_dqi_campaign_id in [66, 67, 554] else ''}
        FROM `{dqi_storage_project}.{table_out}` mydrug
        LEFT JOIN `{dqi_storage_project}.{c_s_schema}.v_drug_denorm` drug
        ON ((mydrug.druglvl = 'NDC11' AND drug.drug_id LIKE mydrug.drug_code)
        OR (mydrug.druglvl = 'NDC9' AND mydrug.drug_code = SUBSTR(drug.drug_id, 1, 9))
        OR (mydrug.druglvl = 'GPI' AND TRIM(drug.gpi_cd) LIKE (TRIM(mydrug.drug_code) || '%')))
        WHERE drug.DRUG_PROD_GID IS NOT NULL;
        """
        mdo.execute_bigquery_query(create_table_query)
        logging.info("Drug validity table created successfully.")

        # Step 3: Insert into drug validity table
        logging.info(f"Inserting data into drug validity table: drug_validity_{tdname}")
        insert_query = f"""
        INSERT INTO `{dqi_storage_project}.{c_s_tdtempx}.drug_validity_{tdname}`
        SELECT DISTINCT
            COALESCE(DRUG.DRUG_PROD_GID, 0) AS DRUG_PROD_GID,
            drug.drug_id,
            drug.MDDB_BRND_GNRC_CD AS BRND_GNRC_CD,
            drug.MDDB_DRUG_MULTI_SRC_CD AS DRUG_MULTI_SRC_CD,
            drug.MDDB_MULTSRC_CD AS PBM_DRUG_MULTI_SRC_CD,
            drug.OTC_DRUG_IND,
            mydrug.rec_type,
            mydrug.drug_code,
            mydrug.druglvl,
            mydrug.ndc9,
            mydrug.ndc11,
            mydrug.gpi,
            mydrug.drug_desc,
            mydrug.b_g,
            mydrug.ms_ss,
            'MDDB' AS DRUG_DEFN
            {', '.join(['mydrug.mony_m', 'mydrug.mony_o', 'mydrug.mony_n', 'mydrug.mony_y', 'mydrug.rx', 'mydrug.otc']) if c_s_dqi_campaign_id in [66, 67, 554] else ''}
        FROM `{dqi_storage_project}.{table_out}` mydrug
        LEFT JOIN `{dqi_storage_project}.{c_s_schema}.v_drug_denorm` drug
        ON ((mydrug.druglvl = 'NDC11' AND drug.drug_id LIKE mydrug.drug_code)
        OR (mydrug.druglvl = 'NDC9' AND mydrug.drug_code = SUBSTR(drug.drug_id, 1, 9))
        OR (mydrug.druglvl = 'GPI' AND TRIM(drug.gpi_cd) LIKE CONCAT(TRIM(mydrug.drug_code), '%')))
        WHERE drug.DRUG_PROD_GID IS NOT NULL;
        """
        mdo.execute_bigquery_query(insert_query)
        logging.info("Data inserted into drug validity table successfully.")

        # Step 4: Delete invalid rows based on conditions
        logging.info("Deleting invalid rows based on conditions...")
        if c_s_dqi_campaign_id in [66, 67, 554]:
            delete_query = f"""
            DELETE FROM `{dqi_storage_project}.{c_s_tdtempx}.drug_validity_{tdname}`
            WHERE druglvl = 'GPI'
            AND ((COALESCE(mony_m, ' ') = 'E' AND PBM_DRUG_MULTI_SRC_CD = 'M')
            OR (COALESCE(mony_o, ' ') = 'E' AND PBM_DRUG_MULTI_SRC_CD = 'O')
            OR (COALESCE(mony_n, ' ') = 'E' AND PBM_DRUG_MULTI_SRC_CD = 'N')
            OR (COALESCE(mony_y, ' ') = 'E' AND PBM_DRUG_MULTI_SRC_CD = 'Y')
            OR (COALESCE(rx, ' ') = 'E' AND COALESCE(OTC_DRUG_IND, ' ') <> 'Y')
            OR (COALESCE(otc, ' ') = 'E' AND OTC_DRUG_IND = 'Y'));
            """
        else:
            delete_query = f"""
            DELETE FROM `{dqi_storage_project}.{c_s_tdtempx}.drug_validity_{tdname}`
            WHERE (b_g = 'B' AND BRND_GNRC_CD = 'GNRC')
            OR (b_g = 'G' AND BRND_GNRC_CD = 'BRND')
            OR (ms_ss IN ('M', 'MS') AND DRUG_MULTI_SRC_CD = 'SINGLE')
            OR (ms_ss IN ('S', 'SS') AND DRUG_MULTI_SRC_CD = 'MULTI');
            """
        mdo.execute_bigquery_query(delete_query)
        logging.info("Invalid rows deleted successfully.")

        # Step 5: Drop duplicates
        #logging.info("Dropping duplicate rows...")
        #subprocess.call(["python", "m_table_drop.py", "validate_drug_duplicates2"])

        # Step 6: Create validate_drug_duplicates2 table
        logging.info("Creating validate_drug_duplicates2 dataset...")
        validate_duplicates_query_str = f"""
            WITH temp_tbl AS (
                SELECT 
                rec_type, 
                drug_defn, 
                drug_id,
                COUNT(*) AS cnt
                FROM `{dqi_storage_project}.{c_s_tdtempx}.drug_validity_{tdname}`
                GROUP BY rec_type, drug_defn, drug_id
                HAVING cnt > 1
            )
            SELECT 
                a.rec_type,
                a.drug_id,
                a.drug_desc,
                a.ndc9,
                a.ndc11,
                a.gpi,
                a.b_g,
                a.ms_ss,
                a.druglvl,
                a.BRND_GNRC_CD,
                a.DRUG_MULTI_SRC_CD
            FROM `{dqi_storage_project}.{c_s_tdtempx}.drug_validity_{tdname}` a
            INNER JOIN temp_tbl b
                ON a.rec_type = b.rec_type
                AND a.drug_id = b.drug_id
            ORDER BY a.rec_type, a.drug_id
            """
        validate_drug_duplicates2_df = mdo.fetch_bigquery_dataframe(validate_duplicates_query_str,"validate_drug_duplicates2")
        logging.info("validate_drug_duplicates2 table created successfully.")

        # Step 7: Check for duplicates
        cnt_validation_duplicates = len(validate_drug_duplicates2_df)
        logging.info(f"Number of duplicate rows found: {cnt_validation_duplicates}")

        if cnt_validation_duplicates > 0:
            logging.error("Duplicate rows found. Exporting to GCS and raising an exception.")
            c_s_rootdir = shared_variables['c_s_rootdir']
            c_s_program = shared_variables['c_s_program']
            c_s_proj = shared_variables['c_s_proj']
            c_s_ticket = shared_variables['c_s_ticket']
            dtf_out = f"{c_s_rootdir}\\{c_s_program}\\{c_s_proj}"
            file_base_name = f"validate_drug_t{c_s_ticket}"
            file_type = 'excel'

            # Export duplicates to Excel
            mdo.write_df_to_gcs(
                df=validate_drug_duplicates2_df,
                file_type=file_type,
                gcs_dir=dtf_out,
                file_name=file_base_name
            )

            # Log error and handle abend
            error_message = f"ERROR: abend message 5 - validate_drug_{tdname}.xlsx"
            logging.error(error_message)
            m_abend_handler(
                abend_message_id=5,
                abend_report=f"validate_drug_{tdname}.xlsx"
            )
            raise Exception(error_message)

        # Step 8: Fetch and sort exclusion and inclusion rows
        logging.info("Fetching and sorting exclusion and inclusion rows...")
        for rec_type, var_name in [('I', 'drug_intake_i'), ('E', 'drug_intake_e')]:
            query_str = f"SELECT * FROM `{dqi_storage_project}.{c_s_tdtempx}.drug_validity_{tdname}` WHERE rec_type='{rec_type}'"
            df = mdo.fetch_bigquery_dataframe(query_str,var_name)
            logging.info(f"Columns in the fetched DataFrame for rec_type '{rec_type}': {df.columns}")
            if 'drug_id' in df.columns:
                df = df.sort_values(by='drug_id')
                globals()[f"{var_name}_df"] = df
                logging.info(f"Fetched and sorted rows for rec_type: {rec_type}")
            else:
                logging.error(f"The 'drug_id' column is missing in the fetched DataFrame for rec_type: {rec_type}")

        # Step 9: Identify invalid exclusions
        logging.info("Identifying invalid exclusions...")
        invalid_exclusions_query = """select drug_id, rec_type from drug_intake_e_df where drug_id not in (select drug_id from drug_intake_i_df)"""
        invalid_exclusions_df = psql.sqldf(invalid_exclusions_query)
        cnt_validation_inclusions = len(invalid_exclusions_df)
        logging.info(f"Number of invalid exclusions found: {cnt_validation_inclusions}")

        if cnt_validation_inclusions > 0:
            logging.error("Invalid exclusions found. Exporting to GCS and raising an exception.")
            dtf_out = f"{c_s_rootdir}\\{c_s_program}\\{c_s_proj}"
            file_base_name = f"{c_s_ticket}"
            file_type = 'excel'

            # Export invalid exclusions to GCS
            mdo.write_df_to_gcs(
                df=invalid_exclusions_df,
                file_type=file_type,
                gcs_dir=dtf_out,
                file_name=file_base_name
            )

            # Log error and handle abend
            error_message = f"ERROR: abend message 6 - {file_base_name}.xlsx"
            logging.error(error_message)
            m_abend_handler(
                abend_message_id=6,
                abend_report=f"{file_base_name}.xlsx"
            )
            raise Exception(error_message)

        logging.info("Drug intake validation process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during drug intake validation. Error: {e}")
        raise
    logging.info("=========================================================")
    logging.info("m_validation_drug_intake completed successfully.")
    logging.info("=========================================================")
if __name__ == "__main__":
    try:
        m_validation_drug_intake(tdname, c_s_tdtempx, c_s_schema, table_out, c_s_dqi_campaign_id, c_s_filedir)
    except Exception as e:
        logging.error(f"Script execution failed with error: {e}")
        raise