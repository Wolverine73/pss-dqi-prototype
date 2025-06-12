"""
File: m_abend_handler.py
Purpose: Converted from the SAS macro m_abend_handler.
         This script stops processing on a critical error event and invokes email alert routines.

Logic Overview:
1. If the error message ID (abend_message_id) is 0 then:
   - In “non-triage” campaigns (c_s_dqi_campaign_id < 500) it calls m_email_fail.
   - In “triage” campaigns (c_s_dqi_campaign_id ≥ 500) it concatenates failure message text.
2. If abend_message_id is nonzero then:
   - It processes the error message (simulating data steps and transposes)
   - For triage campaigns (c_s_dqi_campaign_id ≥ 500), it concatenates error text unless the message is INFORMATIVE or WARNING.
   - For non-triage campaigns it calls m_email_fail_msg using a subject that depends on the (simulated) dqi_message_type.
3. Finally, for non-triage campaigns (c_s_dqi_campaign_id < 500) the program aborts.

Notes:
- This code makes use of global configuration values.
- Logging and exception handling are implemented.
- The SAS “abort” steps are simulated here by calling sys.exit(1).
- Placeholder functions (m_email_fail, m_email_fail_msg, and m_email_targeting_conf) are provided.
"""
import logging
import sys
import os
import m_data_operations as mdo
#import m_email_fail
import m_email_fail_msg
import m_email_targeting_conf
import yaml


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

# -----------------------------------------------------------------------------
# Global configuration variables (these might be loaded externally)
# -----------------------------------------------------------------------------
c_s_dqi_campaign_id = shared_variables['c_s_dqi_campaign_id']          # Example campaign ID
c_s_email_subject = shared_variables['c_s_email_subject']   # Base subject used in email functions
c_s_maindir = shared_variables['c_s_maindir'] # Could include "bss_test" for MDP campaigns
c_s_tdtempx = shared_variables['c_s_tdtempx']
dqi_storage_project = shared_variables['dqi_storage_project']  # Project for DQI storage 
# The following globals support the messaging logic below.
dqi_message_type = "FAILURE MESSAGE"  # Could be "FAILURE MESSAGE", "WARNING MESSAGE", or "INFORMATIVE MESSAGE"
dqi_mco_message = "Critical error encountered"  # Error message text for triage
# For simulation purposes (for later calls)
ctt_application = None

# Variables that will be set during processing
_msg_fail = ""      # Accumulates failure messages when in triage mode
_triage_error = 0   # Flag set to 1 if a triage error occurred

def m_abend_handler(abend_report, abend_message="N", abend_message_id=0):
    logging.info("=========================================================")
    logging.info("Start :: m_abend_handler...")
    logging.info("=========================================================")
    """
    Parameters:
       abend_report (str): The error report message.
       abend_message (str): The error message (default "N").
       abend_message_id (int): The error message identifier (default 0).

    Returns: None.
    Aborts processing if a critical error is encountered.
    """
    global _msg_fail, _triage_error, ctt_application, c_s_maindir, c_s_dqi_campaign_id
    global dqi_message_type, dqi_mco_message  # assumed to be set externally
    
    try:
        logging.info("Starting m_abend_handler with abend_report='%s', abend_message='%s', abend_message_id=%s",
                     abend_report, abend_message, abend_message_id)
    
        # -------------------------------------------------------
        # STEP 1: Process based on abend_message_id.
        # -------------------------------------------------------
        if abend_message_id == 0:
            # Non-converted messages branch.
            if c_s_dqi_campaign_id < 500:
                # non-triage campaigns – call m_email_fail.
                m_email_fail_msg(subject=f"FAILED!! {c_s_email_subject}",
                             report=abend_report,
                             message=abend_message)
            else:
                # triage campaigns – concatenate failure message.
                if _msg_fail:
                    _msg_fail = f"{_msg_fail}|{abend_message}"
                else:
                    _msg_fail = abend_message
                logging.info(f"_msg_fail set to: {_msg_fail}")
                _triage_error = 1
        else:
            # Converted messages branch.
            # In SAS, here a dataset is read and transposed. For this conversion,
            # we assume that required values (such as dqi_message_type and dqi_mco_message)
            # are already available as global variables.
            logging.info("Creating dqi_process_message dataframe...")
            dqi_process_message_str = f"""
            SELECT * 
            FROM `{dqi_storage_project}.{c_s_tdtempx}.dqi_process_message`
            WHERE dqi_message_id = {abend_message_id}"""
            logging.info(f"Executing SQL: {dqi_process_message_str}")
            dqi_process_message_df = mdo.fetch_bigquery_dataframe(dqi_process_message_str,"dqi_process_message")
            dqi_process_message2_df = dqi_process_message_df.transpose()
            macros = {}
            for _,row in dqi_process_message2_df.iterrows():
                macros[row["_name_"]] = row["col1"].strip()
            
            logging.info(f"Macros Created: {macros}")
            for key, values in macros.items():
                print(f"{key}: {values}")
        
            if c_s_dqi_campaign_id >= 500:
                # For triage campaigns.
                if dqi_message_type in ["INFORMATIVE MESSAGE", "WARNING MESSAGE"]:
                    # Do nothing extra.
                    pass
                else:
                    if _msg_fail:
                        _msg_fail = f"{_msg_fail}|{dqi_mco_message}"
                    else:
                        _msg_fail = dqi_mco_message
                    logging.info(f"_msg_fail set to: {_msg_fail}")
                    _triage_error = 1
            else:
                # For non-triage campaigns, call m_email_fail_msg based on dqi_message_type.
                if dqi_message_type == "FAILURE MESSAGE":
                    m_email_fail_msg(subject=f"FAILED!! {c_s_email_subject}",
                                     report=abend_report,
                                     message=abend_message,
                                     message_id=abend_message_id)
                elif dqi_message_type == "WARNING MESSAGE":
                    m_email_fail_msg(subject=f"WARNING!! {c_s_email_subject}",
                                     report=abend_report,
                                     message=abend_message,
                                     message_id=abend_message_id)
                elif dqi_message_type == "INFORMATIVE MESSAGE":
                    m_email_fail_msg(subject=f"INFORMATIVE!! {c_s_email_subject}",
                                     report=abend_report,
                                     message=abend_message,
                                     message_id=abend_message_id)
    
        # -------------------------------------------------------
        # STEP 2: Handle non-triage logic for campaigns with c_s_dqi_campaign_id < 500.
        # -------------------------------------------------------
        # check point converted, validation done::: makkena::: 05/06/2025
        if c_s_dqi_campaign_id < 500:
            if abend_message_id == 0:
                logging.error("ERROR: ABORTING PROGRAM DUE TO PROCESSING ERROR")
                if "bss_test" in c_s_maindir:
                    ctt_application = "MDP"
                else:
                    dqi_messages = "Y"
                    # Simulate a query to obtain dqi_messages from the campaigns table.
                    dqi_campaigns_query = f"""
                                SELECT dqi_messages
                                FROM `{dqi_storage_project}.{c_s_tdtempx}.dqi_campaigns`
                                WHERE dqi_campaign_id = {c_s_dqi_campaign_id}
                            """
                # Fetch the data from Teradata using mdo.fetch_bigquery_dataframe
                    dqi_messages_df = mdo.fetch_bigquery_dataframe(dqi_campaigns_query,"dqi_messages")
                    if not dqi_messages_df.empty:
                        dqi_messages = ''.join(dqi_messages_df['dqi_messages'].astype(str))
                    else:
                        dqi_messages = ""    
                    logging.info(f"NOTE: dqi_messages = {dqi_messages}")
                    if dqi_messages == "Y":
                        ctt_application = "DQI"
                        m_email_targeting_conf()
                # Simulate SAS data _null_ abort; in Python use sys.exit.
                sys.exit(1)
            else:
                if dqi_message_type == "FAILURE MESSAGE":
                    logging.error("ERROR: ABORTING PROGRAM DUE TO PROCESSING ERROR")
                    if "bss_test" in c_s_maindir:
                        ctt_application = "MDP"
                    else:
                        dqi_messages = "Y"
                        dqi_campaigns_query = f"""
                                SELECT dqi_messages
                                FROM `{dqi_storage_project}.{c_s_tdtempx}.dqi_campaigns`
                                WHERE dqi_campaign_id = {c_s_dqi_campaign_id}
                                """
                        dqi_messages_df = mdo.fetch_bigquery_dataframe(dqi_campaigns_query,"dqi_messages")
                        if not dqi_messages_df.empty:
                            dqi_messages = ''.join(dqi_messages_df['dqi_messages'].astype(str))
                        else:
                            dqi_messages = ""    
                        logging.info(f"NOTE: dqi_messages = {dqi_messages}")
                        if dqi_messages == "Y":
                            ctt_application = "DQI"
                            m_email_targeting_conf()
                    sys.exit(1)
                elif dqi_message_type in ["WARNING MESSAGE", "INFORMATIVE MESSAGE"]:
                    # No abort in these cases.
                    pass
        logging.info("=========================================================")
        logging.info("m_abend_handler completed successfully.")
        logging.info("=========================================================")
    except Exception as e:
        logging.error("Error in m_abend_handler: %s", e, exc_info=True)
        sys.exit(1)