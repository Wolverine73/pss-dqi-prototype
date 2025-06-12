"""
File: m_validation_drug_quality.py
Purpose: Converted from the SAS macro m_validation_drug_quality.
         This script validates drug quality by checking various attributes of drug data.
Logic Overview:
    1. Determine the parent macro and intake file type
    2. Validate drug records against various criteria based on campaign type
    3. Check for invalid values, missing required fields, and format issues
    4. Generate reports for any validation issues found
Notes:
    - This code makes use of global configuration values from yaml file.
    - Logging and exception handling are implemented.
"""

import yaml
import logging
import os
import pandas as pd
import m_data_operations as mdo
import m_abend_handler

# Load from shared_variable.yaml
with open('shared_variable.yaml', 'r') as f:
    shared_variables = yaml.safe_load(f)

# Configure logging
macro_test_flag = shared_variables['macro_test_flag']
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

# Get global variables
tdname = shared_variables['tdname']
c_s_tdtempx = shared_variables['c_s_tdtempx']
c_s_schema = shared_variables['c_s_schema']
table_out = shared_variables['table_out']
c_s_dqi_campaign_id = shared_variables['c_s_dqi_campaign_id']
c_s_filedir = shared_variables['c_s_filedir']
dqi_storage_project = shared_variables['dqi_storage_project']
# Define the drug family table name
drug_frmly = f"{dqi_storage_project}.{c_s_tdtempx}.{drug_frmly}"


def m_validation_drug_quality():
    """
    Validates drug quality by checking various attributes of drug data.
    """
    logging.info("=========================================================")
    logging.info("Start :: drug quality validation process...")
    logging.info("=========================================================")
    
    try:
        # Determine parent macro and intake file
        import inspect
        frame = inspect.currentframe().f_back
        _parent = frame.f_code.co_name if frame else ""
        
        _intake_file = ""
        if c_s_dqi_campaign_id == 63:
            if _parent.upper() == 'M_INTAKE_FORM_DRUG_FDRO_ANA':
                _intake_file = '8'
            else:
                _intake_file = '2'
        
        # Fetch drug_in data
        drug_in = f"{dqi_storage_project}.{c_s_tdtempx}.drug_intake_{tdname}"
        drug_in_df = mdo.fetch_bigquery_dataframe(f"SELECT * FROM `drug_in`","drug_in")
        
        # Create validation dataframe
        validation_columns = ['validation_msg']
        if _parent.upper() == 'M_INTAKE_FORM_DRUG':
            drug_validation_df = drug_in_df.copy()
            # Drop columns starting with underscore
            drop_cols = [col for col in drug_validation_df.columns if col.startswith('_')]
            drug_validation_df = drug_validation_df.drop(columns=drop_cols)
        else:
            drug_validation_df = drug_in_df.copy()
        
        # Add validation_msg column
        drug_validation_df['validation_msg'] = ''
        
        # Apply validation rules
        for index, row in drug_validation_df.iterrows():
            validation_msgs = []
            
            # Common validations for all campaigns
            if row['rec_type'] not in ['I', 'E']:
                validation_msgs.append('Invalid rec type')
            
            if row['rec_type'] == 'I' and row['b_g'].upper() not in ['B', 'G', 'A', '']:
                validation_msgs.append('Invalid B_G indicator')
            
            # FDRO validations
            if (c_s_dqi_campaign_id in [30, 35, 556, 45] or 
                (c_s_dqi_campaign_id == 63 and _intake_file == '8') or
                (c_s_dqi_campaign_id == 79 and _parent.upper() == 'M_INTAKE_FORM_DRUG_FDRO_ANA')):
                
                if row['GRDFTHR'].upper() not in [' ', 'N', 'Y', 'M']:
                    validation_msgs.append('Invalid GRDFTHR')
                
                if row['class'].upper() not in ['OTHER', 'MSB', 'SPECIALTY', 'STRIPS/KITS']:
                    validation_msgs.append('Invalid Class Value')
                
                if row['Lbl_Name'] == " ":
                    validation_msgs.append('Label Name cannot be blank')
                
                if row['XDRUG_TEXT'] == " ":
                    validation_msgs.append('XDrug Text cannot be blank')
                
                if row['DRUGMSG'] == " ":
                    validation_msgs.append('Drug Message cannot be blank')
                
                if row['GSTP_ALTERNATIVE_TEXT'] == " ":
                    validation_msgs.append('GSTP Alternative Text cannot be blank')
                
                if row['MDXSTELLENT'] == " ":
                    validation_msgs.append('MDXSTELLENT cannot be blank')
                
                if row['MDPASTELLENT'] == " ":
                    validation_msgs.append('MDPASTELLENT cannot be blank')
                
                if row['PTXSTELLENT'] == " ":
                    validation_msgs.append('PTXSTELLENT cannot be blank')
                
                if row['PTPASTELLENT'] == " ":
                    validation_msgs.append('PTPASTELLENT cannot be blank')
                
                if row['PTHYPERSTELLENT'] > " " or row['MDHYPERSTELLENT'] > " ":
                    if row['HYPER_ALTERNATIVE_TEXT'] == " ":
                        validation_msgs.append('HYPER ALTERNATIVE TEXT cannot be blank')
                    
                    if row['HYPER_GSTP_ALTERNATIVE_TEXT'] == " ":
                        validation_msgs.append('HYPER GSTP ALTERNATIVE TEXT cannot be blank')
                
                if row['INSULIN_CALL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid Insulin calls Value')
                
                if row['rec_type'] == 'I' and row['alternative_text'] == ' ':
                    validation_msgs.append('Alternative Messaging cannot be blank')
                
                if row['add_back_messaging'] > " ":
                    if row['PTXAddbkStellent'] == " ":
                        validation_msgs.append('PTXAddbkStellent cannot be blank')
                    
                    if row['PTPAAddbkStellent'] == " ":
                        validation_msgs.append('PTPAAddbkStellent cannot be blank')
                
                if row['add_back_messaging_hyper'] > " ":
                    if row['PTHyperAddbk'] == " ":
                        validation_msgs.append('PTHyperAddbk cannot be blank')
                
                if row['add_back_merge_drug_hyper_ind'] not in [' ', 'Y', 'N']:
                    validation_msgs.append('Invalid Add Back Merge Drug Hyper indicator Value')
                
                if row['RXCHANGE_SPC'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_SPC Value')
                
                if row['RXCHANGE_MAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_MAIL Value')
                
                if row['RXCHANGE_RETAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_RETAIL Value')
            
            # ACF/ACSF validations
            if (c_s_dqi_campaign_id in [26, 54, 551] or
                (c_s_dqi_campaign_id == 63 and _intake_file == '2') or
                (c_s_dqi_campaign_id == 81 and _parent.upper() == 'M_INTAKE_FORM_DRUG_ACFBF_EX_BOB') or
                (c_s_dqi_campaign_id == 96 and _parent.upper() == 'M_INTAKE_FORM_DRUG_ACFBF_EX_BOB') or
                (c_s_dqi_campaign_id == 79 and _parent.upper() == 'M_INTAKE_FORM_DRUG_ACF')):
                
                if row['GF'] not in ['Y', 'N', ' ', 'M']:
                    validation_msgs.append('Invalid GrandFather Flag')
                
                if len(row['insulin calls']) > 12:
                    validation_msgs.append('Invalid Length of Insulin calls')
                
                if len(row['DRUG NAME']) > 100:
                    validation_msgs.append('Invalid Length of Drung Name')
                
                if len(row['PT_LTR']) > 30:
                    validation_msgs.append('Invalid Length of PT_LTR')
                
                if len(row['Add-back PT_LTR']) > 30:
                    validation_msgs.append('Invalid Length of Add_back_PT_LTR')
                
                if len(row['Insert']) > 30:
                    validation_msgs.append('Invalid Length of pt_Insert')
                
                if len(row['PT_LTR_RETAIL']) > 30:
                    validation_msgs.append('Invalid Length of PT_LTR_RETAIL')
                
                if len(row['Add-back_Retail']) > 30:
                    validation_msgs.append('Invalid Length of Add_back_Retail')
                
                if len(row['MD_LTR']) > 30:
                    validation_msgs.append('Invalid Length of MD_LTR')
                
                if len(row['VF_ACSF_PT_LTR']) > 30:
                    validation_msgs.append('Invalid Length of VF_ACSF_PT_LTR')
                
                if len(row['VF-ACSF_MD_LTR']) > 30:
                    validation_msgs.append('Invalid Length of VF_ACSF_MD_LTR')
                
                if len(row['Add-Back Merge Field']) > 300:
                    validation_msgs.append('Invalid Length of Add_Back_Merge_Field')
                
                if len(row['include or exclude']) > 1:
                    validation_msgs.append('Invalid Length of Include_or_Exclude')
                
                if row['rec_type'] == 'I' and row['alternative_text'] == ' ':
                    validation_msgs.append('Alternative Messaging cannot be blank')
                
                if row['RXCHANGE_SPC'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_SPC Value')
                
                if row['RXCHANGE_MAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_MAIL Value')
                
                if row['RXCHANGE_RETAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_RETAIL Value')
            
            # BC validations
            if (c_s_dqi_campaign_id in [566] or
                (c_s_dqi_campaign_id in [79, 81] and _parent.upper() == 'M_INTAKE_FORM_DRUG_BC_EX_BOB')):
                
                if row['PUE FLAG'] not in ['Y', 'N', 'M']:
                    validation_msgs.append('Invalid PUE Flag')
                
                if row['SPCLTY MANAGED'] not in ['Y', 'N']:
                    validation_msgs.append('Invalid Speciality Managed')
                
                if row['CALL TYPE'] not in ['E', 'S', 'N', ' ']:
                    validation_msgs.append('Invalid Call Type')
                
                if pd.isna(row['EFFECTIVE DATE']):
                    validation_msgs.append('Invalid Effective Date')
                
                if len(row['NDC']) != 11:
                    validation_msgs.append('Invalid NDC 11')
                
                if row['DRUG LABEL NAME'] == ' ':
                    validation_msgs.append('Invalid Drug Label Name')
                
                if len(row['DRUG LABEL NAME']) > 100:
                    validation_msgs.append('Invalid Length Drug Label Name')
                
                if row['DRUG BRAND NAME'] == ' ':
                    validation_msgs.append('Invalid Drug Brand Name')
                
                if len(row['DRUG BRAND NAME']) > 100:
                    validation_msgs.append('Invalid Length Of Drug Brand Name')
                
                if row['DRUG ABBR NAME'] == ' ':
                    validation_msgs.append('Invalid Drug Abbrevation Name')
                
                if len(row['DRUG ABBR NAME']) > 100:
                    validation_msgs.append('Invalid Length of Drug Abbrevation Name')
                
                if row['ALTERNATIVES'] == ' ':
                    validation_msgs.append('Invalid Alternative Text')
                
                if len(row['ALTERNATIVES']) > 300:
                    validation_msgs.append('Invalid Length of Alternative Text')
                
                if row['ADD BACK'] not in ['Y', 'N']:
                    validation_msgs.append('Invalid Add Back')
                
                if len(row['ADD BACK LANGUAGE']) > 300:
                    validation_msgs.append('Invalid Length of Add Back Language')
                
                if row['PRODUCT CODE'] not in ['SP-PDPD', 'FE-MNPA', 'FE-TS']:
                    validation_msgs.append('Invalid Product Code')
                
                if row['MEMBER LETTER TEMPLATE ID'] == ' ':
                    validation_msgs.append('Invalid Member Letter Template Id')
                
                if len(row['MEMBER LETTER TEMPLATE ID']) > 30:
                    validation_msgs.append('Invalid Length of Member Letter Template Id')
                
                if len(row['MEMBER LETTER INSERT TEMPLATE ID']) > 30:
                    validation_msgs.append('Invalid Length of Member Letter Insert Template Id')
                
                if len(row['ADD BACK MEMBER LETTER TEMPLATE']) > 30:
                    validation_msgs.append('Invalid Length of Add Back Member Letter Template')
                
                if len(row['ADD BACK MEMBER LETTER INSERT TE']) > 30:
                    validation_msgs.append('Invalid Length of Add Back Member Letter Insert Template')
                
                if len(row['MEMBER CALL TEMPLATE ID']) > 30:
                    validation_msgs.append('Invalid Length of Member Call Template Id')
                
                if row['PRESCRIBER LETTER TEMPLATE ID'] == ' ':
                    validation_msgs.append('Invalid Prescriber Letter Template Id')
                
                if len(row['PRESCRIBER LETTER TEMPLATE ID']) > 30:
                    validation_msgs.append('Invalid Length of Prescriber Letter Template Id')
                
                if row['GSTP ALTERNATIVES'] == ' ':
                    validation_msgs.append('GSTP ALTERNATIVE TEXT Cannot be blank')
                
                if (row['ADD BACK'] == 'Y' and 
                    (row['ADD BACK LANGUAGE'] == ' ' or row['ADD BACK MEMBER LETTER TEMPLATE'] == ' ')):
                    validation_msgs.append('Invalid Add Back Yes Message')
                
                if (row['ADD BACK'] == 'N' and 
                    (row['ADD BACK LANGUAGE'] > ' ' or row['ADD BACK MEMBER LETTER TEMPLATE'] > ' ')):
                    validation_msgs.append('Invalid Add Back No Message')
                
                if row['RXCHANGE_SPC'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_SPC Value')
                
                if row['RXCHANGE_MAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_MAIL Value')
                
                if row['RXCHANGE_RETAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_RETAIL Value')
            
            # Tier changes validations
            if c_s_dqi_campaign_id in [60, 553, 61, 562]:
                if row['rec_type'] == 'I' and row['ms_ss'].upper() not in ['M', 'S', 'A', '']:
                    validation_msgs.append('Invalid MS_SS indicator')
            else:
                if row['rec_type'] == 'I' and row['ms_ss'].upper() not in ['MS', 'SS', 'A', '']:
                    validation_msgs.append('Invalid MS_SS indicator')
            
            # Health Exchange validations
            if c_s_dqi_campaign_id in [20, 558]:
                if row['RXCHANGE_SPC'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_SPC Value')
                
                if row['RXCHANGE_MAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_MAIL Value')
                
                if row['RXCHANGE_RETAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_RETAIL Value')
            
            # GSTP validations
            if c_s_dqi_campaign_id in [28, 34, 557, 20, 558]:
                if row['rec_type'] == 'I' and row['alternative_text'] == ' ':
                    validation_msgs.append('Alternative Messaging cannot be blank')
            elif c_s_dqi_campaign_id in [67, 554]:
                if row['rec_type'] == 'I' and row['change_type'] in ['EX', 'ST'] and row['alternative_text'] == ' ':
                    validation_msgs.append('Alternative Messaging cannot be blank')
            
            # Common validations for all campaigns
            if row['gpi'] == '' and row['ndc11'] == '' and row['ndc9'] == '':
                validation_msgs.append('GPI and NDC are blank')
            
            # Validate only one drug identifier is populated
            if _parent.upper() == 'M_INTAKE_FORM_DRUG':
                drug_fields = [row['ndc11'], row['ndc9'], row['gpi']]
                cnt_miss_df = sum(1 for field in drug_fields if field)
                if cnt_miss_df > 1:
                    validation_msgs.append('Only 1 of NDC11, NDC9 or GPI can be populated')
            
            # GF validations
            if c_s_dqi_campaign_id in [67, 554, 66]:
                if row['gf'] > ' ' and row['gf'].upper() not in ['N', 'Y', 'M']:
                    validation_msgs.append('Invalid GF')
                
                if row['RXCHANGE_SPC'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_SPC Value')
                
                if row['RXCHANGE_MAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_MAIL Value')
                
                if row['RXCHANGE_RETAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_RETAIL Value')
            
            # DrugMsg_IB validations
            if c_s_dqi_campaign_id in [54, 81, 96, 554]:
                if shared_variables.get('c_s_bob_run_type') == 'IB':
                    for i in range(1, 13):
                        field_name = f'DrugMsg_IB{i}'
                        if field_name in row and len(row[field_name]) > 300:
                            validation_msgs.append(f'Invalid Length of {field_name}')
            
            # Client specific BF validations
            if c_s_dqi_campaign_id == 66:
                for i in range(1, 13):
                    field_name = f'DrugMsg_IB{i}'
                    if field_name in row and len(row[field_name]) > 300:
                        validation_msgs.append(f'Invalid Length of {field_name}')
            
            # NDC and GPI format validations
            if '*' in str(row['ndc9']):
                validation_msgs.append('Invalid NDC9')
            
            if '*' in str(row['ndc11']) and row['sql_join'] != 'WILDCARD':
                validation_msgs.append('Invalid NDC11')
            
            if '-' in str(row['ndc9']):
                validation_msgs.append('Invalid NDC9')
            
            if '-' in str(row['ndc11']):
                validation_msgs.append('Invalid NDC11')
            
            # Check for multiple values separated by comma
            if (',' in str(row['ndc9']) or 
                ',' in str(row['ndc11']) or 
                ',' in str(row['rec_type']) or 
                ',' in str(row['gpi']) or 
                ',' in str(row['b_g']) or 
                ',' in str(row['ms_ss']) or 
                ',' in str(row['age_min']) or 
                ',' in str(row['age_max'])):
                validation_msgs.append('Multiple values separated by comma in one cell')
            
            # NDC length validations
            if row['ndc9'] > ' ' and len(row['ndc9']) != 9:
                validation_msgs.append('Invalid NDC9 length')
            
            if row['ndc11'] > ' ' and len(row['ndc11']) != 11 and row['sql_join'] != 'WILDCARD':
                validation_msgs.append('Invalid NDC11 length')
            
            # GPI validations
            if row['gpi'] > ' ':
                ast_total = len(row['gpi'])
                if row['gpi_len'] not in [2, 4, 6, 8, 10, 12, 14]:
                    validation_msgs.append('Invalid GPI length')
                
                if len(row['gpi']) > 14:
                    validation_msgs.append('Invalid GPI length')
                
                if '*' in row['gpi'] and row['ast'] <= len(row['gpi'].replace('*', '')):
                    validation_msgs.append('Invalid GPI')
                
                if row['gpi'][0] == '*':
                    validation_msgs.append('Invalid GPI')
                
                if row['b_g'] not in ['B', 'G', 'A']:
                    validation_msgs.append('B_G must be specified with GPI')
                
                if row['ms_ss'] not in ['SS', 'MS', 'A']:
                    validation_msgs.append('MS_SS must be specified with GPI')
            
            # Additional NDC and GPI validations
            if 'E' in str(row['ndc9']) and '.' in str(row['ndc9']):
                validation_msgs.append('Invalid NDC9')
            
            if 'E' in str(row['ndc11']) and '.' in str(row['ndc11']):
                validation_msgs.append('Invalid NDC11')
            
            if 'E' in str(row['gpi']) and '.' in str(row['gpi']):
                validation_msgs.append('Invalid GPI')
            
            if 'e' in str(row['ndc9']) and '.' in str(row['ndc9']):
                validation_msgs.append('Invalid NDC9')
            
            if 'e' in str(row['ndc11']) and '.' in str(row['ndc11']):
                validation_msgs.append('Invalid NDC11')
            
            if 'e' in str(row['gpi']) and '.' in str(row['gpi']):
                validation_msgs.append('Invalid GPI')
            
            # Quantity limit validations
            if _parent.upper() == 'M_INTAKE_FORM_DRUG':
                if row['_exceedlmt'] != '000000':
                    limit_fields = ['retail_qty_limit', 'retail_qty_unit', 'retail_qty_time',
                                   'mail_qty_limit', 'mail_qty_unit', 'mail_qty_time']
                    for i, field in enumerate(limit_fields):
                        if row['_exceedlmt'][i] == '1':
                            var_name = field.replace('_', ' ')
                            validation_msgs.append(f'{var_name} exceeds 200 char')
                
                if row['_exceed_rlimit'] == 1:
                    validation_msgs.append('Combined retail quantity limit fields exceeds 200 char')
                
                if row['_exceed_mlimit'] == 1:
                    validation_msgs.append('Combined mail quantity limit fields exceeds 200 char')
            
            # Value Formulary validations
            if (c_s_dqi_campaign_id in [23, 89, 567] or 
                (c_s_dqi_campaign_id == 96 and _parent.upper() == 'M_INTAKE_FORM_DRUG_VF_EX_BOB')):
                
                if row['PUE_FLAG'] not in ['Y', 'M', 'N']:
                    validation_msgs.append('Invalid PUE FLAG')
                
                if row['SPECIALTY_MANAGED_PRODUCT'] not in ['Y', 'N']:
                    validation_msgs.append('Invalid SPECIALTY MANAGED PRODUCT')
                
                if row['CALL_TYPE'] not in ['I', 'N', 'S', ' ']:
                    validation_msgs.append('Invalid Call Type')
                
                if pd.isna(row['EFF_DATE']):
                    validation_msgs.append('Invalid EFF DATE')
                
                if len(row['DRUG LABEL NAME']) > 100:
                    validation_msgs.append('DRUG LABLE NAME exceeds 100 chars')
                
                if row['DRUG LABEL NAME'] == ' ':
                    validation_msgs.append('DRUG LABEL NAME cannot be blank')
                
                if len(row['DRUG BRAND NAME']) > 50:
                    validation_msgs.append('DRUG BRAND NAME exceeds 50 chars')
                
                if row['DRUG BRAND NAME'] == ' ':
                    validation_msgs.append('DRUG BRAND NAME cannot be blank')
                
                if len(row['DRUG ABBR NAME']) > 50:
                    validation_msgs.append('DRUG ABBR NAME exceeds 50 chars')
                
                if row['DRUG ABBR NAME'] == ' ':
                    validation_msgs.append('DRUG ABBR NAME cannot be blank')
                
                if row['CHANGE_TYPE_PBM'] not in ['PA', 'QL', 'DNT'] and row['PBM_ALTERNATIVE'] == ' ':
                    validation_msgs.append('PBM ALTERNATIVE cannot be blank')
                
                if row['CHANGE_TYPE_MDDB'] not in ['PA', 'QL', 'DNT'] and row['MDDB_ALTERNATIVE'] == ' ':
                    validation_msgs.append('MDB ALTERNATIVE cannot be blank')
                
                if row['PBM_ADD_BACK_PRODUCT'] > ' ' and row['PBM_ADD_BACK_TEMPLATE'] == ' ':
                    validation_msgs.append('PBM ADD BACK TEMPLATE must be populated for add back')
                
                if row['PBM_ADD_BACK_PRODUCT'] == ' ' and row['PBM_ADD_BACK_TEMPLATE'] > ' ':
                    validation_msgs.append('PBM ADD BACK PRODUCT must be populated for add back')
                
                if row['MDDB_ADD_BACK_PRODUCT'] > ' ' and row['MDDB_ADD_BACK_TEMPLATE'] == ' ':
                    validation_msgs.append('MDB ADD BACK TEMPLATE must be populated for add back')
                
                if row['MDDB_ADD_BACK_PRODUCT'] == ' ' and row['MDDB_ADD_BACK_TEMPLATE'] > ' ':
                    validation_msgs.append('MDB ADD BACK PRODUCT must be populated for add back')
                
                if row['PBM_MONY_CODE'] not in ['M', 'O', 'N', 'Y']:
                    validation_msgs.append('Invalid PBM MONY CODE')
                
                if row['MDDB_MONY_CODE'] not in ['M', 'O', 'N', 'Y']:
                    validation_msgs.append('Invalid MDDB MONY CODE')
                
                if row['FORMULARY_DESCRIPTION_PBM'] == ' ':
                    validation_msgs.append('1565 FORMULARY DESCRIPTION - PBM cannot be blank')
                
                if row['CHANGE_TYPE_PBM'] not in ['FE', 'FE-TS', 'LC-CO', 'LC-ED', 'LC-FERT', 'LC-HSDD', 
                                                 'LC-OB', 'PA', 'QL', 'SP', 'ST', 'DNT']:
                    validation_msgs.append('Invalid 1565 CHANGE TYPE - PBM')
                
                if row['FORMULARY_GROUP_PBM'] not in ['F', 'NF']:
                    validation_msgs.append('Invalid FORMULARY GROUP - PBM')
                
                if row['FORMULARY_DESCRIPTION_MDDB'] == ' ':
                    validation_msgs.append('1565 FORMULARY DESCRIPTION - MDDB cannot be blank')
                
                if row['CHANGE_TYPE_MDDB'] not in ['FE', 'FE-TS', 'LC-CO', 'LC-ED', 'LC-FERT', 'LC-HSDD', 
                                                  'LC-OB', 'PA', 'QL', 'SP', 'ST', 'DNT']:
                    validation_msgs.append('Invalid 1565 CHANGE TYPE - MDDB')
                
                if row['FORMULARY_GROUP_MDDB'] not in ['F', 'NF']:
                    validation_msgs.append('Invalid FORMULARY GROUP - MDDB')
                
                if ((row['CHANGE_TYPE_PBM'] == 'QL' or row['CHANGE_TYPE_MDDB'] == 'QL') and 
                    pd.isna(row['retail_qty_Limit'])):
                    validation_msgs.append('RETAIL QUANTITY LIMIT - QTY must be populated for QL')
                
                if ((row['CHANGE_TYPE_PBM'] == 'QL' or row['CHANGE_TYPE_MDDB'] == 'QL') and 
                    pd.isna(row['retail_qty_time'])):
                    validation_msgs.append('RETAIL QUANTITY LIMIT - DAYS must be populated for QL')
                
                if ((row['CHANGE_TYPE_PBM'] == 'QL' or row['CHANGE_TYPE_MDDB'] == 'QL') and 
                    row['retail_qty_unit'] == ' '):
                    validation_msgs.append('RETAIL QUANTITY LIMIT - UNITS must be populated for QL')
                
                if ((row['CHANGE_TYPE_PBM'] == 'QL' or row['CHANGE_TYPE_MDDB'] == 'QL') and 
                    pd.isna(row['mail_qty_limit'])):
                    validation_msgs.append('MAIL QUANTITY LIMIT - QTY must be populated for QL')
                
                if ((row['CHANGE_TYPE_PBM'] == 'QL' or row['CHANGE_TYPE_MDDB'] == 'QL') and 
                    pd.isna(row['mail_qty_time'])):
                    validation_msgs.append('MAIL QUANTITY LIMIT - DAYS must be populated for QL')
                
                if ((row['CHANGE_TYPE_PBM'] == 'QL' or row['CHANGE_TYPE_MDDB'] == 'QL') and 
                    row['mail_qty_unit'] == ' '):
                    validation_msgs.append('MAIL QUANTITY LIMIT - UNITS must be populated for QL')
                
                if row['CALL_TYPE'] in ['I', 'S'] and row['CALL_TEMPLATE'] in ['NA', ' ']:
                    validation_msgs.append('CALL TEMPLATE must be populated for Call Types')
                
                if row['CALL_TYPE'] in ['N', ' '] and row['CALL_TEMPLATE'] not in ['NA', ' ']:
                    validation_msgs.append('Call type invalid for populated CALL TEMPLATE')
                
                if row['MBR_LETTER_PBM'] == ' ':
                    validation_msgs.append('MBR LETTER - PBM cannot be blank')
                
                if row['MBR_LETTER_INSERT_PBM'] == ' ':
                    validation_msgs.append('MBR LETTER INSERT - PBM cannot be blank')
                
                if row['MBR_LETTER_PA_PBM'] == ' ':
                    validation_msgs.append('MBR LETTER PA - PBM cannot be blank')
                
                if row['MBR_LETTER_BE_PBM'] == ' ':
                    validation_msgs.append('MBR LETTER BE - PBM cannot be blank')
                
                if row['PRESCRIBER_LETTER_PBM'] == ' ':
                    validation_msgs.append('PRESCRIBER LETTER - PBM cannot be blank')
                
                if row['MBR_LETTER_MDDB'] == ' ':
                    validation_msgs.append('MBR LETTER - PBM cannot be blank')
                
                if row['MBR_LETTER_INSERT_MDDB'] == ' ':
                    validation_msgs.append('MBR LETTER INSERT - MDDB cannot be blank')
                
                if row['MBR_LETTER_PA_MDDB'] == ' ':
                    validation_msgs.append('MBR LETTER PA - MDDB cannot be blank')
                
                if row['MBR_LETTER_BE_MDDB'] == ' ':
                    validation_msgs.append('MBR LETTER BE - MDDB cannot be blank')
                
                if row['PRESCRIBER_LETTER_MDDB'] == ' ':
                    validation_msgs.append('PRESCRIBER LETTER - MDDB cannot be blank')
                
                if (row['MBR_LETTER_PBM'] == 'NA' and
                    row['MBR_LETTER_PA_PBM'] == 'NA' and
                    row['MBR_LETTER_BE_PBM'] == 'NA' and
                    row['MBR_LETTER_MDDB'] == 'NA' and
                    row['MBR_LETTER_PA_MDDB'] == 'NA' and
                    row['MBR_LETTER_BE_MDDB'] == 'NA'):
                    validation_msgs.append('All Member letter templates cannot be NA')
                
                if (row['PRESCRIBER_LETTER_PBM'] == 'NA' and
                    row['PRESCRIBER_LETTER_MDDB'] == 'NA'):
                    validation_msgs.append('All Prescriber letter templates cannot be NA')
                
                if row['RXCHANGE_SPC'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_SPC Value')
                
                if row['RXCHANGE_MAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_MAIL Value')
                
                if row['RXCHANGE_RETAIL'] not in ['N', 'Y']:
                    validation_msgs.append('Invalid RXCHANGE_RETAIL Value')
            
            # Opioids validations
            if c_s_dqi_campaign_id in [52, 53, 563]:
                c_s_opioid_daily_dose_bypass = shared_variables.get('c_s_opioid_daily_dose_bypass', 'N')
                if c_s_opioid_daily_dose_bypass == 'N':
                    if row['alternative_text'].strip().upper() != 'EXCLUDE':
                        if pd.isna(row['opioid_daily_dose_limit']):
                            validation_msgs.append('Invalid Daily Dose Limit')
                        
                        if row['opioid_daily_dose_text'] <= ' ':
                            validation_msgs.append('Missing Daily Dose Text')
            
            # Set validation message
            if validation_msgs:
                drug_validation_df.at[index, 'validation_msg'] = '; '.join(validation_msgs)
        
        # Filter rows with validation messages
        drug_validation_df = drug_validation_df[drug_validation_df['validation_msg'] != '']
        
        # Remove duplicate rows
        drug_validation_df = drug_validation_df.drop_duplicates()
        
        # Check if there are validation issues
        cnt_dxl = len(drug_validation_df)
        logging.info(f"Number of validation issues found: {cnt_dxl}")
        
        if cnt_dxl > 0:
            # Export validation issues to Excel
            c_s_rootdir = shared_variables['c_s_rootdir']
            c_s_program = shared_variables['c_s_program']
            c_s_proj = shared_variables['c_s_proj']
            c_s_ticket = shared_variables['c_s_ticket']
            
            # Determine sheet name
            if c_s_dqi_campaign_id == 557:
                sheet_name = f"{shared_variables.get('c_s_program_type', '')}_validate_drug_quality"
            elif c_s_dqi_campaign_id in [553, 562]:
                sheet_name = f"{shared_variables.get('c_s_frmly_run_type', '')}_validate_drug_quality"
            else:
                sheet_name = "validate_drug_quality"
            
            # Export to Excel
            file_path = f"{c_s_filedir}/validate_drug_{tdname}.xlsx"
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                drug_validation_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Log error and handle abend
            error_message = f"ERROR: abend message 21 - validate_drug_{tdname}.xlsx"
            logging.error(error_message)
            m_abend_handler.m_abend_handler(
                abend_message_id=21,
                abend_report=f"validate_drug_{tdname}.xlsx"
            )
            raise Exception(error_message)
        
        # Additional validation for specific campaign IDs
        if c_s_dqi_campaign_id in [64, 561]:
            # Fetch drug_frmly data
            drug_frmly_df = mdo.fetch_bigquery_dataframe(f"SELECT * FROM `drug_frmly`","drug_frmly")
            
            # Create validation dataframe for drug targeting indicator
            drug_validation_tgt_ind_df = drug_frmly_df[
                ~drug_frmly_df['drug_tgt_ind'].str.upper().isin(['N', 'T', 'E'])
            ]
            
            # Additional validation for CF run type
            if (shared_variables.get('c_s_run_type') == 'CF' and 
                c_s_dqi_campaign_id == 64):
                # Add rows where drug_tgt_ind is 'T' but tier_from or tier_to is empty
                additional_rows = drug_frmly_df[
                    (drug_frmly_df['drug_tgt_ind'].str.upper() == 'T') & 
                    ((drug_frmly_df['tier_from'] == '') | (drug_frmly_df['tier_to'] == ''))
                ]
                drug_validation_tgt_ind_df = pd.concat([drug_validation_tgt_ind_df, additional_rows])
            
            # Remove duplicate rows
            drug_validation_tgt_ind_df = drug_validation_tgt_ind_df.drop_duplicates()
            
            # Check if there are validation issues
            cnt_expt = len(drug_validation_tgt_ind_df)
            logging.info(f"Number of drug targeting indicator issues found: {cnt_expt}")
            
            if cnt_expt > 0:
                # Export validation issues to Excel
                file_path = f"{c_s_filedir}/validate_drug_{tdname}.xlsx"
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    drug_validation_tgt_ind_df.to_excel(writer, sheet_name="validate_drug_tgt_ind", index=False)
                
                # Log error and handle abend
                error_message = f"ERROR: abend message 74 - validate_drug_{tdname}.xlsx"
                logging.error(error_message)
                m_abend_handler.m_abend_handler(
                    abend_message_id=74,
                    abend_report=f"validate_drug_{tdname}.xlsx"
                )
                raise Exception(error_message)
        
        logging.info("Drug quality validation completed successfully.")
    
    except Exception as e:
        logging.error(f"An error occurred during drug quality validation: {e}")
        raise
    
    logging.info("=========================================================")
    logging.info("End :: drug quality validation process...")
    logging.info("=========================================================")

if __name__ == "__main__":
    try:
        m_validation_drug_quality()
        logging.info("Script execution completed successfully.")
    except Exception as e:
        logging.error(f"Script execution failed with error: {e}")
        raise
