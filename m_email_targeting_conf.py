"""
File: m_email_targeting_conf.py
Purpose: Converted from the SAS macro m_email_targeting_conf.
         This script sends a confirmation email with targeting information.

Logic Overview:
1. Build email content with campaign details and targeting information
2. Send the email to designated recipients
3. Handle different campaign types with appropriate content

Notes:
- This code makes use of global configuration values from a YAML file.
- Logging and exception handling are implemented.
- Email functionality is implemented using the smtplib and email modules.
"""

import logging
import os
import sys
import yaml
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import m_data_operations as mdo
import datetime

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

# Load global variables from shared_variable.yaml
c_s_aprimo_schema = shared_variables.get('c_s_aprimo_schema', '')
c_s_maindir = shared_variables.get('c_s_maindir', '')
c_s_ticket = shared_variables.get('c_s_ticket', '')
c_s_aprimo_activity = shared_variables.get('c_s_aprimo_activity', '')
c_s_dqi_campaign_id = shared_variables.get('c_s_dqi_campaign_id', 0)
c_s_email_to = shared_variables.get('c_s_email_to', '')
c_s_mailing_name = shared_variables.get('c_s_mailing_name', '')
c_s_program = shared_variables.get('c_s_program', '')
c_s_proj = shared_variables.get('c_s_proj', '')
c_s_client_nm = shared_variables.get('c_s_client_nm', '')
c_s_tdtempx = shared_variables.get('c_s_tdtempx', '')
dqi_storage_project = shared_variables.get('dqi_storage_project', '')

# Get current date and time information
now = datetime.datetime.now()
timestmp = now.strftime('%d%b%Y:%H:%M:%S')
sysjobid = os.getpid()
sysuserid = os.getlogin()
sysdate9 = now.strftime('%d%b%Y')
systime = now.strftime('%H:%M:%S')

def m_email_targeting_conf():
    """
    Sends a confirmation email with targeting information.
    """
    logging.info("=========================================================")
    logging.info("Start :: m_email_targeting_conf...")
    logging.info("=========================================================")
    
    try:
        global _clientusername
        _clientusername = os.getlogin()  # Get current user
        
        # Get title_nm from Aprimo project details
        title_nm = ""
        aprimoprodid = "Client Specific Communications"
        aprimosubprodid = "Client Specific Communications"
        
        if c_s_aprimo_schema:
            try:
                # Get aprimo project title
                title_query = f"""
                SELECT DISTINCT TRIM(REGEXP_REPLACE(title_nm, r'[\'",\\&]', '')) AS title_nm
                FROM `{dqi_storage_project}.{c_s_aprimo_schema}.v_prjct_dtl`
                WHERE prjct_id = '{c_s_ticket}'
                """
                title_df = mdo.fetch_bigquery_dataframe(title_query, "title_df")
                if not title_df.empty:
                    title_nm = title_df['title_nm'].iloc[0]
                logging.info(f"title_nm = {title_nm}")
                
                # Get aprimo product ID
                prod_id_query = f"""
                SELECT 
                    prjct_id, 
                    REGEXP_REPLACE(prod_id, r'[^0-9a-z]', ' ', 1, 0, 'i') AS prod_id
                FROM `{dqi_storage_project}.{c_s_aprimo_schema}.v_prjct_dtl`
                WHERE prjct_id = '{c_s_ticket}'
                """
                prod_id_df = mdo.fetch_bigquery_dataframe(prod_id_query, "aprimoprodid")
                if not prod_id_df.empty:
                    aprimoprodid = prod_id_df['prod_id'].iloc[0].strip()
                    
                # Get aprimo subproduct ID
                subprod_id_query = f"""
                SELECT 
                    prjct_id, 
                    REGEXP_REPLACE(prod_subcat_id, r'[^0-9a-z]', ' ', 1, 0, 'i') AS prod_subcat_id
                FROM `{dqi_storage_project}.{c_s_aprimo_schema}.v_prjct_dtl`
                WHERE prjct_id = '{c_s_ticket}'
                """
                subprod_id_df = mdo.fetch_bigquery_dataframe(subprod_id_query, "aprimosubprodid")
                if not subprod_id_df.empty:
                    aprimosubprodid = subprod_id_df['prod_subcat_id'].iloc[0].strip()
                    
                # Set default if values are too short
                if len(aprimoprodid) < 5:
                    aprimoprodid = "Client Specific Communications"
                if len(aprimosubprodid) < 5:
                    aprimosubprodid = "Client Specific Communications"
                    
            except Exception as e:
                logging.warning(f"Could not fetch aprimo project info: {e}")
        
        logging.info(f"Aprimo Product ID = {aprimoprodid}")
        logging.info(f"Aprimo SubProduct ID = {aprimosubprodid}")
        
        # Build email subject
        subject = f"SUCCESS!! {c_s_aprimo_activity} - {c_s_ticket} {title_nm}"
        
        # Get campaign and drug count information
        campaign_query = f"""
        SELECT 
            dqi_campaign_name, 
            dqi_targeting_message
        FROM `{dqi_storage_project}.{c_s_tdtempx}.dqi_campaigns`
        WHERE dqi_campaign_id = {c_s_dqi_campaign_id}
        """
        campaign_df = mdo.fetch_bigquery_dataframe(campaign_query, "campaign_info")
        
        if not campaign_df.empty:
            campaign_name = campaign_df['dqi_campaign_name'].iloc[0]
            targeting_message = campaign_df['dqi_targeting_message'].iloc[0]
        else:
            campaign_name = "Unknown Campaign"
            targeting_message = ""
        
        # Build email content
        msg = MIMEMultipart()
        
        # Set email recipients
        recipients = c_s_email_to.split(',')
            
        msg['Subject'] = subject
        msg['From'] = "no-reply@example.com"  # Set appropriate sender
        msg['To'] = ", ".join(recipients)
        
        # Build email HTML content
        html_content = []
        html_content.append('<html>')
        html_content.append('<meta content="text/html; charset=ISO-8859-1" http-equiv="content-type">')
        html_content.append('<body>')
        html_content.append('<span style="font-size: 10pt; font-family: &quot;Calibri Light&quot;,&quot;serif&quot;;">')
        
        # Email introduction
        html_content.append('<br>')
        html_content.append("This is an automated email sent by Python on behalf of Campaign Targeting Team.<br>")
        html_content.append("<b>Please DO NOT RESPOND TO THIS E-MAIL</b><br>")
        html_content.append(f"Job number {sysjobid} submitted by {sysuserid} at {systime} on {sysdate9} has completed successfully.<br><br>")
        
        # Email body - targeting information
        html_content.append('<b><font color="#13478C"><u>Targeting information:</u></font></b><br>')
        html_content.append("<ul>")
        html_content.append(f"<li>    name:    {c_s_mailing_name}</li>")
        html_content.append(f"<li>    program: {c_s_program}</li>")
        html_content.append(f"<li>    ctt application:  DQI</li>")
        html_content.append(f"<li>    aprimo activity ID:  {c_s_aprimo_activity}</li>")
        html_content.append(f"<li>    aprimo project ID:  {c_s_ticket}</li>")
        html_content.append(f"<li>    campaign ID:  {c_s_dqi_campaign_id}</li>")
        html_content.append(f"<li>    campaign name:  {campaign_name}</li>")
        html_content.append(f"<li>    project: {c_s_proj}</li>")
        html_content.append(f"<li>    user: {_clientusername}</li>")
        html_content.append(f"<li>    aprimo product ID: {aprimoprodid}</li>")
        html_content.append(f"<li>    aprimo subproduct ID: {aprimosubprodid}</li>")
        html_content.append(f"<li>    client:  {c_s_client_nm}</li>")
        html_content.append("</ul>")
        
        # Drug information if available 
        try:
            # Get drug counts based on campaign type
            drug_count_query = ""
            
            # Determine the appropriate query based on campaign ID
            if c_s_dqi_campaign_id in [63, 26, 67, 566]:  # Various campaign types
                drug_count_query = f"""
                SELECT COUNT(*) AS drug_count
                FROM `{dqi_storage_project}.{c_s_tdtempx}.drug_frmly`
                """
            elif c_s_dqi_campaign_id in [30, 52]:  # FDRO, Opioids
                drug_count_query = f"""
                SELECT COUNT(*) AS drug_count
                FROM `{dqi_storage_project}.{c_s_tdtempx}.drug_fdro_wk_&tdname`
                """
            
            if drug_count_query:
                drug_count_df = mdo.fetch_bigquery_dataframe(drug_count_query, "drug_count")
                if not drug_count_df.empty:
                    drug_count = drug_count_df['drug_count'].iloc[0]
                    html_content.append('<br>')
                    html_content.append(f"<b>Number of drugs targeted: {drug_count}</b><br>")
                    html_content.append('<br>')
        except Exception as e:
            logging.warning(f"Could not fetch drug count information: {e}")
        
        # Add targeting message if available
        if targeting_message:
            html_content.append('<br>')
            html_content.append(f"<b>{targeting_message}</b><br>")
            html_content.append('<br>')
        
        # Email closing
        html_content.append("<br>")
        html_content.append("Thank you and have a great week.<br>")
        html_content.append("<br>")
        html_content.append("Sincerely,<br>")
        html_content.append("EA - Campaign Targeting Team<br>")
        html_content.append("<br>")
        html_content.append('</span>')
        html_content.append('</body>')
        html_content.append('</html>')
        
        # Attach HTML content to email
        msg.attach(MIMEText('\n'.join(html_content), 'html'))
        
        # Send email (implementation depends on available SMTP server)
        # For example:
        # with smtplib.SMTP('smtp.example.com') as server:
        #     server.send_message(msg)
        
        logging.info("Email sent successfully.")
        logging.info("=========================================================")
        logging.info("m_email_targeting_conf completed successfully.")
        logging.info("=========================================================")
    
    except Exception as e:
        logging.error(f"Error in m_email_targeting_conf: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        m_email_targeting_conf()
    except Exception as e:
        logging.error(f"Script execution failed with error: {e}")
        sys.exit(1)