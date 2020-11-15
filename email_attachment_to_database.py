# Import dependence to interact with the operating system
import os

# Import dependence to log progress
import logging

# Import dependence to disable InsecureRequestWarning
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import dependence to read config
from configparser import ConfigParser

# Import dependences to read email via exchange protocol
from exchangelib import Credentials, Configuration, Account, DELEGATE
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter

# Import dependences to manipulate data
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR

# Import dependences to send email with MIME
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


# Initiate variables from config
parser = ConfigParser()
parser.read('HOS.ini')
email_address = parser.get('credentials', 'email_address')
email_password = parser.get('credentials', 'email_password')
email_recipient = parser.get('credentials', 'email_recipient')
email_from_subject = parser.get('emails', 'email_from_subject')
email_to_subject = parser.get('emails', 'email_to_subject')
download_file_name = parser.get('files', 'download_file_name')
download_location = parser.get('files', 'download_location')
log_file = parser.get('files', 'log_file')
email_server = parser.get('settings', 'email_server')
db_host = parser.get('db', 'db_host')
db_name = parser.get('db', 'db_name')
db_table_name = parser.get('db', 'db_table_name')


# Configure logging information
logging.basicConfig(level=logging.INFO, filename=log_file, filemode='a', 
                    format='%(asctime)s -%(levelname)s- %(message)s', datefmt='%d-%b-%y %H:%M:%S')


# Specify download path for attachment 
download_path = os.path.join(download_location, download_file_name)


# Download email attachment from user
def download_email_attachment():
    
    try:
        # Initiate variables to read inbox
        creds = Credentials(username=email_address,password=email_password)
        config = Configuration(server=email_server, credentials=creds)
        account = Account(primary_smtp_address=email_address
                          , config=config
                          , autodiscover=False
                          , access_type=DELEGATE)

        # Filter emails with specific subject
        filter_email = account.inbox.filter(subject__startswith=email_from_subject)
        
        # Fetch the latest email from filtered email & download the attachment to local drive
        for msg in filter_email.order_by('-datetime_received')[:1]:
            
            # Create a function attribute
            download_email_attachment.subject = msg.subject
            
            for i in range(len(msg.attachments)):
                
                # Business requirement: only 1 xlsx should be attached in the email
                if msg.attachments[i].name.endswith('.xlsx'):
                    with open(download_path, 'wb') as f:
                        f.write(msg.attachments[i].content)

        # Log info
        logging.info(f"Downloaded email attachment from '{download_email_attachment.subject}'")
        
    except Exception as e:
        
        # Log error
        logging.error("Exception occurred", exc_info=True)


        
# Import data from local drive to SQL server
def import_data():
    try:
        # Create mssql engine connection
        engine = create_engine('mssql+pyodbc://' + db_host + '/' + db_name + '?driver=SQL+Server+Native+Client+11.0', fast_executemany=True)
        
        # Read and filter data
        data = pd.read_excel(download_path,skiprows=4)
        data = data[data['Status'].isin(['In Progress', 'New', 'Pending Acknowledgement'])]

        # Import data to mssql server
        data.to_sql(db_table_name, engine, if_exists='replace', index=False, dtype={col_name: NVARCHAR(length=255) for col_name in data})
        
        # Create a function attribute
        import_data.count = len(data)
        
        # Log info
        logging.info(f"Imported {len(data)} lines of data")
        
    except Exception as e:
        
        # Log error
        logging.error("Exception occurred", exc_info=True)


        
# Send email notification with log attached
def send_email_log(subject, body, filename):
    try:
        # Construct email object
        msg = MIMEMultipart()
        msg['From'] = email_address
        msg['To'] = email_address
        msg['Subject'] = subject
        
        # Send plain text (can send html etc)
        msg.attach(MIMEText(body,'plain'))
        
        # Send email with attachments (text, image etc)
        attachment = open(filename, 'rb')
        part = MIMEBase('application', 'octat-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= " + filename)
        msg.attach(part)
        
        # Convert msg object into string
        text = msg.as_string()
        
        # Send email via SMTP server and logout when done
        server = smtplib.SMTP('smtp-mail.outlook.com:587')
        server.starttls()
        server.ehlo()
        server.login(email_address, email_password)
        server.sendmail(email_address, email_recipient, text)
        server.quit()
        
        # Log info
        logging.info(f"Email notification is sent to {email_recipient}")
        
    except Exception as e:
        
        # Log error
        logging.error("Exception occurred", exc_info=True)

        
# Call functions        
download_email_attachment()        
import_data()
msg = f"{import_data.count} lines of data are imported to table '{db_table_name}' from email '{download_email_attachment.subject}'"
send_email_log(email_to_subject, msg, log_file)
