import pandas as pd

# Import dependences to send email with MIME
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging

# Import dependence to read config
from configparser import ConfigParser


# Initialize variables
parser = ConfigParser()
parser.read('pull_covid_csv.ini')
log_file = parser.get('files', 'log_file')
save_to_filepath = parser.get('files', 'save_to_filepath')
email_to_subject = parser.get('emails', 'email_to_subject')
email_to_error_subject = parser.get('emails', 'email_to_error_subject')
email_address = parser.get('credentials', 'email_address')
email_recipient = parser.get('credentials', 'email_recipient')
email_password = parser.get('credentials', 'email_password')

# Configure logging information
logging.basicConfig(level=logging.INFO, filename=log_file, filemode='a', 
                    format='%(asctime)s -%(levelname)s- %(message)s', datefmt='%d-%b-%y %H:%M:%S')


# Define send_email function
def send_email(subject, body, attachment=True, filename=log_file):
    '''Send email with or without attachment.'''

    logging.info('Trying to send email')
    
    try:
        # Construct email object
        msg = MIMEMultipart()
        msg['From'] = email_address
        msg['To'] = email_address
        msg['Subject'] = subject
        
        # Send plain text (can send html etc)
        msg.attach(MIMEText(body,'plain'))
        
        if attachment:
            # Send email with attachments (text, image etc)
            attachment = open(filename, 'r+b')
            part = MIMEBase('application', 'octat-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= " + filename)
            msg.attach(part)
            attachment.close()
        
        # Convert msg object into string
        text = msg.as_string()
        
        # Connect to email and login
        server = smtplib.SMTP('smtp-mail.outlook.com:587')
        server.starttls()
        server.ehlo()
        server.login(email_address, email_password)
        
        # Send email
        server.sendmail(email_address, email_recipient, text)
        server.quit()
        
        logging.info(f'Email notification is sent to {email_recipient}')
        
    except Exception as e:
        
        logging.error('Exception occurred', exc_info=True)
        raise


# Define pull_data function
def pull_data():
    '''Pull data from covidtracking.com'''
    
    logging.info('Trying to download the CSVs')
    
    try: 
        # Initialize variables
        historical_url = "https://api.covidtracking.com/v1/states/daily.csv"
        current_url = "https://api.covidtracking.com/v1/states/current.csv"
        
        # Read from API
        covid_historical_df = pd.read_csv(historical_url)
        covid_current_df = pd.read_csv(current_url)
        
        # Write to CSV
        covid_historical_df.to_csv(f'{save_to_filepath}\daily.csv', index=False)
        covid_current_df.to_csv(f'{save_to_filepath}\current.csv', index=False)
        
        logging.info('Covid CSVs are downloaded')
        
    except Exception as e:

        logging.error('Exception occurred', exc_info=True)
        
        # Send error email
        send_email(email_to_error_subject, 'Unable to download CSVs')
        
        raise
    

def main():
    '''Run functions'''
    
    try:
        
        # Pull data to folder
        pull_data()
        logging.info('Pocess completed')
        
        # Send completion email
        send_email(email_to_subject, 'COVID CSVs are downloaded')
        
    except Exception as e:
        logging.error('Error occurred; Ended program')
    
    
if __name__ == "__main__":
    main()
