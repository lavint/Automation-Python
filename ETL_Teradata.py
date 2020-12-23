# Import dependences
import pandas as pd
import time
from datetime import datetime
import logging
import sys

# Import dependences to interact with database
from sqlalchemy import create_engine
import sqlalchemy_teradata
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import exc

# Import dependences to send email with MIME
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Import dependence to read config
from configparser import ConfigParser


# Initiate variables from config
parser = ConfigParser()
parser.read('config.ini')
log_file = parser.get('files', 'log_file')
query_file = parser.get('files', 'query_file')
user_dsn = parser.get('database', 'user_dsn')
td_driver = parser.get('database', 'td_driver')
port = parser.get('database', 'port')
temp_db = parser.get('database', 'temp_db')
prod_db = parser.get('database', 'prod_db')
service_account = parser.get('credentials', 'service_account')
service_pw = parser.get('credentials', 'service_pw')
ldap_account = parser.get('credentials', 'ldap_account')
ldap_pw = parser.get('credentials', 'ldap_pw')
email_to_subject = parser.get('emails', 'email_to_subject')
email_to_error_subject = parser.get('emails', 'email_to_error_subject')
email_address = parser.get('credentials', 'email_address')
email_password = parser.get('credentials', 'email_password')
email_recipient = parser.get('credentials', 'email_recipient')


# Configure logging information
logging.basicConfig(level=logging.INFO, filename=log_file, filemode='a', 
                    format='%(asctime)s -%(levelname)s- %(message)s', datefmt='%d-%b-%y %H:%M:%S')


# Create a function to send email notification with log attached
def send_email(subject, body, attachment=True, filename=log_file):
    '''Send email with or without attachment.'''
    # Log info
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
        logging.info(f'Email notification is sent to {email_recipient}')
        
    except Exception as e:
        
        # Log error
        logging.error('Exception occurred', exc_info=True)
        raise


# Create a function to connect to SQL engine
def conn_engine(account, password, ldap_login=False, dsn=user_dsn, driver=td_driver, port=port):
    '''Create engine and establish database connection, return engine.'''
    #Log info
    logging.info('Trying to create engine')
    
    try:
        
        # Check login mechanism and use proper connection string to connect
        if ldap_login == False:
            engine = create_engine(f'teradata://{account}:{password}@{dsn}:{port}/?driver={driver}')
        else:
            engine = create_engine(f'teradata://{account}:{password}@{dsn}:{port}/?authentication=LDAP&driver={driver}')
        db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
        db_session.execute('SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;')  # To avoid locking tables when doing select on tables
        db_session.commit()
        Base = declarative_base(cls=DeferredReflection)
        Base.query = db_session.query_property()
        
        # Log info
        logging.info('Successfully Created engine')
        return engine
    
    except Exception as e:
        
        # Get exception information
        exc_type, exc_obj, exc_tb = sys.exc_info()
        
        # Log error
        logging.error(f'Error Occurred when creating engine:{exc_type}: {exc_obj}')
        
        # Rethrow exception
        raise

        
def process_sql(query, engine, max_retry=50):
    '''Read data from database to dataframe. If failed, read again until max retry reached.'''
    # Dispose engine to ensure no database connections are carried over
    engine.dispose()
    
    # Initialize variables
    is_read = 0
    retry = 0
    
    # Log info
    logging.info('Trying to read data')
    
    # Run query until result is returned to dataframe or max retry is reached
    while (is_read == 0) and (retry <= max_retry):
        try:
            retry += 1   
            # Run select query
            df = pd.read_sql(query,engine)
            is_read = 1
            
            # Log info
            logging.info('Successfully read data')
            
            return df
        
        except Exception as e:
            
            # Get exception information
            exc_type, exc_obj, exc_tb = sys.exc_info() 
            
            # Remove SQL statement returned in DatabaseError
            if exc_type == exc.DatabaseError:
                err = str(exc_obj).split(')')[2]
            else:
                err = exc_obj
            
            # Log error
            logging.error(f'''Error occurred when executing query 
                             Max attempt: {max_retry + 1} ; Current attempt #{retry} 
                            {exc_type}: {err}''')
            
            # Wait 60s before rerunning the query
            time.sleep(60)
            
            # If max retry is reached, send error email and rethrow exception
            if retry-1 == max_retry:
                send_email(email_to_error_subject, f'Unable to query data; Max Attempt #{max_retry + 1} reached')
                raise
                

# Create a function to append new rows to prod table
def append_new_rows_to_prod(df, temp, prod, engine):
    '''Delete then add data to temp table, insert only new data to prod table, return rows inserted.'''
    # Dispose engine to ensure no database connections are carried over
    engine.dispose()
    
    # Log info
    logging.info('Trying to append new rows to PROD')
    
    try:
        
        # Delete from temp table
        engine.execute(f'''DELETE FROM {temp}''')
        
        # Get database name and table name
        db = temp.split('.')[0]
        temp_table= temp.split('.')[1]
        
        # Write dataframe to temp table
        df.to_sql(name=temp_table, con=engine, schema=db, if_exists='append', index=False)

        # Insert only new records to prod table
        r = engine.execute(f'''INSERT INTO {prod}
                                SELECT * FROM {temp}
                                EXCEPT
                                SELECT * FROM {prod}''')
        
        # Log info
        logging.info('Successfully appended new rows to prod')
        
        # Return number of rows inserted to prod
        return r.rowcount
    
    except Exception as e:
        
        # Get exception information
        exc_type, exc_obj, exc_tb = sys.exc_info()
        
        # Remove SQL statement returned in DatabaseError
        if exc_type == exc.DatabaseError:
            err = str(exc_obj).split(')')[2]
        else:
            err = exc_obj
            
        # Log error
        logging.error(f'''Error occurred when appending new rows to prod 
                         {exc_type}: {err}''')
        
        # Send email
        send_email(email_to_error_subject, 'Unable to append new rows to prod')
        
        # Rethrow exception
        raise

        
def main():
    '''Run functions'''
    try:
        
        # create engine and connection
        engine = conn_engine(service_account, service_pw)
        
        # Read sql query in text file
        with open(query_file,'r') as q:
            query = q.read()
        
        # Read data from database to dataframe
        df = process_sql(query, engine)
        
        # Insert only new records and get number of rows inserted
        insert_row_count = append_new_rows_to_prod(df, temp_db, prod_db, engine)
        
        # Get today's date
        today = datetime.today().strftime('%Y-%m-%d')
        
        # Send email
        send_email(email_to_subject, f'{insert_row_count} rows are inserted to {prod_db} on {today}')
    
    except Exception as e:
        
        # Log error
        logging.error(f'''Error occurred; Ended program''')
        
        # Send email
        send_email(email_to_error_subject, f'Check log')
    
    finally:
        
        # Dispose engine and close connections
        engine.dispose()
    
    

if __name__ == "__main__":
    main()
