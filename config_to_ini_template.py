from configparser import ConfigParser
# Create Config
config = ConfigParser()
config['settings'] = {
}

config['files'] = {
    'log_file': 'Runtime_info.log',
    'query_file': 'query.txt'
    'save_to_filepath': r'\\domain\network\path'
}

config['emails'] = {
    'email_to_subject': 'Email Subject',
    'email_to_error_subject': 'ERROR OCCURRED'
}

config['database'] = {
    'user_dsn': 'tdprod',
    'td_driver': 'Teradata Database ODBC Driver 17.00',
    'temp_db': 'Database.Table',
    'prod_db': 'Database.Table',
    'port': '1025'
}

config['credentials'] = {
    'email_address': '',
    'email_password': '',
    'email_recipient': '',
    'service_account': '',
    'service_pw': '',
    'ldap_account': '',
    'ldap_pw': ''

}

with open('config.ini', 'w') as f:
    config.write(f)