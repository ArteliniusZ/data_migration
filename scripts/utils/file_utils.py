# utils/file_utils.py

from configparser import ConfigParser
import logging

logging.basicConfig(level=logging.INFO) 

def load_credentials(file_path='C:/Users/azgardan/Data_Migration_project/data_migration/config/config.ini'):
    config = ConfigParser()
    config.read(file_path)

    return {
        'oracle_username': config.get('database', 'oracle_username'),
        'oracle_password': config.get('database', 'oracle_password'),
        'oracle_host': config.get('database', 'oracle_host'),
        'oracle_port': config.get('database', 'oracle_port'),

        'postgres_username': config.get('database', 'postgres_username'),
        'postgres_password': config.get('database', 'postgres_password'),
        'postgres_host': config.get('database', 'postgres_host'),
        'postgres_port': config.get('database', 'postgres_port'),

        'ssh_host': config.get('ssh', 'ssh_host'),
        'ssh_port': config.get('ssh', 'ssh_port'),
        'ssh_username': config.get('ssh', 'ssh_username'),
        'ssh_private_key': config.get('ssh', 'ssh_private_key'),
        'ssh_passkey': config.get('ssh', 'ssh_passkey'),
        'local_port': config.get('ssh', 'local_port')
    }


def save_dataframe_to_csv(dataframe, file_path, index=False):
    try:
        dataframe.to_csv(file_path, index=index)
        print(f"DataFrame saved to {file_path}")
    except Exception as e:
        logging.error(f"Error saving DataFrame to CSV: {str(e)}")

