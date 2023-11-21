# utils/database_utils.py

from sqlalchemy import create_engine
from sqlalchemy import text
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO) 

def get_oracle_connection(credentials, ssh_tunnel):
    try:
        local_port = str(ssh_tunnel.local_bind_port)
        connection_str = f"oracle+cx_oracle://{credentials['oracle_username']}:{credentials['oracle_password']}@127.0.0.1:{str(local_port)}/?service_name=orclpdb"
        engine = create_engine(connection_str)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session
    except Exception as e:
        logging.error(f"Error establishing Oracle connection: {str(e)}")
    

def get_postgres_connection(credentials, ssh_tunnel):
    try:
        local_port = str(ssh_tunnel.local_bind_port)
        connection_str = f"postgresql://{credentials['postgres_username']}:{credentials['postgres_password']}@127.0.0.1:{local_port}/epic4_db"
        engine = create_engine(connection_str)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session
    except Exception as e:
        logging.error(f"Error establishing PostgreSQL connection: {str(e)}")

def establish_oracle_ssh_tunnel(credentials):
    try:
        ssh_tunnel = SSHTunnelForwarder(
            (credentials['ssh_host'], int(credentials['ssh_port'])),
            ssh_username=credentials['ssh_username'],
            ssh_pkey=credentials['ssh_private_key'],  # Path to the private key file
            ssh_private_key_password=credentials['ssh_passkey'],
            remote_bind_address=(credentials['oracle_host'], 1521)  # Oracle default port
        )
        ssh_tunnel.start()
        return ssh_tunnel
    except Exception as e:
        logging.error(f"Error establishing Oracle SSH tunnel: {str(e)}")
    

def establish_postgres_ssh_tunnel(credentials):
    try:
        ssh_tunnel = SSHTunnelForwarder(
            (credentials['ssh_host'], int(credentials['ssh_port'])),
            ssh_username=credentials['ssh_username'],
            ssh_pkey=credentials['ssh_private_key'],  # Path to the private key file
            ssh_private_key_password=credentials['ssh_passkey'],
            remote_bind_address=(credentials['postgres_host'], 5432)  # PostgreSQL default port
        )
        ssh_tunnel.start()
        return ssh_tunnel
    except Exception as e:
        logging.error(f"Error establishing PostgreSQL SSH tunnel: {str(e)}")

def query_to_dataframe(session, sql_script_path, parameters=None):
    
    try:
        # Load the SQL script content
        with open(sql_script_path, 'r') as file:
            sql_query = file.read()

        # Execute the SQL query with optional parameters
        if parameters:
            result = session.execute(text(sql_query), parameters)
        else:
            result = session.execute(text(sql_query))

        # Convert the result to a pandas DataFrame
        result_df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return result_df
    except Exception as e:
        logging.error(f"Error creating a dataframe: {str(e)}")
    
