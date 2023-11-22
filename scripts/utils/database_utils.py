# utils/database_utils.py

from sqlalchemy import create_engine, text, select
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
import pandas as pd
import csv
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




def extract_and_write_data(session, csv_file_path, table_name, unique_identifier, chunk_size=1000):    
    try:
    
        last_value = None

        while True:
            query = select("*").select_from(text(str(table_name))).limit(chunk_size)

            if last_value:
                query = query.where(text(f"{unique_identifier} > :last_value")).params(last_value=last_value)

            data = session.execute(query)
            rows = data.fetchall()

            if not rows:
                break

            # Write data to CSV file
            with open(csv_file_path, 'a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write header only for the first chunk
                if last_value is None:
                    csv_writer.writerow(data.keys())

                # Write data
                csv_writer.writerows(rows)

            last_value = rows[-1][0]  # Assuming the first column is an identifier, adjust if needed
            
    except Exception as e:
        logging.error(f"Error extracting and writing data: {str(e)}")



    



   

