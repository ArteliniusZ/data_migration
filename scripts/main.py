import sys
sys.path.append("C:/Users/azgardan/Data_Migration_project/data_migration/scripts/datagen")
sys.path.append("C:/Users/azgardan/Data_Migration_project/data_migration/scripts/utils")
from oracle_data import generate_fake_data, delete_fake_data, update_tables
#from postgres_data import generate_fake_data, delete_fake_data, update_tables
from file_utils import load_credentials, save_dataframe_to_csv, upload_to_s3, read_aws_credentials
from database_utils import query_to_dataframe, extract_and_write_data
from database_utils import get_postgres_connection, establish_postgres_ssh_tunnel
from database_utils import establish_oracle_ssh_tunnel, get_oracle_connection



if __name__ == "__main__":

##################################################CSV_TO_AWS/S3######################################################
        
    local_file_path = 'C:/Users/azgardan/Data_Migration_project/data_migration/output/csv/data_read_table.csv'
    s3_bucket = 'intership2023'
    s3_key = 'student2/epic8/csv_to_s3.csv'

            # Read AWS credentials from config.ini
    aws_access_key_id, aws_secret_access_key = read_aws_credentials()

            # Upload data to S3
    upload_to_s3(local_file_path, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key)

    ####################################GENERATING, DELETING and UPDATING###################################################

    num_rows_to_generate = 1000

    #generate_fake_data(num_rows=num_rows_to_generate)
    #delete_fake_data(['regions', 'countries', 'locations', 'warehouses', 'employees', 'inventories', 'products', 'product_categories', 'order_items', 'orders', 'customers', 'contacts'])


    #update_values = {'region_name': 'jesus'}  # Values to be updated
    #update_conditions = {'region_id': 5}  # Conditions to identify the row(s) to be updated

    #update_tables('regions', update_values, update_conditions)

    #####################################WRITING DIRECTLY DATA FROM TABLE TO A CSV FILE#################################
    """try:

        credentials = load_credentials()

        tunnel = establish_oracle_ssh_tunnel(credentials)
        #postgres_tunnel = establish_postgres_ssh_tunnel(credentials)
        print("SSH tunnel established")
        session = get_oracle_connection(credentials, tunnel)
        print("Session established")

        extract_and_write_data(session, 'C:/Users/azgardan/Data_Migration_project/data_migration/output/csv/data_read_table.csv', 'regions', 'region_id', chunk_size=1000)

    except Exception as e:
            print(f"An error occurred: {str(e)}")
    finally:  
        try:
            tunnel.stop()
            print("SSH Tunnel stopped successfully.")
        except Exception as e:
            print(f"Error stopping SSH tunnel: {str(e)}")  
        try:
            session.close()
            print("Database session closed successfully.")
        except Exception as e:
            print(f"Error closing database session: {str(e)}")"""

        

    
################################DATA EXPORTATION TO A LOCAL CSV FILE BY READING DATA FROM SQL FILE###################################
"""    try:
        credentials = load_credentials()


    # Establish SSH tunnel
        ssh_tunnel = establish_oracle_ssh_tunnel(credentials)

    # Get PostgreSQL database session
        session = get_oracle_connection(credentials, ssh_tunnel)

    # Specify the target ORDER_DATE
        target_order_date = "05-DEC-21"
        parameters = {"target_order_date": target_order_date}

        # Replace with the actual path to your SQL script
        sql_script_path = "C:/Users/azgardan/Data_Migration_project/data_migration/scripts/sqls/big_sql_joins.sql"
            # Execute the SQL query and get the result as a DataFrame
        result_df = query_to_dataframe(session, sql_script_path, parameters)
            # Print the DataFrame
        print(result_df)

        #saving the dataframe
        csv_file_path = "C:/Users/azgardan/Data_Migration_project/data_migration/output/csv/oracle_stat.csv"

        save_dataframe_to_csv(result_df, csv_file_path)


    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        try:
            if 'ssh_tunnel' in locals() and ssh_tunnel is not None:
                ssh_tunnel.stop()
                print("SSH Tunnel stopped.")
        except Exception as e:
            print(f"Error stopping SSH Tunnel: {str(e)}")

        try:
            if 'session' in locals() and session is not None:
                session.close()
                print("Session closed.")
        except Exception as e:
            print(f"Error closing session: {str(e)}")"""


   