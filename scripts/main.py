import sys
sys.path.append("C:/Users/azgardan/Data_Migration_project/data_migration/scripts/datagen")
sys.path.append("C:/Users/azgardan/Data_Migration_project/data_migration/scripts/utils")
#from oracle_data import generate_fake_data, delete_fake_data, update_tables
#from postgres_data import generate_fake_data, delete_fake_data, update_tables
from file_utils import load_credentials, save_dataframe_to_csv
from database_utils import query_to_dataframe
from database_utils import get_postgres_connection, establish_postgres_ssh_tunnel
from database_utils import establish_oracle_ssh_tunnel, get_oracle_connection



if __name__ == "__main__":

    #num_rows_to_generate = 1000

    #generate_fake_data(num_rows=num_rows_to_generate)
    #delete_fake_data(['regions', 'countries', 'locations', 'warehouses', 'employees', 'inventories', 'products', 'product_categories', 'order_items', 'orders', 'customers', 'contacts'])


    #update_values = {'region_name': 'jesus'}  # Values to be updated
    #update_conditions = {'region_id': 5}  # Conditions to identify the row(s) to be updated

    #update_tables('regions', update_values, update_conditions)



    #CREATING AND SAVING THE DATAFRAME
    try:
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
            print(f"Error closing session: {str(e)}")