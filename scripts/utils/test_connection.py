# test_connections.py

from file_utils import load_credentials
from database_utils import get_oracle_connection, establish_oracle_ssh_tunnel
from sqlalchemy import text


credentials = load_credentials()

try:
    # Establish Oracle SSH Tunnel
    oracle_tunnel = establish_oracle_ssh_tunnel(credentials)
    print("Oracle SSH Tunnel established.")

    # Test Oracle Connection
    oracle_conn = get_oracle_connection(credentials, oracle_tunnel)
    print("Connected to Oracle database.")

    # Test Query
    test_query = text('SELECT * FROM v$database')
    test_result = oracle_conn.execute(test_query)
    for row in test_result:
        print(row)

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close Oracle Connection and Stop Tunnel, regardless of success or failure
    try:
        if 'oracle_tunnel' in locals() and oracle_tunnel is not None:
            oracle_tunnel.stop()
            print("Oracle SSH Tunnel stopped.")
    except Exception as e:
        print(f"Error stopping Oracle SSH Tunnel: {str(e)}")


    try:
        if 'oracle_conn' in locals() and oracle_conn is not None:
            oracle_conn.close()
            print("Oracle connection closed.")
    except Exception as e:
        print(f"Error closing Oracle connection: {str(e)}")

    









    



# Close Oracle SSH Tunnel when done


# Establish PostgreSQL SSH Tunnel
#postgres_tunnel = establish_postgres_ssh_tunnel(credentials)
#print("PostgreSQL SSH Tunnel established.")

# Test PostgreSQL Connection
#postgres_conn = get_postgres_connection(credentials)
#print("Connected to PostgreSQL database.")

# Close PostgreSQL SSH Tunnel when done
#postgres_tunnel.stop()

# Close connections
#postgres_conn.close()
