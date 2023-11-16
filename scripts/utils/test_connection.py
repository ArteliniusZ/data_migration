# test_connections.py

from file_utils import load_credentials
from database_utils import get_oracle_connection, establish_oracle_ssh_tunnel
from sqlalchemy import text


credentials = load_credentials()

# Establish Oracle SSH Tunnel
oracle_tunnel = establish_oracle_ssh_tunnel(credentials)
print("Oracle SSH Tunnel established.")

# Test Oracle Connection
oracle_conn = get_oracle_connection(credentials, oracle_tunnel)
print("Connected to Oracle database.")

test = oracle_conn.execute(text('select * from v$database'))
for row in test:
    print(row)

oracle_tunnel.stop()


oracle_conn.close()





    



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
