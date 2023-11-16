# utils/database_utils.py

from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker


def get_oracle_connection(credentials):
    connection_str = f"oracle+cx_oracle://{credentials['oracle_username']}:{credentials['oracle_password']}@127.0.0.1:{credentials['local_port']}/?service_name=orclpdb"
    engine = create_engine(connection_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def get_postgres_connection(credentials):
    connection_str = f"postgresql://{credentials['postgres_username']}:{credentials['postgres_password']}@127.0.0.1:{credentials['local_port']}/epic4_db"
    engine = create_engine(connection_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def establish_oracle_ssh_tunnel(credentials):
    ssh_tunnel = SSHTunnelForwarder(
        (credentials['ssh_host'], int(credentials['ssh_port'])),
        ssh_username=credentials['ssh_username'],
        ssh_pkey=credentials['ssh_private_key'],  # Path to the private key file
        ssh_private_key_password=credentials['ssh_passkey'],
        remote_bind_address=(credentials['oracle_host'], 1521)  # Oracle default port
    )
    ssh_tunnel.start()
    return ssh_tunnel

def establish_postgres_ssh_tunnel(credentials):
    ssh_tunnel = SSHTunnelForwarder(
        (credentials['ssh_host'], int(credentials['ssh_port'])),
        ssh_username=credentials['ssh_username'],
        ssh_pkey=credentials['ssh_private_key'],  # Path to the private key file
        ssh_private_key_password=credentials['ssh_passkey'],
        remote_bind_address=(credentials['postgres_host'], 5432)  # PostgreSQL default port
    )
    ssh_tunnel.start()
    return ssh_tunnel
