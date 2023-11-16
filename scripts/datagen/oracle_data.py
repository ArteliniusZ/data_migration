import sys
sys.path.append("C:/Users/azgardan/Data_Migration_project/data_migration/scripts/utils")
from faker import Faker
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from database_utils import get_oracle_connection, establish_oracle_ssh_tunnel
from file_utils import load_credentials

credentials = load_credentials()
fake = Faker()

def generate_fake_data(num_rows):

    ssh_tunnel = establish_oracle_ssh_tunnel(credentials)
    session = get_oracle_connection(credentials, ssh_tunnel)
    # Insert fake data into regions table
    for _ in range(num_rows):
        session.execute(text("INSERT INTO regions (region_name) VALUES (:region_name)"),
                           dict(region_name=fake.word()))

    # Insert fake data into countries table
    for _ in range(num_rows):
        session.execute("""
            INSERT INTO countries (country_id, country_name, region_id) 
            VALUES (:country_id, :country_name, :region_id)
            """,
            country_id=fake.country_code(),
            country_name=fake.country(),
            region_id=fake.random_int(min=1, max=num_rows)  # Assuming region_id exists
        )

    # Insert fake data into locations table
    for _ in range(num_rows):
        session.execute("""
            INSERT INTO locations (address, postal_code, city, state, country_id) 
            VALUES (:address, :postal_code, :city, :state, :country_id)
            """,
            address=fake.address(),
            postal_code=fake.zipcode(),
            city=fake.city(),
            state=fake.state(),
            country_id=fake.country_code()
        )

    # Insert fake data into warehouses table
    for _ in range(num_rows):
        session.execute("""
            INSERT INTO warehouses (warehouse_name, location_id) 
            VALUES (:warehouse_name, :location_id)
            """,
            warehouse_name=fake.word(),
            location_id=fake.random_int(min=1, max=num_rows)  # Assuming location_id exists
        )

    # Insert fake data into employees table
    for _ in range(num_rows):
        session.execute("""
            INSERT INTO employees (first_name, last_name, email, phone, hire_date, manager_id, job_title) 
            VALUES (:first_name, :last_name, :email, :phone, :hire_date, :manager_id, :job_title)
            """,
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            email=fake.email(),
            phone=fake.phone_number(),
            hire_date=fake.date_this_decade(),
            manager_id=fake.random_int(min=1, max=num_rows),  # Assuming manager_id exists
            job_title=fake.job()
        )
    
    session.commit()


    # Close the session
    ssh_tunnel.stop()

    session.close()

    # Similar inserts for other tables...

if __name__ == "__main__":
    # Example usage
    num_rows_to_generate = 20

# Call the function
    generate_fake_data(num_rows=num_rows_to_generate)

