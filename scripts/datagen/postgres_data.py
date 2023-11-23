import sys
sys.path.append("C:/Users/azgardan/Data_Migration_project/data_migration/scripts/utils")
from faker import Faker
import faker_commerce
from sqlalchemy import text
from database_utils import get_postgres_connection, establish_postgres_ssh_tunnel
from file_utils import load_credentials

credentials = load_credentials()
fake = Faker()
fake.add_provider(faker_commerce.Provider)

def generate_fake_data(num_rows):
    try:
        ssh_tunnel = establish_postgres_ssh_tunnel(credentials)
        print("Postgres SSH tunnel established")
        session = get_postgres_connection(credentials, ssh_tunnel)
        print("Connected to Postgres database")


        reset_id_queries = [
        "SELECT setval('regions_region_id_seq', COALESCE(MAX(region_id), 1), false) FROM regions",
        "SELECT setval('locations_location_id_seq', COALESCE(MAX(location_id), 1), false) FROM locations",
        "SELECT setval('warehouses_warehouse_id_seq', COALESCE(MAX(warehouse_id), 1), false) FROM warehouses",
        "SELECT setval('employees_employee_id_seq', COALESCE(MAX(employee_id), 1), false) FROM employees",
        "SELECT setval('product_categories_category_id_seq', COALESCE(MAX(category_id), 1), false) FROM product_categories",
        "SELECT setval('products_product_id_seq', COALESCE(MAX(product_id), 1), false) FROM products",
        "SELECT setval('orders_order_id_seq', COALESCE(MAX(order_id), 1), false) FROM orders",
        "SELECT setval('customers_customer_id_seq', COALESCE(MAX(customer_id), 1), false) FROM customers",
        "SELECT setval('contacts_contact_id_seq', COALESCE(MAX(contact_id), 1), false) FROM contacts"
        ]

        for query in reset_id_queries:
            session.execute(text(query))


        regions_data = [{'region_name': fake.word()} for _ in range(num_rows)]
        session.execute(text("INSERT INTO regions (region_name) VALUES (:region_name)"), regions_data)
        print("regions done")


        # Insert fake data into countries table
        countries_data = [
            {
                'country_id': fake.country_code(),
                'country_name': fake.country(),
                'region_id': fake.random_int(min=1, max=num_rows)
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO countries (country_id, country_name, region_id) VALUES (:country_id, :country_name, :region_id)"),
            countries_data
        )
        print('countries done')

        locations_data = [
            {
                'address': fake.address(),
                'postal_code': fake.zipcode(),
                'city': fake.city(),
                'state': fake.state(),
                'country_id': fake.country_code()
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO locations (address, postal_code, city, state, country_id) VALUES (:address, :postal_code, :city, :state, :country_id)"),
            locations_data
        )
        print('locations done')


        warehouses_data = [
            {
                'warehouse_name': fake.word(),
                'location_id': fake.random_int(min=1, max=num_rows)
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO warehouses (warehouse_name, location_id) VALUES (:warehouse_name, :location_id)"),
            warehouses_data
        )
        print('warehouses done')


      # Insert fake data into product_categories table in bulk
        product_categories_data = [
            {'category_name': fake.ecommerce_category()}
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO product_categories (category_name) VALUES (:category_name)"),
            product_categories_data
        )
        print('product_categories done')

        
        # Insert fake data into products table in bulk
        products_data = [
            {
                'product_name': fake.ecommerce_name(),
                'description': fake.word(),
                'standard_cost': fake.random_int(min=10, max=50),
                'list_price': fake.random_int(min=100, max=500),
                'category_id': fake.random_int(min=1, max=num_rows)
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO products (product_name, description, standard_cost, list_price, category_id) VALUES (:product_name, :description, :standard_cost, :list_price, :category_id)"),
            products_data
        )
        print('products done')

            
        # Insert fake data into inventories table in bulk
        inventories_data = [
            {
                'product_id': fake.random_int(min=1, max=num_rows),
                'warehouse_id': fake.random_int(min=1, max=num_rows),
                'quantity': fake.random_int()
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO inventories (product_id, warehouse_id, quantity) VALUES (:product_id, :warehouse_id, :quantity)"),
            inventories_data
        )
        print('inventories done')


        # Insert fake data into order_items table in bulk
        order_items_data = [
    {
        'order_id': fake.random_int(min=1, max=num_rows),
        'item_id': fake.random_int(min=1, max=num_rows),
        'product_id': fake.random_int(min=1, max=num_rows),
        'quantity': fake.random_int(),
        'unit_price': fake.random_int(min=100, max=500)
    }
    for _ in range(num_rows)]
        session.execute(
            text("INSERT INTO order_items (order_id, item_id, product_id, quantity, unit_price) VALUES (:order_id, :item_id, :product_id, :quantity, :unit_price)"),
            order_items_data
        )
        print('order_items done')


        # Insert fake data into orders table in bulk
        orders_data = [
            {
                'customer_id': fake.random_int(min=1, max=num_rows),
                'status': fake.word(),
                'salesman_id': fake.random_int(min=1, max=num_rows),
                'order_date': fake.date_this_decade()
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO orders (customer_id, status, salesman_id, order_date) VALUES (:customer_id, :status, :salesman_id, :order_date)"),
            orders_data
        )
        print('orders done')


        # Insert fake data into customers table in bulk
        customers_data = [
            {
                'name': fake.name(),
                'address': fake.address(),
                'website': fake.word(),
                'credit_limit': fake.random_int(min=1000, max=5000)
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO customers (name, address, website, credit_limit) VALUES (:name, :address, :website, :credit_limit)"),
            customers_data
        )
        print('customers done')


# Insert fake data into contacts table in bulk
        contacts_data = [
            {
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'customer_id': fake.random_int(min=1, max=num_rows)
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO contacts (first_name, last_name, email, phone, customer_id) VALUES (:first_name, :last_name, :email, :phone, :customer_id)"),
            contacts_data
        )
        print('contacts done')



        # Insert fake data into employees table
        # Insert fake data into employees table in bulk
        employees_data = [
            {
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'hire_date': fake.date_this_decade(),
                'manager_id': fake.random_int(min=1, max=num_rows),
                'job_title': fake.job()
            }
            for _ in range(num_rows)
        ]
        session.execute(
            text("INSERT INTO employees (first_name, last_name, email, phone, hire_date, manager_id, job_title) VALUES (:first_name, :last_name, :email, :phone, :hire_date, :manager_id, :job_title)"),
            employees_data
        )
        print('employees done')

        # Commit the changes
        session.commit()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the session and stop the SSH tunnel, regardless of success or failure
        try:
            if 'ssh_tunnel' in locals() and ssh_tunnel is not None:
                ssh_tunnel.stop()
                print("Postgres SSH Tunnel stopped.")
        except Exception as e:
            print(f"Error stopping Postgres SSH Tunnel: {str(e)}")

        try:
            if 'session' in locals() and session is not None:
                session.close()
                print("Postgres session closed.")
        except Exception as e:
            print(f"Error closing Postgres session: {str(e)}")


def delete_fake_data(table_names):
    try:
        ssh_tunnel = establish_postgres_ssh_tunnel(credentials)
        print("Postgres SSH tunnel established.")
        session = get_postgres_connection(credentials, ssh_tunnel)
        print("Connected to Postgres database.")


        # Insert fake data into regions table
        for table_name in table_names:
            session.execute(text(f"DELETE FROM {table_name}"))
        session.commit()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the session and stop the SSH tunnel, regardless of success or failure
        try:
            if 'ssh_tunnel' in locals() and ssh_tunnel is not None:
                ssh_tunnel.stop()
                print("Postgres SSH Tunnel stopped.")
        except Exception as e:
            print(f"Error stopping Postgres SSH Tunnel: {str(e)}")

        try:
            if 'session' in locals() and session is not None:
                session.close()
                print("Postgres session closed.")
        except Exception as e:
            print(f"Error closing Postgres session: {str(e)}")

    

def update_tables(table_name, set_values, conditions):
    try:
        ssh_tunnel = establish_postgres_ssh_tunnel(credentials)
        print("Postgres SSH tunnel established.")
        session = get_postgres_connection(credentials, ssh_tunnel)
        print("Connected to Postgres database.")

         # Generate the SET clause for the update statement
        set_clause = ', '.join([f"{column} = :{column}" for column in set_values.keys()])

        # Generate the WHERE clause for the update statement
        where_clause = ' AND '.join([f"{column} = :{column}" for column in conditions.keys()])

        # Construct the SQL update statement
        update_query = text(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}")

        # Execute the update statement with the provided values
        session.execute(update_query, {**set_values, **conditions})

        # Commit the changes
        session.commit()

        print(f"Table '{table_name}' updated successfully.")

    except Exception as e:
            print(f"An error occurred: {str(e)}")

    finally:
            # Close the session and stop the SSH tunnel, regardless of success or failure
            try:
                if 'ssh_tunnel' in locals() and ssh_tunnel is not None:
                    ssh_tunnel.stop()
                    print("Postgres SSH Tunnel stopped.")
            except Exception as e:
                print(f"Error stopping Postgres SSH Tunnel: {str(e)}")

            try:
                if 'session' in locals() and session is not None:
                    session.close()
                    print("Postgres session closed.")
            except Exception as e:
                print(f"Error closing Postgres session: {str(e)}")


