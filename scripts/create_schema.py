import snowflake.connector # Snowflake connector for Python
from config import snowflake_config # Importing configuration details

def create_orders_table():
    conn = None
    cursor = None
    
    try:
        # 1. Connect to Snowflake
        conn = snowflake.connector.connect(**snowflake_config)
        cursor = conn.cursor()
        
        #2. Create Schema if not exists
        create_schema_query = f"""
        CREATE SCHEMA IF NOT EXISTS {snowflake_config['database']}.{snowflake_config['schema']};
        """
        cursor.execute(create_schema_query)
        print(f"Schema '{snowflake_config['schema']}' ensured.")

        # 3. Define and create the ORDERS Table in Snowflake
        create_table_query = f"""
        CREATE OR REPLACE TABLE {snowflake_config['database']}.{snowflake_config['schema']}.ORDERS (
            ORDER_ID BIGINT NOT NULL PRIMARY KEY,
            USER_ID BIGINT NOT NULL,
            EVAL_SET VARCHAR(50),
            ORDER_NUMBER BIGINT,
            ORDER_DOW INT,
            ORDER_HOUR_OF_DAY INT,
            DAYS_SINCE_PRIOR_ORDER FLOAT,
            ORDER_DATE DATE NOT NULL,
            LOAD_AT TIMESTAMP_NTZ NOT NULL
        );
        """
        cursor.execute(create_table_query)
        print("Table ORDERS created successfully in Snowflake.")

        # 4. Define and create the AISLES Table in Snowflake
        create_aisles_table_query = f"""
        CREATE OR REPLACE TABLE {snowflake_config['database']}.{snowflake_config['schema']}.AISLES (
            AISLE_ID INT NOT NULL PRIMARY KEY,
            AISLE VARCHAR(255) NOT NULL
        );
        """
        cursor.execute(create_aisles_table_query)
        print("Table AISLES created successfully in Snowflake.")

        # 5. Define and create the DEPARTMENTS Table in Snowflake
        create_departments_table_query = f"""
        CREATE OR REPLACE TABLE {snowflake_config['database']}.{snowflake_config['schema']}.DEPARTMENTS (
            DEPARTMENT_ID INT NOT NULL PRIMARY KEY,
            DEPARTMENT VARCHAR(100) NOT NULL
        );
        """
        cursor.execute(create_departments_table_query)
        print("Table 'DEPARTMENTS' created successfully in Snowflake.")

        #6. Define and create the PRODUCTS Table in Snowflake
        create_products_table_query = f"""
        CREATE OR REPLACE TABLE {snowflake_config['database']}.{snowflake_config['schema']}.PRODUCTS (
            PRODUCT_ID BIGINT NOT NULL PRIMARY KEY,
            PRODUCT_NAME VARCHAR(100) NOT NULL,
            AISLE_ID INT NOT NULL,
            DEPARTMENT_ID int NOT NULL,
            FOREIGN KEY (AISLE_ID) REFERENCES AISLES(AISLE_ID),
            FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS(DEPARTMENT_ID)
        );
        """
        cursor.execute(create_products_table_query)
        print("Table 'PRODUCTS' created successfully in Snowflake.")
        

    #7. Define and create the ORDER_PRODUCTS Table in Snowflake
        create_order_products_table_query = f"""
        CREATE OR REPLACE TABLE {snowflake_config['database']}.{snowflake_config['schema']}.ORDER_PRODUCTS (
            ORDER_ID BIGINT NOT NULL,
            PRODUCT_ID BIGINT NOT NULL,
            ADD_TO_CART_ORDER INT NOT NULL,
            REORDERED boolean NOT NULL,
            PRIMARY KEY (ORDER_ID, PRODUCT_ID),
            FOREIGN KEY (ORDER_ID) REFERENCES ORDERS(ORDER_ID),
            FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCTS(PRODUCT_ID),
            LOAD_AT TIMESTAMP_NTZ NOT NULL
        );
        """
        cursor.execute(create_order_products_table_query)
        print("Table 'ORDER_PRODUCTS' created successfully in Snowflake.")
    
    except Exception as e:
        print(f"Error creating table: {e}")
        
    finally:
        # 4. Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_orders_table()