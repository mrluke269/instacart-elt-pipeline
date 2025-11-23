from airflow import DAG 
from airflow.operators.python import PythonOperator
import os
import snowflake.connector
from config import snowflake_config
from datetime import datetime

DATA_DIR = '/opt/airflow/data'
print(f"Container data folder path: {DATA_DIR}")
AISLES_PATH = os.path.join(DATA_DIR,'aisles.csv')
DEPARTMENTS_PATH = os.path.join(DATA_DIR,'departments.csv')
PRODUCTS_PATH = os.path.join(DATA_DIR,'products.csv')
print(f"Aisles data path in container: {AISLES_PATH}")
print(f"Departments data path in container: {DEPARTMENTS_PATH}")
print(f"Products data path in container: {PRODUCTS_PATH}")

# task 1: load aisles data to snowflake
def load_aisles_to_snowflake():
    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(**snowflake_config)
        cursor = conn.cursor()
      
        aisles_file_url = f'file://{AISLES_PATH}'
       
        put_query = f"""
            PUT '{aisles_file_url}' @~ AUTO_COMPRESS=TRUE OVERWRITE=TRUE"""
        cursor.execute(put_query)
        print(cursor.fetchall()) 
        data_filename_in_stage = os.path.basename(aisles_file_url) + ".gz"  
       
        # delete existing data in aisles table before loading
        truncate_query = f"DELETE FROM {snowflake_config['database']}.{snowflake_config['schema']}.AISLES"
        cursor.execute(truncate_query)

        # load data into aisles table
        copy_query = f"""
            COPY INTO {snowflake_config['database']}.{snowflake_config['schema']}.AISLES(aisle_id, aisle)
            FROM @~/{data_filename_in_stage}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            ON_ERROR = 'CONTINUE'"""
        cursor.execute(copy_query)
        print(cursor.fetchall())
        
    except Exception as e:
        print(f"Error in load_aisles_to_snowflake: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# task 2: load departments data to snowflake
def load_departments_to_snowflake():
    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(**snowflake_config)
        cursor = conn.cursor()
        
        departments_file_url = f'file://{DEPARTMENTS_PATH}'

        # put file to snowflake stage
        put_query = f"""
            PUT '{departments_file_url}' @~ AUTO_COMPRESS=TRUE OVERWRITE=TRUE"""
        cursor.execute(put_query)
        print(cursor.fetchall()) 
        data_filename_in_stage = os.path.basename(departments_file_url) + ".gz"  

        # delete existing data in departments table before loading
        truncate_query = f"DELETE FROM {snowflake_config['database']}.{snowflake_config['schema']}.DEPARTMENTS"
        cursor.execute(truncate_query)

        # load data into departments table
        copy_query = f"""
            COPY INTO {snowflake_config['database']}.{snowflake_config['schema']}.DEPARTMENTS(department_id, department)
            FROM @~/{data_filename_in_stage}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            ON_ERROR = 'CONTINUE'"""
        cursor.execute(copy_query)
        print(cursor.fetchall())
    except Exception as e:
        print(f"Error in load_departments_to_snowflake: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# task 3: load products data to snowflake
def load_products_to_snowflake():
    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(**snowflake_config)
        cursor = conn.cursor()
        
        products_file_url = f'file://{PRODUCTS_PATH}'

        # put file to snowflake stage
        put_query = f"""
            PUT '{products_file_url}' @~ AUTO_COMPRESS=TRUE OVERWRITE=TRUE"""
        cursor.execute(put_query)
        print(cursor.fetchall()) 
        data_filename_in_stage = os.path.basename(products_file_url) + ".gz"  

        # delete existing data in products table before loading
        truncate_query = f"DELETE FROM {snowflake_config['database']}.{snowflake_config['schema']}.PRODUCTS"
        cursor.execute(truncate_query)

        # load data into products table
        copy_query = f"""
            COPY INTO {snowflake_config['database']}.{snowflake_config['schema']}.PRODUCTS(product_id, product_name, aisle_id, department_id)
            FROM @~/{data_filename_in_stage}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            ON_ERROR = 'CONTINUE'"""
        cursor.execute(copy_query)
        print(cursor.fetchall())
    except Exception as e:
        print(f"Error in load_products_to_snowflake: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

with DAG(
    dag_id='static_EL_dag',
    start_date=datetime(2025, 11, 11),
    schedule_interval=None,
    catchup=False,
    tags=['static','EL','instacart']
) as dag:

    load_aisles_task = PythonOperator(
        task_id='load_aisles_to_snowflake',
        python_callable=load_aisles_to_snowflake
    )

    load_departments_task = PythonOperator(
        task_id='load_departments_to_snowflake',
        python_callable=load_departments_to_snowflake
    )

    load_products_task = PythonOperator(
        task_id='load_products_to_snowflake',
        python_callable=load_products_to_snowflake
    )

[load_aisles_task, load_departments_task] >> load_products_task
