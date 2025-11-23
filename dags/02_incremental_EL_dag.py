import pandas as pd # For data manipulation
from pathlib import Path # For path manipulations
from airflow import DAG # For defining the DAG
from airflow.operators.python import PythonOperator # For Python tasks
from airflow.operators.bash import BashOperator # For Container Bash tasks 
from datetime import date, datetime
import os # For directory operations
import snowflake.connector
from config import snowflake_config
from airflow.exceptions import AirflowSkipException

# Build path for saving and reading files in the container
DATA_DIR = '/opt/airflow/data' # Define the base data directory in container
print(f"Container data folder path: {DATA_DIR}")
FILTERED_DIR = os.path.join(DATA_DIR, 'filtered') # Define the filtered data directory in container
os.makedirs(FILTERED_DIR, exist_ok=True) # Ensure the filtered directory exists in the container
print(f"Created in container: {FILTERED_DIR}")
ORDERS_PATH = os.path.join(DATA_DIR,'orders_with_dates_2025.csv') # Full path to orders data in container
ORDER_PRODUCTS_PATH = os.path.join(DATA_DIR,'order_products_filtered_2025.csv') # Full path to order_products data in container
print(f"Orders data path in container: {ORDERS_PATH}")




# Task 1: Filter orders data for the run_date, Cleanup existing data, save data to disk, load to snowflake and push path to XCom
def filtered_orders_and_save(**context):
    try:
        ti = context['ti']
        run_date = context['execution_date'].strftime('%Y-%m-%d')
        conn = None
        cursor = None
        try:
            conn = snowflake.connector.connect(**snowflake_config)
            cursor = conn.cursor()
            
            # Cleanup existing data for the run_date in ORDER_PRODUCTS table (Child table)
            delete_query_order_products = f"""
            DELETE FROM {snowflake_config['database']}.{snowflake_config['schema']}.ORDER_PRODUCTS
            WHERE order_id IN (
                SELECT order_id FROM {snowflake_config['database']}.{snowflake_config['schema']}.ORDERS
                WHERE order_date = '{run_date}'
            );
            """
            cursor.execute(delete_query_order_products)
            print(f"Deleted existing records from ORDER_PRODUCTS for order_date={run_date}")

             # Cleanup existing data for the run_date in ORDERS table (Parent table)
            delete_query_orders = f"""
            DELETE FROM {snowflake_config['database']}.{snowflake_config['schema']}.ORDERS
            WHERE order_date = '{run_date}';
            """
            cursor.execute(delete_query_orders)
            print(f"Deleted existing records from ORDERS for order_date={run_date}")
        except Exception as e:
            print(f"Error during cleanup in Snowflake: {e}")
            raise e
        finally:
            if cursor:
                cursor.close() 
            if conn:
                conn.close()
   
        df = pd.read_csv(ORDERS_PATH) # Read orders data
        df['order_date'] = pd.to_datetime(df['order_date']).dt.strftime('%Y-%m-%d') # Ensure order_date is in 'YYYY-MM-DD' format

        # Filter orders for the run_date
        filtered = df[df['order_date'] == run_date]
        print(f"\nOrders for {run_date}: {len(filtered)}")

        
        # If no orders for the run_date, exit the task
        if filtered.empty:
            print(f"No orders found for the run_date: {run_date}. Exiting task.")
            raise AirflowSkipException(f"No orders found for the run_date: {run_date}") # Raise AirflowSkipException to mark task as skipped


        # Save filtered data
        output_path = os.path.join(FILTERED_DIR, f'orders_{run_date}.csv') # DEFINE the file output path and name in container
        filtered.to_csv(output_path, index=False) # Save to CSV
        print(f"Filtered orders saved to {output_path}")

        # Load to Snowflake
        # Establish Snowflake connection
        conn = None
        cursor = None
        try:
            conn = snowflake.connector.connect(**snowflake_config)
            cursor = conn.cursor()
            # Build a file URL
            file_uri = 'file://' + Path(output_path).resolve().as_posix() # 'file://' is the protocol for file URIs stored in the local file system
                                                                          # Path(output_path) takes string path and coversts to a Path object
                                                                          # .resolve() gets absolute path
                                                                          # .as_posix() converts Windows backslashes to forward slashes

            # Put the filtered data into stage
            put_query = f"""
            PUT '{file_uri}' @~ AUTO_COMPRESS=TRUE OVERWRITE=TRUE"""                     # Pick file from local file system
                                                                                         # and put into Snowflake stage  
                                                                                         # Stage is user stage (~)
            cursor.execute(put_query) # Execute the PUT command
            print(cursor.fetchall()) # Print the result of the PUT command
            filename_in_stage = os.path.basename(output_path) + ".gz"  # Snowflake auto-compresses the file during PUT, so we add ".gz" to the filename
                                                                       # # FROM @~/{os.path.basename(output_path)}
                                                                       # This line locates the file put in the Snowflake stage in the previous step.
                                                                       # os.path.basename(output_path) extracts just the filename from the full output_path.
                                                                       # Example: if output_path is "C:\Users\You\data\filtered\orders_2025-11-13.csv"
                                                                       # os.path.basename(output_path) will return "orders_2025-11-13.csv"
                                                                       # When Put command uploads the file to the stage, the stage only stores the filename, not the full path.
                                                                       # If not used os.path.basename, the full path would be included, not the filename, causing an error in locating the file in the stage.

            # Copy into Snowflake table
            copy_query = f"""
            COPY INTO {snowflake_config['database']}.{snowflake_config['schema']}.ORDERS(order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order, order_date, LOAD_AT) 
            FROM 
                (SELECT $1::BIGINT,
                $2::BIGINT,
                $3::VARCHAR,
                $4::BIGINT,
                $5::INT,
                $6::INT,
                $7::FLOAT,
                $8::DATE,
                CURRENT_TIMESTAMP()
                FROM  @~/{filename_in_stage}
            )
            
            FILE_FORMAT = (TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            empty_field_as_null = TRUE)
            ON_ERROR = 'CONTINUE';
            """


            cursor.execute(copy_query) # Execute the COPY command
            print(f"COPY command: successfully loaded orders data from stage.")

            # Push path to XCom for downstream tasks
            order_ids = filtered['order_id'].tolist() # Extract order IDs
            ti.xcom_push(key='order_ids', value=order_ids) # Push order IDs to XCom
            ti.xcom_push(key='filtered_orders_and_save_path', value=output_path) # Push path to XCom for downstream tasks
            ti.xcom_push(key='run_date', value=run_date) # Push run_date to XCom
            print(f"Pushed to XCom: filtered_orders_and_save_path={output_path}, run_date={run_date}, order_ids count={len(order_ids)}")
        
        except Exception as e:
            print(f"Error loading to Snowflake:{e}")
            raise e
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    except Exception as e:  
        print(f"Error filtered_orders_and_save: {e}")
        raise e

# Task 2: Read and filter the order_products data for the extracted order IDs, cleanup, then save to container, load to snowflake and push path to XCom
def filter_order_products_and_save(**context):
    try:
        ti = context['ti']
        run_date = ti.xcom_pull(key='run_date', task_ids='filtered_orders_and_save') # Pull run_date from XCom
        order_ids = ti.xcom_pull(key='order_ids', task_ids='filtered_orders_and_save') # Pull order IDs from XCom
        try:
            conn = snowflake.connector.connect(**snowflake_config)
            cursor = conn.cursor()
             # Cleanup existing data for the run_date in ORDER_PRODUCTS table
            delete_query_order_products = f"""
            DELETE FROM {snowflake_config['database']}.{snowflake_config['schema']}.ORDER_PRODUCTS
            WHERE order_id IN ({', '.join(map(str, order_ids))});
            """
            cursor.execute(delete_query_order_products)
            print(f"Deleted existing records from ORDER_PRODUCTS for {len(order_ids)} orders")
        except Exception as e:
            print(f"Error during cleanup in Snowflake: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        df_order_products = pd.read_csv(ORDER_PRODUCTS_PATH)   # Read order_products data
        filtered_order_products = df_order_products[df_order_products['order_id'].isin(order_ids)] # Filter order_products for the extracted order IDs
        print(f"\nFiltered order_products for {len(order_ids)} orders: {len(filtered_order_products)}")

        # Save filtered data
        output_path_order_products = os.path.join(FILTERED_DIR, f'order_products_{run_date}.csv') # DEFINE the file output path and name in container
        filtered_order_products.to_csv(output_path_order_products, index=False) # Save to CSV
        print(f"Filtered order_products saved to {output_path_order_products}")

        # Load to Snowflake
        # Establish Snowflake connection
        conn = None
        cursor = None
        try:
            conn = snowflake.connector.connect(**snowflake_config)
            cursor = conn.cursor()
            # Build a file URL
            file_uri = 'file://' + Path(output_path_order_products).resolve().as_posix() 

            put_query = f"""
            PUT '{file_uri}' @~ AUTO_COMPRESS=TRUE OVERWRITE=TRUE"""   
            cursor.execute(put_query) # Execute the PUT command
            print(cursor.fetchall()) # Print the result of the PUT command

            filename_in_stage = os.path.basename(output_path_order_products) + ".gz"

            # Copy into Snowflake table
            copy_query = f"""
            COPY INTO {snowflake_config['database']}.{snowflake_config['schema']}.ORDER_PRODUCTS(order_id, product_id, add_to_cart_order, reordered, LOAD_AT) 
            FROM (
                SELECT
                $1::BIGINT,
                $2::BIGINT, 
                $3::INT,
                $4::BOOLEAN,
                CURRENT_TIMESTAMP()
                FROM  @~/{filename_in_stage})

            FILE_FORMAT = (TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            empty_field_as_null = TRUE)

            ON_ERROR = 'CONTINUE';
            """

            cursor.execute(copy_query)
            print(f"COPY command: successfully loaded order_products data from stage.")
        except Exception as e:
            print(f"Error loading to Snowflake:{e}")
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        # Push path to XCom
        ti.xcom_push(key='filtered_order_products_path', value=output_path_order_products)
        
    except Exception as e:
        print(f"Error in filter_order_products_and_save: {e}")
        raise e

# Task 3: validate loaded data in Snowflake using SQL/python
def validate_loaded_data(**context):
    try:
        ti = context['ti']
        run_date = ti.xcom_pull(key='run_date', task_ids='filtered_orders_and_save') # Pull run_date from XCom

        conn = None
        cursor = None
        try:
            conn = snowflake.connector.connect(**snowflake_config)
            cursor = conn.cursor()
        #    Check 1: NULL order_id in ORDERS
            cursor.execute(f"""
            SELECT COUNT(*) FROM {snowflake_config['database']}.{snowflake_config['schema']}.ORDERS
            WHERE order_id IS NULL 
              AND order_date = '{run_date}'
        """)
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                raise ValueError(f"Found {null_count} NULL order_ids in ORDERS for {run_date}")
        
        #    Check 2: NULL product_id in today's ORDER_PRODUCTS
            cursor.execute(f"""
            SELECT COUNT(*) FROM {snowflake_config['database']}.{snowflake_config['schema']}.ORDER_PRODUCTS
            WHERE product_id IS NULL
              AND order_id IN (
                  SELECT order_id FROM {snowflake_config['database']}.{snowflake_config['schema']}.ORDERS 
                  WHERE order_date = '{run_date}'
              )
        """)
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                raise ValueError(f"Found {null_count} NULL product_ids in ORDER_PRODUCTS for {run_date}")
            print(f"All validations passed for {run_date}")
        
        except Exception as e:
            print(f"Validation error: {e}")
            raise e
        
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        print(f"Error in validate_loaded_data: {e}")
        raise e


# Define the DAG
with DAG(
    dag_id='incremental_EL_dag',
    start_date=datetime(2025, 11, 15),
    schedule_interval='@daily',
    catchup=False,
    tags=['instacart', 'incremental', 'EL']
) as dag:

    # Define tasks
    filtered_orders_and_save_task = PythonOperator(
        task_id='filtered_orders_and_save',
        python_callable=filtered_orders_and_save,
        retry_delay=300, # 5 minutes
        retries=3,
        retry_exponential_backoff=False
    )
    filter_order_products_and_save_task = PythonOperator(
        task_id='filter_order_products_and_save',
        python_callable=filter_order_products_and_save,
        retry_delay=300, # 5 minutes
        retries=3,
        retry_exponential_backoff=False
    )
    validate_loaded_data_task = PythonOperator(
        task_id='validate_loaded_data',
        python_callable=validate_loaded_data,
        retry_delay=300, # 5 minutes
        retries=3,
        retry_exponential_backoff=False
    )

    transform_dbt_task = BashOperator(
        task_id='transform_data_with_dbt',
        bash_command = 'dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --target prod')
    
    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command = 'dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --target prod')

    # Set task dependencies
    filtered_orders_and_save_task >> filter_order_products_and_save_task >> validate_loaded_data_task >> transform_dbt_task >> dbt_test_task

    