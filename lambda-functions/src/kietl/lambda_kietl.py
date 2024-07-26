import json
import logging
import traceback
import boto3
import psycopg2

from src.common.db_utils import create_connection, fetch_data_by_sql, create_binary_from_list_data

# Thiết lập logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Hàm chính được gọi bởi AWS Lambda
def pg_to_s3(event, context):
    config_file_path = 'src/kietl/pg_to_s3_config.json'

    try:
        # Read the JSON config file into a dictionary
        map_config = None
        with open(config_file_path, 'r') as file:
            map_config = json.load(file)
        
        pg_config = map_config['pg']
        s3_config = map_config['s3']
        
        conn = psycopg2.connect(
            host=pg_config['host'], 
            user=pg_config['user'], 
            password=pg_config['password'],
            database=pg_config['database']
        )
        
        # init s3 & put csv_binary to s3 bucket under the object/key path
        client = boto3.client('s3')
        
        list_source_table = ['account', 'account_type', 'customer', 'payment_transaction', 'payment_type']
        save_dir = s3_config['object_key']
       
        for source_table in list_source_table:
            full_source_table = f"{pg_config['schema']}.{source_table}"
            list_map_rows = fetch_data_by_sql(conn, f"select * from {full_source_table}")
            csv_binary = create_binary_from_list_data(list_map_rows)
            
            logger.info(f"DEBUG key: {save_dir}{source_table}")
            
            
            client.put_object(
                Body=csv_binary.read(), 
                Bucket=s3_config['bucket_name'],
                Key=f'{save_dir}{source_table}.csv'
            )
            
            # break

        conn.close()
        
        return {
            "status": "SUCCESS", 
            "message": f"Successfully load 3 tables to s3"
        }
    
    except Exception as ex:
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())
        
        return {
            "status": "FAIL", 
            "message": f"Failed to load 3 tables to s3. Details: {ex}"
        }


def lambda_handler(event, context):
    try:
        # Log thông tin về sự kiện và ngữ cảnh
        logger.info(f"EVENT: {event}")
        logger.info(f"CONTEXT: {context}")

        # Lấy thông tin từ event (lấy trong file test/resources/ai4e_crawler/event.json)
        table_name = event['table_name']
        bucket_name = event['bucket_name']
        object_key = event['object_key']

        # Tạo kết nối đến cơ sở dữ liệu
        connection = create_connection()

        # Tạo câu truy vấn SQL
        query_str_sql = f"select * from {table_name}"
        # Lấy dữ liệu từ cơ sở dữ liệu (list of dicts)
        results = fetch_data_by_sql(connection, query_str_sql)

        # Tạo file nhị phân từ dữ liệu
        csv_binary = create_binary_from_list_data(results)

        # Tạo client S3
        client = boto3.client('s3')
        
        # Đưa file nhị phân lên S3
        client.put_object(
            Body=csv_binary.read(), 
            Bucket=bucket_name,
            Key=object_key
        )

        # Đóng kết nối đến cơ sở dữ liệu
        connection.close()
        # Trả về trạng thái thành công
        return {"status": "SUCCESS"}

    except Exception as ex:
        # Nếu có lỗi, log lỗi và trả về trạng thái thất bại
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())
        
        return {"status": "FAIL", "error": f"{ex}"}
    

