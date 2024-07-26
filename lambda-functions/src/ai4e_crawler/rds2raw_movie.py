import logging
import traceback
import boto3

from src.common.db_utils import create_connection, fetch_data_by_sql, create_binary_from_list_data
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    try:
        logger.info(f"EVENT: {event}")
        logger.info(f"CONTEXT: {context}")

        table_name = event['table_name']
        bucket_name = event['bucket_name']
        object_key = event['object_key']

        connection = create_connection()

        query_str_sql = f"select * from {table_name}"
        results = fetch_data_by_sql(connection, query_str_sql)

        csv_binary = create_binary_from_list_data(results)

        client = boto3.client('s3')
        client.put_object(
            Body=csv_binary.read(), 
            Bucket=bucket_name,
            Key=object_key
        )

        connection.close()
        return {"status": "SUCCESS"}
    except Exception as ex:
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())
        return {"status": "FAIL", "error": f"{ex}"}
