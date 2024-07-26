import logging
import traceback

from src.ai4e_crawler.utils.crawler_utils import crawl_box_office
from src.common.db_utils import create_schema_tables_exist, create_connection, insert_db_using_copy_string_iterator
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    try:
        logger.info(f"EVENT: {event}")
        logger.info(f"CONTEXT: {context}")
        table_name = event['table_name']
        schema_name = event['schema_name']
        url = event['url']
        data_field = event['data_field']
        data_date = event['data_date']
        data_field_tuple = [(field["field"], field["type"]) for field in data_field]
        connection = create_connection()

        create_schema_tables_exist(connection, schema_name, table_name, data_field_tuple)
        movies = crawl_box_office(url, data_date)
        insert_db_using_copy_string_iterator(connection, table_name, data_field_tuple, movies, size=1024 * 8, schema_name=schema_name)
        connection.close()

        return {"status": "SUCCESS"}
    except Exception as ex:
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())

        return {"status": "FAIL", "error": f"{ex}"}
