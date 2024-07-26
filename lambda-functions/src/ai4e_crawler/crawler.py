import logging
import traceback

from src.ai4e_crawler.utils.crawler_utils import crawl_data
from src.common.db_utils import create_db_if_exists, create_connection, insert_db_using_copy_string_iterator
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    try:
        logger.info(f"EVENT: {event}")
        logger.info(f"CONTEXT: {context}")

        logger.info(f"test update code")

        table_name = event['table_name']
        urls = event['urls']
        data_field = event['data_field']
        data_field_tuple = [(field["field"], field["type"]) for field in data_field]
        connection = create_connection()

        create_db_if_exists(connection, table_name, data_field_tuple)
        movies = [crawl_data(url) for url in urls]
        print(f"number movies: {len(movies)}")
        flat_movies_list = [item for sublist in movies for item in sublist]
        insert_db_using_copy_string_iterator(connection, table_name, data_field_tuple, flat_movies_list, size=1024 * 8)
        connection.close()




        return {"status": "SUCCESS"}
    except Exception as ex:
        logger.error(f'FATAL ERROR: {ex} %s')
        logger.error('TRACEBACK:')
        logger.error(traceback.format_exc())

        return {"status": "FAIL", "error": f"{ex}"}
