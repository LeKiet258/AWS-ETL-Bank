import os
import sys


# Getting to the Lambda directory
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../"))
from src.ai4e_crawler.hanhtd2_crawler import lambda_handler
from tests.common.utils import get_event_input_by_path


def test():
    path_input = "ai4e_crawler/event_imdb.json"
    event = get_event_input_by_path(path_input)
    context = None
    
    from datetime import date, timedelta

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    start_date = date(2000, 3, 1)
    end_date = date(2001, 1, 1)
    for single_date in daterange(start_date, end_date):
        print(single_date.strftime("%Y-%m-%d"))
        event['data_date'] = single_date.strftime("%Y-%m-%d")
        payload = lambda_handler(event, context)
        print(payload)
    assert 1 == 1


if __name__ == '__main__':
    test()
