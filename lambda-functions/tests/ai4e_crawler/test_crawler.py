import os
import sys


# Getting to the Lambda directory
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../"))
from src.ai4e_crawler.crawler import lambda_handler
from tests.common.utils import get_event_input_by_path


def test():
    path_input = "ai4e_crawler/event.json"
    event = get_event_input_by_path(path_input)
    context = None

    payload = lambda_handler(event, context)
    print(payload)
    assert 1 == 1


if __name__ == '__main__':
    test()
