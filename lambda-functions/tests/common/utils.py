import os
import json


def get_event_input_by_path(path):
    input_dir = os.path.join(os.path.dirname(os.path.realpath(__file__))) + "/../resources/"
    json_file_dir = input_dir + path
    with open(json_file_dir, encoding="utf-8") as json_file:
        event = json.load(json_file)
        return event
