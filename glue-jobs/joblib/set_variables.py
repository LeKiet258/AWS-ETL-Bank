import os
import boto3
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
with open(os.path.join(__location__, "variables.properties")) as f:
    l = [line.split("=") for line in f.readlines()]
    variables = {key.strip(): value.strip() for key, value in l}
    for key, value in variables.items():
        os.environ[key] = value