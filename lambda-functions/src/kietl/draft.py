import json
import os
    
if __name__ == '__main__':
    config_file_path = 'src/kietl/pg_to_s3_config.json'
    map_config = None

    with open(config_file_path, 'r') as file:
        map_config = json.load(file)

    print(map_config['pg'])