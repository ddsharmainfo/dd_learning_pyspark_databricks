import json
import os


DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(DIR, './5_config_json.json')
ENVIRONMENT = 'Dev'


def read_config(file_path, env):
    with open(file_path, 'r') as file:
        config = json.load(file)
        key1 = config['key1']
        key2 = config['key2']
        key3 = config['key3']
    return key1, key2, key3


key1, key2, key3 = read_config(CONFIG_FILE, ENVIRONMENT)

print('Value for Key1: ', key1)
print('Value for Key2: ', key2)
print('Value for Key3: ', key3)