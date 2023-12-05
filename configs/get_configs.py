import os
import configparser


DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(DIR, './configs.conf')

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

db_connect = config.get('IDE', "db_connect")
print('Param db_connect value:', db_connect)
