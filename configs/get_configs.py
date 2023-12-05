import configparser

config = configparser.ConfigParser()
config.read('./configs.conf')

db_connect = config.get('IDE', "db_connect")
print('Param db_connect value:', db_connect)
