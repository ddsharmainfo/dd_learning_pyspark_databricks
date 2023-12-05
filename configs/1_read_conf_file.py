import os
import configparser

DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(DIR, './1_config.conf')

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

print('\n ===== Dev configs =====')
ENV_VAR = 'DEV'
param1 = config.get(ENV_VAR, "param1")
param2 = config.get(ENV_VAR, "param2")
print('Value of param1 is = ', param1)
print('Value of param2 is = ', param2)

print('\n ===== QA configs =====')
ENV_VAR = 'QA'
param1 = config.get(ENV_VAR, "param1")
param2 = config.get(ENV_VAR, "param2")
print('Value of param1 is = ', param1)
print('Value of param2 is = ', param2)

print('\n===== PROD configs =====')
ENV_VAR = 'PROD'
param1 = config.get(ENV_VAR, "param1")
param2 = config.get(ENV_VAR, "param2")
print('Value of param1 is = ', param1)
print('Value of param2 is = ', param2)