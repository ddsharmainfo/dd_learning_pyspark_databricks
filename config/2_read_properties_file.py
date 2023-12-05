import os
import configparser

DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(DIR, './2_config.properties')

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
param3 = config.get(ENV_VAR, "param3")
param4 = config.get(ENV_VAR, "param4")
print('Value of param3 is = ', param3)
print('Value of param4 is = ', param4)

print('\n===== PROD configs =====')
ENV_VAR = 'PROD'
param5 = config.get(ENV_VAR, "param5")
param6 = config.get(ENV_VAR, "param6")
print('Value of param5 is = ', param5)
print('Value of param6 is = ', param6)