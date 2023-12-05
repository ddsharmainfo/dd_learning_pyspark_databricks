import yaml
import os

DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(DIR, './3_config_yaml.yaml')
ENVIRONMENT = 'dev'


def read_config(file_path, env):
    with open(file_path, 'r') as file:
        config_data = yaml.safe_load(file)
    return config_data.get(env, {})


config_for_environment = read_config(CONFIG_FILE, ENVIRONMENT)

# Print the config data
print(f"Config for {ENVIRONMENT} environment:")
print(config_for_environment)

# Access specific values
database_host = config_for_environment.get('database', {}).get('host', 'N/A')
api_key = config_for_environment.get('api_key', 'N/A')

print("\nAccessing Values:")
print(f"Database Host: {database_host}")
print(f"API Key: {api_key}")
