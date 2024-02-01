# Import modules
import os
import base64
import argparse

import yaml
import dotenv
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras as extras


# Functions
def read_params(config_path):
    """
    read parameters from the params.yaml file
    input: params.yaml location
    output: parameters as dictionary
    """

    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    
    return config


def load_credentials(credentials_path):
    """
    Load environment variables to path
    input: Path to .env file
    output: Load ENV variables
    """

    # Load ENV variables
    dotenv_path = os.path.join(os.getcwd(), credentials_path)
    dotenv.load_dotenv(dotenv_path)


def encrypt_credentials(dwh_credentials, prefix, project_dir):
    """
    encrypt dwh credentials
    input: dwh credebtials
    output: encrypted dwh credebtials
    """

    # Load ENV variables
    load_credentials(project_dir + dwh_credentials['path'])

    # Encrypt
    dwh_credentials_encrypted = {}
    for e in dwh_credentials[f'{prefix.lower()}_env']:
        dwh_credentials_encrypted[e] = base64.b64encode(os.getenv(e).encode("utf-8"))
    
    print(dwh_credentials_encrypted)


def decrypt_credentials(dwh_credentials, prefix, project_dir):
    """
    decrypt dwh credentials
    input: encrypted dwh credebtials
    output: decrypted dwh credebtials
    """

    # Load ENV variables
    load_credentials(project_dir + dwh_credentials['path'])

    # Decrypt
    dwh_credentials_decrypted = {}
    for e in dwh_credentials[f'{prefix.lower()}_env']:
        dwh_credentials_decrypted[e] = base64.b64decode(os.getenv(e)).decode("utf-8")
    
    return dwh_credentials_decrypted


def db_connection(dwh_credentials, prefix, project_dir):
    """
    connect to on-premise dwh
    input: None
    output: connection string
    """

    # Decrypt credentials
    dwh_credentials_decrypted = decrypt_credentials(dwh_credentials, prefix, project_dir)
    host = dwh_credentials_decrypted[f'{prefix}_HOST']
    port = dwh_credentials_decrypted[f'{prefix}_PORT']
    dbname = dwh_credentials_decrypted[f'{prefix}_DB_NAME']
    user = dwh_credentials_decrypted[f'{prefix}_USER']
    password = dwh_credentials_decrypted[f'{prefix}_PASSWORD']
    
    # Connect to DB
    conn_str = f'postgresql+psycopg2://{user}:{password}@{host}/{dbname}'
    conn = create_engine(conn_str)

    # Logs
    print("Connection successful")
    
    return conn


def db_upload_connection(dwh_credentials, prefix, project_dir):
    """
    connect to on-premise dwh
    input: None
    output: connection string
    """

    # Decrypt credentials
    dwh_credentials_decrypted = decrypt_credentials(dwh_credentials, prefix, project_dir)
    host = dwh_credentials_decrypted[f'{prefix}_HOST']
    port = dwh_credentials_decrypted[f'{prefix}_PORT']
    dbname = dwh_credentials_decrypted[f'{prefix}_DB_NAME']
    user = dwh_credentials_decrypted[f'{prefix}_USER']
    password = dwh_credentials_decrypted[f'{prefix}_PASSWORD']

    # Connect to DB
    conn = psycopg2.connect(database=dbname, user=user, password=password, host=host, port=port)

    # Logs
    print("Connection successful")

    return conn


def mlflow_connection(mlflow_credentials, prefix, project_dir):
    # Decrypt credentials
    mlflow_credentials_decrypted = decrypt_credentials(mlflow_credentials, prefix, project_dir)
    user = mlflow_credentials_decrypted[f'{prefix}_USER']
    password = mlflow_credentials_decrypted[f'{prefix}_PASSWORD']

    # Set username and password when authentication was added
    os.environ['MLFLOW_TRACKING_USERNAME'] = user
    os.environ['MLFLOW_TRACKING_PASSWORD'] = password

    # Logs
    print("MLFlow connection credentials set up")
    # print(os.getenv('MLFLOW_TRACKING_USERNAME'))
    # print(os.getenv('MLFLOW_TRACKING_PASSWORD'))


# Run code
if __name__ == "__main__":
    # Parameter arguments
    args = argparse.ArgumentParser()
    args.add_argument("--config", default="params.yaml")
    parsed_args = args.parse_args()

    # Encrypt credentials
    encrypt_credentials(read_params(parsed_args.config)["db_credentials"], 'DWH', read_params(parsed_args.config)["project_dir"])

    # DB connection
    print(db_connection(read_params(parsed_args.config)["db_credentials"], 'DWH', read_params(parsed_args.config)["project_dir"]))

    # MLflow credentials set up
    mlflow_connection(read_params(parsed_args.config)["db_credentials"], "MLFLOW", read_params(parsed_args.config)["project_dir"])

    # DB upload connection
    print(db_upload_connection(read_params(parsed_args.config)["db_credentials"], 'DWH', read_params(parsed_args.config)["project_dir"]))