import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from ruamel.yaml import YAML
import os

class DataLoad:
    def __init__(self):
        self.connection_config = self.get_config()

        self.engine = create_engine(
            f'postgresql+psycopg2://'
            f'{self.connection_config["username"]}:{self.connection_config["password"]}'
            f'@{self.connection_config["host"]}:{self.connection_config["port"]}'
            f'/{self.connection_config["dbname"]}'
            )
        
        self.connect_to_db()

    def get_config(self):
        config_path = Path(__file__).parent.joinpath('config', 'config.yaml')

        if not os.path.exists(config_path):
            raise FileExistsError('You need a config.yaml file to exist in the config folder.'
                                  'It has the key host, port, dbname, username, and password'
                                   ' keys and then their values')
        
        yaml = YAML(typ='safe', pure=True)

        with open(str(config_path), 'r') as file:
            config = yaml.load(file)

        return config
    
    def connect_to_db(self):
        with self.engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM Customers"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

             
        print("Number of rows in Customers table:\n", df.to_string())

        with self.engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM Sales"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

             
        print("Number of rows in Salses table:\n", df.to_string())
        
if __name__ == '__main__':
    dl = DataLoad()
    