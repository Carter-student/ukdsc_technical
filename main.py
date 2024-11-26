import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from ruamel.yaml import YAML
import os
import string

import shutil


class DataPrep:
    def __init__(self):
        self.connection_config = self.get_config('config.yaml')
        self.datatype_config = self.get_config('data_types.yaml')

        self.engine = create_engine(
            f'postgresql+psycopg2://'
            f'{self.connection_config["username"]}:{self.connection_config["password"]}'
            f'@{self.connection_config["host"]}:{self.connection_config["port"]}'
            f'/{self.connection_config["dbname"]}'
            )
        
        self.customers_df = self.get_all_data_table('customers')
        self.sales_df = self.get_all_data_table('sales')

        self.customers_df = self.clean_data(self.customers_df, 'customers')
        self.sales_df = self.clean_data(self.sales_df, 'sales')


    def get_config(self, config_name):
        config_path = Path(__file__).parent.joinpath('config', config_name)

        if not os.path.exists(config_path):
            raise FileExistsError('You need a config.yaml file to exist in the config folder.'
                                  'It has the key host, port, dbname, username, and password'
                                   ' keys and then their values')
        
        yaml = YAML(typ='safe', pure=True)

        with open(str(config_path), 'r') as file:
            config = yaml.load(file)

        return config
    
    def get_all_data_table(self, table_name):
        with self.engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name}"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df 
    
    def clean_data(self, df, table_name):
        for col in df.columns:
            col_astype = self.datatype_config[table_name][col]
            
            df[col] = df[col].astype(col_astype)
            if col_astype == 'str':
                # string columns often contain . and other punctuation as well as
                # sporadic capitalisation
                df[col] = df[col].str.replace(f'[{string.punctuation}]', '', regex=True).str.lower()

        pd.testing.assert_frame_equal(df.drop_duplicates(), df)
        return df


        
class TransformSave:
    def __init__(self, customers_df, sales_df ):
        self.root_dir = Path(__file__).parent
        self.output_dir = self.root_dir.joinpath('outputs')
        self.prepare_output_dir()
        self.perform_tasks(customers_df, sales_df)

    def prepare_output_dir(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)
            
    def perform_tasks(self, customers_df, sales_df):

        print('There are', len(customers_df['customer_id'].unique()), 'unique customers')

        sorted_customers = customers_df.sort_values(['age'], ascending=True)
        sorted_customers.to_csv(
            self.output_dir.joinpath('sorted_customers.csv'), index=False
            )
        
        print(sorted_customers)
        
        sum_sales = sales_df.groupby('customer_id', as_index=False).agg(
            total_sale_amount=('sale_amount', 'sum')
            )
        
        max_row = sum_sales.loc[sum_sales['total_sale_amount'].idxmax(), :]
        max_customer = customers_df.loc[
            customers_df['customer_id'] == max_row['customer_id']
            ].reset_index(drop=True)
        
        max_customer.to_csv(self.output_dir.joinpath('max_customer.csv'), index=False)
        print(max_customer)

        sum_sales_product = sales_df.groupby('product_category', as_index=False).agg(
            total_sale_amount=('sale_amount', 'sum')
            )
        max_row = sum_sales_product.loc[sum_sales_product['total_sale_amount'].idxmax(), :]
        max_row.to_csv(self.output_dir.joinpath('max_product.csv'), index=False)
        print(max_row)
        
        transactions_by_date = sales_df.sort_values('sale_date', ascending=False)
        transactions_by_date.to_csv(self.output_dir.joinpath('transactions_by_date.csv'), index=False)
        print(transactions_by_date)

if __name__ == '__main__':
    loaded_data = DataPrep()
    TransformSave(customers_df=loaded_data.customers_df, sales_df=loaded_data.sales_df)