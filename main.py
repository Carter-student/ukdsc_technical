import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from ruamel.yaml import YAML
import os
import string

import shutil


class DataPrep:
    def __init__(self, use_cache=False):
        self.use_cache = use_cache
        self.project_root = Path(__file__).parent

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
        cache_location = self.project_root.joinpath('cache', table_name + '_data.csv')
        if self.use_cache:
            if os.path.exists(self.project_root.joinpath(cache_location)):
                return pd.read_csv(cache_location)
            
        with self.engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name}"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        df.to_csv(cache_location, index=False)
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
        self.perform_part2_tasks(customers_df.copy(), sales_df.copy()) # Using a copy to gurantee no changes in the cleaned data
        self.perform_part3_tasks(customers_df, sales_df)

    def prepare_output_dir(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)
            
    def save_task(self, df, task_name, subfolder):
        path_to_subfolder = self.output_dir.joinpath(subfolder)
        if not os.path.exists(path_to_subfolder):
            os.makedirs(path_to_subfolder)
        df.to_csv(self.output_dir.joinpath(subfolder, task_name + '.csv'), index=False)


    def perform_part2_tasks(self, customers_df, sales_df, subfolder='part_2'):
        # Task 1
        number_of_cutomers =  len(customers_df['customer_id'].unique())
        print('There are', number_of_cutomers, 'unique customers')
        self.save_task(
            pd.DataFrame({'There Are This many unique customers': [number_of_cutomers]}),
            '1_number_unique_customers',
            subfolder
            )
       
       # Task 2
        sorted_customers = customers_df.sort_values(['age'], ascending=True)
        
        print(sorted_customers)
        self.save_task(sorted_customers, '2_sorted_customer', subfolder)
        
        # Task 3
        spend_by_customer = self.get_total_spend_by_customer(customers_df, sales_df)
        max_customer_row = pd.DataFrame([spend_by_customer.loc[spend_by_customer['total_spend'].idxmax(), :]]).reset_index(drop=True)
        max_customer_row.columns = list(customers_df.columns) + ['total_spend'] 

        self.save_task(spend_by_customer, 'customer_spend', 'part3')
        self.save_task(max_customer_row, '3_max_customer', subfolder)
        print(max_customer_row)

        # Task 4
        sum_sales_product = self.get_sales_per_category(sales_df)
        max_row = sum_sales_product.loc[sum_sales_product['total_spend'].idxmax(), :]
        self.save_task(sum_sales_product, 'sales_per_category', 'part3')
        self.save_task(max_row, '4_max_product', subfolder)
        print(max_row)
        
        # Task 5
        transactions_by_date = sales_df.sort_values('sale_date', ascending=False)
        self.save_task(transactions_by_date, '5_transactions_by_date', subfolder)
        self.save_task(transactions_by_date, 'transactions_sorted_by_date', 'part3')

        print(transactions_by_date)

    def get_total_spend_by_customer(self, customers_df, sales_df):
        spend_by_customer = customers_df.merge(
            sales_df, how='left', on='customer_id'
            ).groupby(
                ['customer_id', 'customer_name', 'age', 'region'],
                as_index=False
                ).agg(total_spend=('sale_amount', 'sum'))
        
        spend_by_customer['total_spend'] = spend_by_customer['total_spend'].round(2)

        if spend_by_customer.shape[0] != len(customers_df['customer_id'].unique()):
            raise ValueError('Number of customer_ids and customer_names are not equal'
                             'There may be a misspelling in the customer_name.')
        
        return spend_by_customer
    
    def get_sales_per_category(self, sales_df):
        sales_per_category = sales_df.groupby('product_category', as_index=False).agg(total_spend=('sale_amount','sum'))
        sales_per_category['total_spend'] = sales_per_category['total_spend'].round(2)
        return sales_per_category
    
    def perform_part3_tasks(self, customers_df, sales_df, subfolder='part3'):
        self.save_task(customers_df, 'cleaned_customers', subfolder)
        self.save_task(sales_df, 'cleaned_sales', subfolder)
        
if __name__ == '__main__':
    loaded_data = DataPrep(use_cache=True)
    TransformSave(customers_df=loaded_data.customers_df, sales_df=loaded_data.sales_df)