import pandas as pd
from sqlalchemy import create_engine, inspect, text
import os
import re

# Fetching environment variables
snow_user = os.getenv('snowflake_user')
snow_pass = os.getenv('snowflake_pass')
snow_wh = os.getenv('snowflake_wh')
snow_role = os.getenv('snowflake_role')
snow_account = os.getenv('snowflake_account')

# Define the Snowflake connection URL
connection_url = (
    f"snowflake://{snow_user}:{snow_pass}@{snow_account}/AUTOHECK/RAW"
    f"?warehouse={snow_wh}&role={snow_role}"
)

print('This is my snowflake con')
print(connection_url)

# Create the SQLAlchemy engine
engine = create_engine(connection_url)

inspector = inspect(engine)

os.chdir("./Data")

autocheck_data = os.listdir() # This assigns the list data objects available in your directory to the variable on the left
existing_tables = inspector.get_table_names()
for data in autocheck_data: # loops through each data objects (CSVs)
    with engine.connect() as connection:
        connection.execute(text(f"USE DATABASE AUTOCHECK"))
        if data[:-4] not in existing_tables:
            df = pd.read_csv(data) # reads the data as pandas dataframe
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            print(df.head(2)) # prints the first 2 rows of the data
            print(f'Loading {data[:-4]} into snowflake') # prints a log to the console to show what the program is doing at the moment
            """
                The code below loads each of the data object to the specified location as specified in your configuration
            """
            df.to_sql(f'{data[:-4]}', \
                con=connection, \
                schema = 'RAW', \
                if_exists = 'replace', \
                index = False, method='multi')
        else:
            df = pd.read_csv(data) # reads the data as pandas dataframe
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            print(df.head(2)) # prints the first 2 rows of the data
            print(f'Loading temp_{data[:-4]} into snowflake') # prints a log to the console to show what the program is doing at the moment
            df.to_sql(f'temp_{data[:-4]}', \
                con=connection, \
                schema = 'RAW', \
                if_exists = 'append', \
                index = False, method='multi')
            print(f'Loaded temp_{data[:-4]} object to the db')
            
            if data[:-4].lower() == 'borrower_data':
                target_id = 'borrower_id'
            elif data[:-4].lower() == 'loan_data':
                target_id = 'loan_id'
            elif data[:-4].lower() == 'repayment_data':
                target_id = 'payment_id'
            elif data[:-4].lower() == 'schedule_data':
                target_id = 'schedule_id'
            cols = []
            for col in df.columns:
                cols.append('source.'+col)
                
            src_cols = ', '.join(cols)
            columns = ', '.join(df.columns)
            merge_sql = f"""
                        MERGE INTO RAW.{data[:-4]} AS target
                        USING RAW.temp_{data[:-4]} AS source
                        ON target.{target_id} = source.{target_id}
                        WHEN NOT MATCHED THEN
                            INSERT ({columns}) VALUES ({src_cols});
                        """
            connection.execute(text(merge_sql))
            print(f'Merged temp_{data[:-4]} object into {data[:-4]} object')

            connection.execute(text(f"DROP TABLE IF EXISTS RAW.temp_{data[:-4]}"))

    connection.close()
print('Loaded all data objects to the db')