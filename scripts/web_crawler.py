from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
#from apscheduler.schedulers.blocking import BlockingScheduler
import os
from getpass import getpass as gp
import json
from sqlalchemy import create_engine, inspect, text
import pandas as pd
import time
from datetime import date
import random

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

# Create the SQLAlchemy engine for snowflake connection string
engine = create_engine(connection_url)

inspector = inspect(engine)

existing_tables = inspector.get_table_names()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
]

# Define function to create Selenium driver
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run headless for faster scraping
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def scrape_cars45():
    base_url = 'https://www.cars45.com/listing'
    driver = create_driver()
    driver.get(base_url)
    ## Read number of results from page
    num_res = driver.find_elements(By.XPATH, "//div[@class='pagination__summary']/p")
    num_listings = int(num_res[0].text.split(' of ')[1].split(' results')[0].replace(' ', ''))
    pages = round(num_listings/15) 
    
    df_list = []
    df_log_list = []
    
    page = 1
    while page <= pages:
        print(f'currently on page: {page}')
        page_url = f'https://www.cars45.com/listing?page={page}'

        driver.get(page_url)

        car_img = []
        # Find the <img> element within the <div> by its class name
        img_elements = driver.find_elements(By.CSS_SELECTOR, 'div.car-feature__image img')

        # Extract the src attribute
        for img in img_elements:
            car_img.append(img.get_attribute('src'))

        div = driver.find_elements(By.CLASS_NAME, 'car-feature__details')
        div2 = driver.find_elements(By.XPATH, "//div[@class='car-feature__details']//div[@class='car-feature__others']")

        for details in div:
            # Find all p elements within that div
            paragraphs_amount = details.find_elements(By.XPATH, "//p[@class='car-feature__amount']")
            paragraphs_name = details.find_elements(By.XPATH, "//p[@class='car-feature__name']")
            paragraphs_region = details.find_elements(By.XPATH, "//p[@class='car-feature__region']")

            car_amount = [p.text for p in paragraphs_amount]
            car_name = [p.text for p in paragraphs_name]
            car_region = [p.text for p in paragraphs_region]

        other_details = []
        for details in div2:
            # Find all p elements within that div
            paragraphs = details.find_elements(By.TAG_NAME, 'span')

            # Print the text of each p element
            for p in paragraphs:
                other_details.append(p.text)

        car_type = []
        misc = []
        i = 0
        for val in other_details:
            if i % 2 == 0:
                car_type.append(val)
            if i % 2 != 0:
                misc.append(val)
            i+=1
        
        if len(car_img)==len(car_name)==len(car_amount)==len(car_region)==len(car_type)==len(misc):
            df = pd.DataFrame({'car_img': car_img, 'car_brand': car_name, 'car_amount': car_amount, 'car_region': car_region, 'car_type': car_type, 'misc': misc})
            print(df.head(1))
            df_list.append(df)
        else:
            # Store data to an error log csv if number of rows extracted within the page does not match for all attributes
            data = {'page': page, 'car_img': car_img, 'car_brand': car_name, 'car_amount': car_amount, 
                    'car_region': car_region, 'car_type': car_type, 'misc': misc}
            
            # Convert dictionary to JSON string
            json_string = json.dumps(data, indent=4)  # `indent` is optional and formats the JSON output
            #test_df = pd.DataFrame({'page': 1, 'data': json_string})
            df_log = pd.DataFrame([data], index=[page])
            print(f'Appending df log for page: {page}')
            df_log_list.append(df_log)

        ## Load everey ten pages of web extracts to destination table
        if page % 10 == 0 and len(df_list) > 0:
            combined_df = pd.concat([dfs for dfs in df_list ])
            print(f'Shape of dataframe to be saved: {combined_df.shape}')
            df_list = []
            connection = engine.connect()

            ## Load to destination table if not exists
            if 'car_listings' not in existing_tables:
                
                connection.execute(text("USE DATABASE AUTOCHECK"))
                connection.execute(text("BEGIN TRANSACTION"))
                combined_df.to_sql('car_listings', \
                    con=connection, \
                    schema = 'CAR_LISTING', \
                    if_exists = 'append', \
                    index = False, method='multi')
                connection.execute(text("COMMIT"))
                connection.close()
            else:

                ## Load a temp table to snowflake
                connection.execute(text("USE DATABASE AUTOCHECK"))
                connection.execute(text("BEGIN TRANSACTION"))
                df.to_sql(f'tmp_car_listings', \
                    con=connection, \
                    schema = 'CAR_LISTING', \
                    if_exists = 'replace', \
                    index = False, method='multi')
                connection.execute(text("COMMIT"))
                print(f'Loaded tmp_car_listings object to the db')

                ## Perform a merge using the temp table to insert new records to destination table
                merge_sql = f"""
                    MERGE INTO CAR_LISTING.car_listings AS target
                    USING CAR_LISTING.tmp_car_listings AS src
                    ON target.car_img = src.car_img
                    WHEN NOT MATCHED THEN
                        INSERT (car_img, car_brand, car_amount, car_region, car_type, misc) VALUES (src.car_img, src.car_brand, src.car_amount, src.car_region, src.car_type, src.misc);
                    """
                connection.execute(text("BEGIN TRANSACTION"))
                connection.execute(text(merge_sql))
                connection.execute(text("COMMIT"))
                connection.close()
                print(f'Merged tmp_car_listings object into car_listings object')

        ## Load web extract from last page to db
        if (pages - page) < 10 and (page % 10) != 0 and len(df_list) > 0:
            combined_df = pd.concat([dfs for dfs in df_list ])
            print(f'Shape of dataframe to be saved: {combined_df.shape}')
            df_list = []
            connection = engine.connect()

            ## Load to destination table if not exists
            if 'car_listings' not in existing_tables:
                
                connection.execute(text("USE DATABASE AUTOCHECK"))
                connection.execute(text("BEGIN TRANSACTION"))
                combined_df.to_sql('car_listings', \
                    con=connection, \
                    schema = 'CAR_LISTING', \
                    if_exists = 'append', \
                    index = False, method='multi')
                connection.execute(text("COMMIT"))
                connection.close()
            else:
                ## Load a temp table to snowflake
                connection.execute(text("USE DATABASE AUTOCHECK"))
                connection.execute(text("BEGIN TRANSACTION"))
                df.to_sql(f'tmp_car_listings', \
                    con=connection, \
                    schema = 'CAR_LISTING', \
                    if_exists = 'replace', \
                    index = False, method='multi')
                connection.execute(text("COMMIT"))
                print(f'Loaded tmp_car_listings object to the db')

                ## Perform a merge using the temp table to insert new records to destination table
                merge_sql = f"""
                    MERGE INTO CAR_LISTING.car_listings AS target
                    USING CAR_LISTING.tmp_car_listings AS src
                    ON target.car_img = src.car_img
                    WHEN NOT MATCHED THEN
                        INSERT (car_img, car_brand, car_amount, car_region, car_type, misc) VALUES (src.car_img, src.car_brand, src.car_amount, src.car_region, src.car_type, src.misc);
                    """
                connection.execute(text("BEGIN TRANSACTION"))
                connection.execute(text(merge_sql))
                connection.execute(text("COMMIT"))
                print(f'Merged tmp_car_listings object into car_listings object')
            connection.close()
            
        # Wait for dynamic content to load
        time.sleep(random.uniform(5, 10))  # Random sleep to mimic human behavior

        page += 1
        
    today = date.today()
    if len(df_log_list) > 0:
        combined_df_logs = pd.concat([dfs for dfs in df_log_list ])
        combined_df_logs.to_csv(f'./Data/car_listing_logs_{today}.csv', index=False)

    driver.quit()


# Start the crawler
if __name__ == "__main__":
    scrape_cars45()