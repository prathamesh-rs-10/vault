import argparse
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the company.csv file
companies_df = pd.read_csv('company.csv')

# Define the PostgreSQL database connection parameters
db_host = "192.168.3.66"
db_name = "postgres"
db_user = "ps"
db_password = "ps"
db_port = "5432"
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Create a single table to store all data
table_name = 'all_company_data'

# Connect to the database
connection = engine.raw_connection()
cursor = connection.cursor()

# Iterate over the companies
for index, row in companies_df.iterrows():
    company_symbol = row['Symbol']
    company_name = row['Company Name']

    url = f'https://screener.in/company/{company_symbol}/consolidated/'
    delay = 1  # seconds
    max_retries = 5

    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            break
        except requests.exceptions.ConnectionError:
            logging.error(f"Attempt {attempt+1}/{max_retries} failed to connect to {url}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP Error {e.response.status_code} for {url}. Skipping...")
            break
        except Exception as e:
            logging.error(f"Error for {url}: {e}. Skipping...")
            break
    else:
        logging.error(f"Failed to connect to {url} after {max_retries} attempts. Skipping...")
        continue

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the profit-loss section and table
        data = soup.find(id="profit-loss")
        if data:
            tdata = data.find("table")
            if tdata:
                rows = tdata.find_all("tr")
                table_data = []
                for row in rows:
                    row_data = [cell.text.strip() for cell in row.find_all(["th", "td"])]
                    table_data.append(row_data)

                # Convert the scraped table data to a DataFrame
                df_table = pd.DataFrame(table_data)
                df_table.iloc[0, 0] = 'Section'
                df_table.columns = df_table.iloc[0]
                df_table = df_table.iloc[1:, :-2]

                # Transpose the DataFrame to have columns as periods and rows as metrics
                df_table = df_table.set_index('Section').transpose()

                # Reset index after transpose and add an 'id' column
                df_table.reset_index(inplace=True)
                df_table.rename(columns={'index': 'Period'}, inplace=True)
                df_table['id'] = range(1, len(df_table) + 1)

                # Rearrange columns to put 'id' at the beginning
                columns = ['id'] + [col for col in df_table.columns if col != 'id']
                df_table = df_table[columns]

                # Identify and clean numeric data
                for col in df_table.columns[2:]:  # Skip 'id' and 'Period' columns
                    if df_table[col].str.isnumeric().all():
                        df_table[col] = df_table[col].str.replace(',', '').apply(pd.to_numeric, errors='coerce')
                    elif '%' in df_table[col].astype(str).iloc[0]:  # Check if '%' is present
                        df_table[col] = df_table[col].str.replace(',', '').str.replace('%', '/100').apply(eval)

                # Add a new column for the company name
                df_table['company'] = company_name

                # Check if table exists, create if not
                cursor.execute("""
                    SELECT to_regclass(%s);
                """, (table_name,))
                if not cursor.fetchone()[0]:
                    cursor.execute("""
                        CREATE TABLE {} (
                            id SERIAL PRIMARY KEY,
                            "Period" text,
                            "company" text
                        );
                    """.format(table_name))
                    connection.commit()

                # Check if columns exist, create if not
                for col in df_table.columns:
                    cursor.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s AND column_name::text = %s;
                    """, (table_name, col))
                    if not cursor.fetchone():
                        cursor.execute("""
                            ALTER TABLE {} ADD COLUMN "{}" numeric;
                        """.format(table_name, col))
                    connection.commit()

                                # Insert data into the table
                df_table.to_sql(table_name, engine, if_exists='append', index=False)
                connection.commit()

                # Close cursor and connection
                cursor.close()
                connection.close()
                logging.info("Data loaded for {}".format(company_name))

                time.sleep(2)

        else:
            logging.error("No data found at the given URL or no Profit-Loss section available")

    else:
        logging.error("Failed to retrieve data from {}. Status code: {}".format(url, response.status_code))

# End of the script
logging.info("Script execution completed")
