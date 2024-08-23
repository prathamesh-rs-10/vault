import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, inspect
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the company.csv file
try:
    companies_df = pd.read_csv('company.csv')
    logging.info("Successfully loaded company.csv file.")
except Exception as e:
    logging.error(f"Error loading company.csv file: {e}")
    raise

# Define the PostgreSQL database connection parameters
db_host = "192.168.3.66"
db_name = "postgres"
db_user = "ps"
db_password = "ps"
db_port = "5432"

try:
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    connection = engine.connect()
    logging.info("Successfully connected to the PostgreSQL database.")
except Exception as e:
    logging.error(f"Error connecting to the PostgreSQL database: {e}")
    raise

# Create a single table to store all data
table_name = 'all_company_data'
all_columns = set()  # To keep track of all possible columns

# Iterate over the companies
for index, row in companies_df.iterrows():
    company_symbol = row['Symbol']
    company_name = row['Company Name']
    logging.info(f"Processing data for {company_name} ({company_symbol})...")

    # URL for the profit-loss data
    url = f'https://screener.in/company/{company_symbol}/consolidated/'

    # Send a GET request to the URL
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        logging.info(f"Successfully retrieved data from {url}")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        continue
    except Exception as e:
        logging.error(f"An error occurred while requesting data from {url}: {e}")
        continue

    # Parse the HTML content using BeautifulSoup
    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        data = soup.find(id="profit-loss")
        if not data:
            logging.warning(f"No profit-loss section found for {company_name}")
            continue

        tdata = data.find("table")
        if not tdata:
            logging.warning(f"No table found in profit-loss section for {company_name}")
            continue
    except Exception as e:
        logging.error(f"Error parsing HTML content for {company_name}: {e}")
        continue

    # Process table data
    try:
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

        # Update all_columns with the current company's columns
        current_columns = set(df_table.columns)
        all_columns.update(current_columns)
        logging.info(f"Identified {len(current_columns)} columns for {company_name}: {current_columns}")

        # Add a new column for the company name
        df_table['company'] = company_name

        # Align with the full set of columns, filling missing columns with None
        df_table = df_table.reindex(columns=sorted(all_columns), fill_value=None)
    except Exception as e:
        logging.error(f"Error processing table data for {company_name}: {e}")
        continue

    # Insert the DataFrame into the PostgreSQL table
    try:
        df_table.to_sql(table_name, engine, if_exists='append', index=False)
        logging.info(f"Successfully inserted data for {company_name} into {table_name}")
    except Exception as e:
        logging.error(f"Error inserting data for {company_name} into the database: {e}")
        continue

    # Validate insertion by querying the table
    try:
        inspector = inspect(engine)
        table_columns = inspector.get_columns(table_name)
        table_column_names = [col['name'] for col in table_columns]
        logging.info(f"Current columns in {table_name}: {table_column_names}")

        result = connection.execute(f"SELECT COUNT(*) FROM {table_name} WHERE company = %s", (company_name,))
        row_count = result.fetchone()[0]
        logging.info(f"Inserted {row_count} rows for {company_name}.")
    except Exception as e:
        logging.error(f"Error validating data insertion for {company_name}: {e}")
        continue

    # Add delay to prevent server overload
    time.sleep(5)  # Adjust the sleep time as needed

# Close database connection
try:
    connection.close()
    logging.info("Database connection closed.")
except Exception as e:
    logging.error(f"Error closing the database connection: {e}")

# End of the script
logging.info("Script execution completed")
