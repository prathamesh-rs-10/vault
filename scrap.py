import argparse
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the companies and tables
companies = ['TCS', 'ITC', 'NTPC']
tables = ['tcs_data', 'itc_data', 'ntpc_data']

# Check if the number of companies and tables match
if len(companies) != len(tables):
    logging.error("Number of companies and tables must match")
    exit(1)

# Iterate over the companies and tables
for company, table in zip(companies, tables):
    # URL for the profit-loss data
    url = f'https://screener.in/company/{company}/consolidated/'

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
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

                # Log and print the cleaned and transposed DataFrame
                logging.info("Cleaned and transposed DataFrame with 'id' column:")
                print(df_table)

                # Load data to PostgreSQL
                db_host = "192.168.3.66"
                db_name = "postgres"
                db_user = "ps"
                db_password = "ps"
                db_port = "5432"
                engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

                # Set the column names
                df_table.columns = [
                    'id',
                    '0',
                    'Sales +',
                    'Expenses +',
                    'Operating Profit',
                    'OPM %',
                    'Other Income +',
                    'Interest',
                    'Depreciation',
                    'Profit before tax',
                    'Tax %',
                    'Net Profit +',
                    'EPS in Rs',
                    'Dividend Payout %'
                ]

                # Write the DataFrame to the database
                df_table.to_sql(table, engine, if_exists='replace', index=False)
                logging.info("Data loaded to PostgreSQL")

                # Use the existing PostgreSQL connection
                connection = engine.raw_connection()
                cursor = connection.cursor()

                # List the current columns in the table to verify names
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s;
                """, (table,))
                columns = cursor.fetchall()
                logging.info(f"Columns in '{table}' table:")
                for column in columns:
                    print(column)

                rename_queries = [
                    f"""ALTER TABLE {table} RENAME COLUMN "Sales +" TO sales;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "0" TO month;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Expenses +" TO expenses;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Operating Profit" TO operating_profit;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "OPM %" TO operating_profit_margin;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Other Income +" TO other_income;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Interest" TO interest;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Depreciation" TO depreciation;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Profit before tax" TO profit_before_tax;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Tax %" TO tax_rate;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Net Profit +" TO net_profit;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "EPS in Rs" TO earnings_per_share;""",
                    f"""ALTER TABLE {table} RENAME COLUMN "Dividend Payout %" TO dividend_payout_ratio;"""
                ]

                for query in rename_queries:
                    try:
                        cursor.execute(query)
                        connection.commit()
                        logging.info(f"Successfully executed query: {query}")
                    except Exception as e:
                        logging.error(f"Error with query: {query}\n{e}")

                # Close cursor and connection
                cursor.close()
                connection.close()
                logging.info("Data transformed and connections closed")

        else:
            logging.error("No data found at the given URL or no Profit-Loss section available")

    else:
        logging.error(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
