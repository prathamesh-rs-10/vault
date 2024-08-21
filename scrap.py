import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from sqlalchemy import create_engine
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Web scraping
    url = 'https://screener.in/company/RELIANCE/consolidated/'
    logging.info(f"Fetching data from URL: {url}")
    webpage = requests.get(url)
    soup = bs(webpage.text, 'html.parser')
    data = soup.find('section', id="profit-loss")

    if data is not None:
        tdata = data.find("table")
        if tdata is not None:
            table_data = []
            for row in tdata.find_all('tr'):
                row_data = []
                for cell in row.find_all(['th', 'td']):
                    row_data.append(cell.text.strip())
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

            # Database connection details
            db_host = "192.168.3.66"
            db_name = "postgres"
            db_user = "ps"
            db_password = "ps"
            db_port = "5432"
            engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
            logging.info("Database connection established successfully.")

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
            df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
            logging.info("Data loaded to PostgreSQL successfully.")

            # Use the existing PostgreSQL connection
            connection = engine.raw_connection()
            cursor = connection.cursor()

            # Rename columns (if not done already)
            rename_queries = [
                """ALTER TABLE profit_loss_data RENAME COLUMN "Sales +" TO sales;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Period" TO month;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Expenses +" TO expenses;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Operating Profit" TO operating_profit;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "OPM %" TO operating_profit_margin;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Other Income +" TO other_income;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Interest" TO interest;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Depreciation" TO depreciation;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Profit before tax" TO profit_before_tax;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Tax %" TO tax_rate;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Net Profit +" TO net_profit;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "EPS in Rs" TO earnings_per_share;""",
                """ALTER TABLE profit_loss_data RENAME COLUMN "Dividend Payout %" TO dividend_payout_ratio;"""
            ]

            for query in rename_queries:
                try:
                    cursor.execute(query)
                    connection.commit()
                    logging.info(f"Successfully executed query: {query}")
                except Exception as e:
                    logging.error(f"Error executing query: {query}\n{e}")
                    connection.rollback()

            # Apply data type conversions with comma removal
            transformation_queries = [
                """ALTER TABLE profit_loss_data ALTER COLUMN month TYPE VARCHAR;""",  # Convert 'month' to string
                """ALTER TABLE profit_loss_data ALTER COLUMN tax_rate TYPE FLOAT USING (REGEXP_REPLACE(tax_rate, ',', '', 'g')::FLOAT);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN dividend_payout_ratio TYPE FLOAT USING (REGEXP_REPLACE(dividend_payout_ratio, ',', '', 'g')::FLOAT);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN earnings_per_share TYPE FLOAT USING (REGEXP_REPLACE(earnings_per_share, ',', '', 'g')::FLOAT);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN operating_profit_margin TYPE FLOAT USING (REGEXP_REPLACE(operating_profit_margin, ',', '', 'g')::FLOAT);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN sales TYPE INTEGER USING (REGEXP_REPLACE(sales, ',', '', 'g')::INTEGER);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN expenses TYPE INTEGER USING (REGEXP_REPLACE(expenses, ',', '', 'g')::INTEGER);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN operating_profit TYPE INTEGER USING (REGEXP_REPLACE(operating_profit, ',', '', 'g')::INTEGER);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN other_income TYPE INTEGER USING (REGEXP_REPLACE(other_income, ',', '', 'g')::INTEGER);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN interest TYPE INTEGER USING (REGEXP_REPLACE(interest, ',', '', 'g')::INTEGER);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN depreciation TYPE INTEGER USING (REGEXP_REPLACE(depreciation, ',', '', 'g')::INTEGER);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN profit_before_tax TYPE INTEGER USING (REGEXP_REPLACE(profit_before_tax, ',', '', 'g')::INTEGER);""",
                """ALTER TABLE profit_loss_data ALTER COLUMN net_profit TYPE INTEGER USING (REGEXP_REPLACE(net_profit, ',', '', 'g')::INTEGER);"""
            ]

            for query in transformation_queries:
                try:
                    cursor.execute(query)
                    connection.commit()
                    logging.info(f"Successfully executed transformation query: {query}")
                except Exception as e:
                    logging.error(f"Error executing transformation query: {query}\n{e}")
                    connection.rollback()

        else:
            logging.error("Failed to find table data in the profit-loss section.")
    else:
        logging.error("Failed to find profit-loss section in the webpage.")

except Exception as e:
    logging.error(f"An error occurred: {e}")

finally:
    # Close the database connection
    try:
        cursor.close()
        connection.close()
        logging.info("Database connection closed.")
    except Exception as e:
        logging.error(f"Error closing database connection: {e}")
