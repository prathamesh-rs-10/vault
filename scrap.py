import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from sqlalchemy import create_engine
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

        # Identify and clean numeric data
        for col in df_table.columns[2:]:  # Skip 'id' and 'Period' columns
            if df_table[col].str.isnumeric().all():
                df_table[col] = df_table[col].str.replace(',', '').apply(pd.to_numeric, errors='coerce')
            elif '%' in df_table[col].astype(str).iloc[0]:  # Check if '%' is present
                df_table[col] = df_table[col].str.replace(',', '').str.replace('%', '/100').apply(eval)

        # Log and print the cleaned and transposed DataFrame
        logging.info("Cleaned and transposed DataFrame with 'id' column:")
        print(df_table)

        # Load data to Postgres
        db_host = "192.168.3.66"
        db_name = "postgres"
        db_user = "ps"
        db_password = "ps"
        db_port = "5432"
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        logging.info("Data loaded to PostgreSQL")

        # Use the existing PostgreSQL connection
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # List the current columns in the table to verify names
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'profit_loss_data';
        """)
        columns = cursor.fetchall()
        logging.info("Columns in 'profit_loss_data' table:")
        for column in columns:
            print(column)

        # Rename columns one by one with error handling
        rename_queries = [
            """ALTER TABLE profit_loss_data RENAME COLUMN "Sales +" TO sales;""",
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
                logging.error(f"Error with query: {query}\n{e}")

        # Close cursor and connection
        cursor.close()
        connection.close()
        logging.info("Data transformed and connections closed")

else:
    logging.error("No data found at the given URL or no Profit-Loss section available")
