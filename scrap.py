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

        # Load data to Postgres
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
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        logging.info("Data loaded to PostgreSQL")

        # Use the existing PostgreSQL connection
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # Rename columns one by one with error handling
        rename_queries = [
            """ALTER TABLE profit_loss_data RENAME COLUMN "Sales +" TO sales;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "0" TO month;""",
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

        # Apply the correct data types to each column
        df_table['month'] = df_table['month'].astype(str)  # Convert 'month' to string

        # Convert columns to their respective data types, removing commas only if they exist
        df_table['tax_rate'] = df_table['tax_rate'].apply(lambda x: float(x.replace(',', '').replace('%', '/100')) if ',' in x else float(x.replace('%', '/100').eval()) if '%' in x else float(x))
        df_table['dividend_payout_ratio'] = df_table['dividend_payout_ratio'].apply(lambda x: float(x.replace(',', '').replace('%', '/100')) if ',' in x else float(x.replace('%', '/100').eval()) if '%' in x else float(x))
        df_table['earnings_per_share'] = df_table['earnings_per_share'].apply(lambda x: float(x.replace(',', '')) if ',' in x else float(x))
        df_table['operating_profit_margin'] = df_table['operating_profit_margin'].apply(lambda x: float(x.replace(',', '').replace('%', '/100')) if ',' in x else float(x.replace('%', '/100').eval()) if '%' in x else float(x))

        # Convert all other columns (excluding 'id' and 'month') to integer
        int_columns = ['sales', 'expenses', 'operating_profit', 'other_income', 
                       'interest', 'depreciation', 'profit_before_tax', 'net_profit']
        for col in int_columns:
            df_table[col] = df_table[col].apply(lambda x: int(x.replace(',', '')) if ',' in x else int(x))

        # Log and print the cleaned and transposed DataFrame
        logging.info("Cleaned and transposed DataFrame with 'id' column:")
        print(df_table)

        # Close cursor and connection
        cursor.close()
        connection.close()
        logging.info("Data transformed and connections closed")

else:
    logging.error("No data found at the given URL or no Profit-Loss section available")
