import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Web scraping
url = 'https://screener.in/company/RELIANCE/consolidated/'
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

        df_table = pd.DataFrame(table_data)
        df_table.iloc[0, 0] = 'Section'
        df_table.columns = df_table.iloc[0]
        df_table = df_table.iloc[1:, :-2]

        # Load data to Postgres
        db_host = "192.168.3.66"
        db_name = "postgres"
        db_user = "ps"
        db_password = "ps"
        db_port = "5432"
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        print("Data loaded to Postgres")

        # Apply transformations using SQL commands
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        cur = conn.cursor()

        # Remove commas and convert percentage columns to decimal
        cur.execute("""
            UPDATE profit_loss_data
            SET "Operating Profit" = REPLACE("Operating Profit", ',', ''),
                "OPM %" = REPLACE("OPM %", ',', '') || '/100',
                "Net Profit %" = REPLACE("Net Profit %", ',', '') || '/100',
                "EPS in Rs" = REPLACE("EPS in Rs", ',', ''),
                "Dividend Payout %" = REPLACE("Dividend Payout %", ',', '') || '/100';
        """)
        conn.commit()

        # Convert string columns to numeric
        cur.execute("""
            ALTER TABLE profit_loss_data
            ALTER COLUMN "Operating Profit" TYPE numeric,
            ALTER COLUMN "OPM %" TYPE numeric,
            ALTER COLUMN "Net Profit %" TYPE numeric,
            ALTER COLUMN "EPS in Rs" TYPE numeric,
            ALTER COLUMN "Dividend Payout %" TYPE numeric;
        """)
        conn.commit()

        cur.close()
        conn.close()

        print("Transformations applied")
