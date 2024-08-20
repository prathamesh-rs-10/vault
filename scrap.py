import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Web scraping
url = 'https://www.screener.in/company/RELIANCE/consolidated/'
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

        # Generate dates
        start_date = datetime.strptime('Mar 2013', '%b %Y')
        dates = [(start_date + timedelta(days=i*365)).strftime('%b %Y') for i in range(len(table_data) - 1)]

        df_table = pd.DataFrame(table_data[1:], columns=['Sales +', 'Expenses +', 'Operating Profit', 'OPM %', 'Other Income +', 'Interest', 'Depreciation', 'Profit before tax', 'Tax %', 'Net Profit +', 'EPS in Rs', 'Dividend Payout %'])
        df_table['Date'] = dates
        df_table['id'] = range(1, len(df_table) + 1)
        df_table = df_table[['id', 'Date', 'Sales +', 'Expenses +', 'Operating Profit', 'OPM %', 'Other Income +', 'Interest', 'Depreciation', 'Profit before tax', 'Tax %', 'Net Profit +', 'EPS in Rs', 'Dividend Payout %']]

        # Load data to Postgres
        db_host = "192.168.3.66"
        db_name = "postgres"
        db_user = "ps"
        db_password = "ps"
        db_port = "5432"
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        print("Data loaded to Postgres")
