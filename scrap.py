import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
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

        for i in df_table.iloc[:, 1:].columns:
            df_table[i] = df_table[i].str.replace(',', '').str.replace('%', '/100').apply(eval)

        print(df_table)

        # Load data to Postgres
        db_host = "192.168.3.66"
        db_name = "postgres"
        db_user = "ps"
        db_password = "ps"
        db_port = "5432"
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        df_table.to_sql('profit_loss_data', engine, if_exists='replace', index=False)
        print("Data loaded to Postgres")

        # Use the existing PostgreSQL connection
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # Rename columns one by one
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
            cursor.execute(query)
            connection.commit()

        # Transform data in Postgres
        cursor.execute("""
            ALTER TABLE profit_loss_data
            ALTER COLUMN sales TYPE numeric,
            ALTER COLUMN expenses TYPE numeric,
            ALTER COLUMN operating_profit TYPE numeric,
            ALTER COLUMN other_income TYPE numeric,
            ALTER COLUMN interest TYPE numeric,
            ALTER COLUMN depreciation TYPE numeric,
            ALTER COLUMN profit_before_tax TYPE numeric,
            ALTER COLUMN net_profit TYPE numeric,
            ALTER COLUMN earnings_per_share TYPE numeric;

            ALTER TABLE profit_loss_data
            ALTER COLUMN operating_profit_margin TYPE decimal(4, 2),
            ALTER COLUMN tax_rate TYPE decimal(4, 2),
            ALTER COLUMN dividend_payout_ratio TYPE decimal(4, 2);
        """)
        connection.commit()

        # Add ID column
        cursor.execute("""
            ALTER TABLE profit_loss_data
            ADD COLUMN id SERIAL PRIMARY KEY;
        """)
        connection.commit()

        # Transpose data
        cursor.execute("""
            CREATE TABLE transposed_data AS
            SELECT 
                section, 
                MAX(CASE WHEN column_name = 'Mar_2013' THEN value::numeric END) AS Mar_2013,
                MAX(CASE WHEN column_name = 'Mar_2014' THEN value::numeric END) AS Mar_2014,
                MAX(CASE WHEN column_name = 'Mar_2015' THEN value::numeric END) AS Mar_2015,
                MAX(CASE WHEN column_name = 'Mar_2016' THEN value::numeric END) AS Mar_2016,
                MAX(CASE WHEN column_name = 'Mar_2017' THEN value::numeric END) AS Mar_2017,
                MAX(CASE WHEN column_name = 'Mar_2018' THEN value::numeric END) AS Mar_2018,
                MAX(CASE WHEN column_name = 'Mar_2019' THEN value::numeric END) AS Mar_2019,
                MAX(CASE WHEN column_name = 'Mar_2020' THEN value::numeric END) AS Mar_2020,
                MAX(CASE WHEN column_name = 'Mar_2021' THEN value::numeric END) AS Mar_2021,
                MAX(CASE WHEN column_name = 'Mar_2022' THEN value::numeric END) AS Mar_2022,
                MAX(CASE WHEN column_name = 'Mar_2023' THEN value::numeric END) AS Mar_2023
            FROM 
                profit_loss_data
            UNPIVOT 
                (value FOR column_name IN ("Mar_2013", "Mar_2014", "Mar_2015", "Mar_2016", "Mar_2017", "Mar_2018", "Mar_2019", "Mar_2020", "Mar_2021", "Mar_2022", "Mar_2023"))
            GROUP BY 
                section;
        """)
        connection.commit()

        # Close cursor and connection
        cursor.close()
        connection.close()

        print("Data transformed and transposed in Postgres")
