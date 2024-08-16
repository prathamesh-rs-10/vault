import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
 
url = 'https://screener.in/company/RELIANCE/consolidated/'
webpage = requests.get(url)
soup = bs(webpage.text,'html.parser')
 
data = soup.find('section', id="profit-loss")
tdata= data.find("table")
 
table_data = []
 
for row in tdata.find_all('tr'):
    row_data = []
    for cell in row.find_all(['th','td']):
        row_data.append(cell.text.strip())
    table_data.append(row_data)
 
 
df_table = pd.DataFrame(table_data)
df_table.iloc[0,0] = 'Section'
df_table.columns = df_table.iloc[0]
df_table = df_table.iloc[1:,:-2]
for i in df_table.iloc[:,1:].columns:
    df_table[i] = df_table[i].str.replace(',','').str.replace('%','/100').apply(eval)
# df_table = df_table.set_index('')
print(df_table)
