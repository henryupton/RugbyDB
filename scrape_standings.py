from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import pandas as pd
import rugbydb.dbtools as dbt
import rugbydb.scrapingtools as st

## Initialise output dictionary of annual snapshots
df_dict = {}

for year in range(2014, 2018):
    year_str = str(year)
    ## Open hypertext file object
    url = 'http://www.heartlandchampionship.co.nz/Fixtures/Standings/{year}'.format(year=year_str)
    html = urlopen(url)

    ## Beautiful soup object
    soup = BeautifulSoup(html, 'lxml')

    ## Run UDF to pull out relevant cells
    rows = st.find_and_split_rows(soup, html_object='tr')

    ## Leave out the nonsense
    default_teams = ['Mid Canterbury', 'South Canterbury', 'Wanganui', 'Thames Valley', 'Wairarapa Bush', 'Horowhenua-Kapiti', \
             'Buller', 'King Country', 'North Otago', 'West Coast', 'Poverty Bay', 'East Coast']
    rows = [x for x in rows if x[0] in default_teams]

    ## Find headers for the table
    headers = soup.find_all('th')

    for i in range(len(headers)):
        ## Strip html
        headers[i] = BeautifulSoup(str(headers[i]), 'lxml').get_text()
        ## Remove unwanted chararcters
        headers[i] = re.sub(r'\n|\xa0|\[|\]', '', headers[i])

    ## Clean off unrelated headers and account for possible missing columns in a year
    default_headers = ['Team', 'Played', 'Win', 'Draw', 'Loss', 'For', 'Against', 'BP1', 'BP2', 'Points']
    headers = [x for x in headers if x in default_headers]

    ## Convert to pandas DF
    standings = pd.DataFrame(rows)

    ## Use new column names
    standings.columns = headers

    ## Add snapshot key column
    standings['snapshot_key'] = year_str

    ## Append to dictionary of DataFrames
    key = 'standings_' + year_str
    df_dict[key] = standings

## Commit to db
for df in df_dict.values():
    dbt.pandas_to_postgres(df, table_name="web_data.l_www_we_standings_hist")
