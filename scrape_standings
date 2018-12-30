from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import pandas as pd

## Retrieve tables from HTML
def find_and_split_rows(soup, html_object):
    ## Return all rows from webpage
    rows = soup.find_all(html_object)
    for i in range(len(rows)):
        ## Split string of row in to list of cells on commas
        rows[i] = str(rows[i].find_all('td')) \
            .split(',')

        ## Strip away html tags
        for j in range(len(rows[i])):
            rows[i][j] = BeautifulSoup(rows[i][j], 'lxml').get_text()

            ## Remove unwanted chararcters and whitespace
            rows[i][j] = re.sub(r'\n|\xa0|\[|\]', '', rows[i][j]) \
                .strip()
    return rows

## Initialise output dictionary of annual snapshots
df_dict = {}

for year in range(2011, 2018):
    year_str = str(year)
    ## Open hypertext file object
    url = 'http://www.heartlandchampionship.co.nz/Fixtures/Standings/{year}'.format(year=year_str)
    html = urlopen(url)

    ## Beautiful soup object
    soup = BeautifulSoup(html, 'lxml')

    ## Run UDF to pull out relevant cells
    rows = find_and_split_rows(soup, html_object='tr')

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

    ## Remove incomplete seasons
    #standings = standings[standings['Played']=='8']

    ## Add snapshot key column
    standings['snapshotKey'] = year_str

    ## Append to dictionary of DataFrames
    key = 'standings' + year_str
    df_dict[key] = standings
