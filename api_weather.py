import urllib.request
import json
import pandas as pd
from rugbydb import dbtools as dbt


## List of cities to pull data from
city_lst = ['Whangarei'
            ,'Auckland'
            ,'Tauranga'
            ,'New Plymouth'
            ,'Hamilton'
            ,'Whanganui'
            ,'Thames'
            ,'Dunedin'
            ,'Gisborne'
            ,'Nelson'
            ,'Christchurch'
            ,'Paraparaumu']

## API key for access to openweathermap.org
api_key = 'c930f3a7c95bbc2e2bc0a01939785144'

## Select weather or forecast
type_flag = 'weather'

## Init output Dataframe
output_df = None

for city in city_lst:
    ## API address template
    api_str = 'http://api.openweathermap.org/data/2.5/{type}?q={city},nz&units=metric&APPID={api_key}'

    try:
        ## Pass city name and auth key to URL
        with urllib.request.urlopen(api_str.format(api_key=api_key, type=type_flag, city=city)) as response:
            json_str = str(response.read())[2:-1]

        ## Sense check that JSON structure is static
        #print(json_str)

    except (Exception, urllib.error.HTTPError) as error:
        print(error.code)
        print(error.read())

    ## Parse JSON as dict object
    json_load = json.loads(json_str)

    ## Extract relevant data from JSON
    dict = {'location': json_load['name']
            ,'country': json_load['sys']['country']
            ,'epoc_unix_time': json_load['dt']
            ,'sunrise': json_load['sys']['sunrise']
            ,'sunset': json_load['sys']['sunset']
            ,'longitude': json_load['coord']['lon']
            ,'latitude': json_load['coord']['lat']
            ,'wind_speed': json_load['wind']['speed']
            ,'wind_chill': json_load['wind']['deg']
            ,'description': json_load['weather'][0]['description']
            ,'temperature': json_load['main']['temp']
            ,'pressure': json_load['main']['pressure']
            ,'humidity': json_load['main']['humidity']
            ,'min_temp': json_load['main']['temp_min']
            ,'max_temp': json_load['main']['temp_max']}

    ## Convert dict to pandas_df
    load_df = pd.DataFrame(data=dict, index=[0])
    
    ## Check if its first iteration else union rows
    if output_df is None:
        output_df = load_df
    else:
        output_df = pd.concat([output_df, load_df])

## Export to postgreSQL
dbt.pandas_to_postgres(output_df, 'api_data.l_api_dl_weather')
