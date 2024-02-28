import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import config

def player_by_season():

    df = pd.DataFrame()

    years = [2019,2020,2021,2022,2023] #define years to get data from

    #for each season, get all player info and create dataframe
    for i in years:
        url = f'https://www.pro-football-reference.com/years/{i}/fantasy.htm'
        table_data = pd.read_html(url)
        year_df = table_data[0]
        year_df['season'] = i
        df = pd.concat([df,year_df],axis=0)

    #pass columns list as df columns
    my_columns = ['rank','player','tm','fantpos','age','g','gs','passing_cmp','passing_att','passing_yds','passing_td','passing_int','rushing_att','rushing_yds','rushing_yds_att','rushing_td',
                'receiving_tgt','receiving_rec','receiving_yds','receiving_yds_rec','receiving_td','fmb','fl','td','2pm','2pp','fantpt','ppr','dkpt','fdpt','vbd','posrank','ovrank','season']

    df.columns = my_columns

    df = df.loc[df['rank']!='Rk'] #exclude rows with column names

    #change dtypes for numberic columns
    df[df.columns[4:33]] = df[df.columns[4:33]].apply(pd.to_numeric)

    #for dtypes for a few columns that raised an error
    df['rank'] = df['rank'].astype(int)
    df['player'] = df['player'].astype(str)
    df['tm'] = df['tm'].astype(str)
    df['fantpos'] = df['fantpos'].astype(str)

    #create additional metrics
    df['receiving_yds_g'] = df['receiving_yds'] / df['g']
    df['receiving_tgt_g'] = df['receiving_tgt'] / df['g']
    df['receiving_rec_g'] = df['receiving_rec'] / df['g']
    df['receiving_td_g'] = df['receiving_td'] / df['g']
    df['rushing_yds_g'] = df['rushing_yds'] / df['g']
    df['rushing_att_g'] = df['rushing_att'] / df['g']
    df['rushing_td_g'] = df['rushing_td'] / df['g']
    df['td_g'] = (df['rushing_td'] + df['receiving_td']) / df['g']

    #fill nulls with 0
    df = df.fillna(0)

    return df

def write_to_big_query(df,table):
    credentials = service_account.Credentials.from_service_account_file(config.credentials)

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    #append data to bigquery
    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = "CREATE_IF_NEEDED"
    job_config.write_disposition = "WRITE_TRUNCATE"
    table_id = f"fantasy_football.{table}"
    job = client.load_table_from_dataframe(dataframe=df,
                                                destination=table_id, 
                                                job_config=job_config)
    return print(f"{table} created with " + str(len(df)) + " rows"), job.result()


player_by_season = player_by_season()
print('player by season df created')

write_to_big_query(player_by_season,'player_by_season')