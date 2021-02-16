# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from json import loads
import pandas as pd
import requests

@click.command()
@click.argument('input_filepath', type=click.Path())
@click.argument('output_filepath', type=click.Path())
def main(input_filepath , output_filepath ):
#def main():

    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """

    departement = 'Yvelines'

    #GET all the data for a departement at any date
    api_url_histo = 'https://coronavirusapi-france.now.sh/AllDataByDepartement?Departement=' + departement
    r = requests.get(api_url_histo)
    dic = loads(r.text)  
    df = pd.DataFrame.from_dict(dic['allDataByDepartement'])

    df= df[['date','reanimation']]
    
    #filtre departement
    df['ds'] = pd.to_datetime(df['date']).dt.date 

    #get rid of any missing values
    a= len(df)
    df = df.dropna(how='any')
    print(f'Dropped : {a - len(df)} rows')

    #rename & convert to integer
    df['y'] = df['reanimation'].astype('int')

    #only keep two columns
    df = df[['ds', 'y']]

    #write parquet
    latest = df['ds'].max().strftime('%Y%m%d')
    savename = f'data{departement}{latest}.pq'
    #savepath = 'gdrive/MyDrive/dsdata/'

    df.to_parquet(f'{savename}')
     


    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
