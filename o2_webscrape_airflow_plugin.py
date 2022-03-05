import json
import logging
import pandas
import re
from datetime import datetime
from tempfile import NamedTemporaryFile

import requests
from bs4 import BeautifulSoup
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models.baseoperator import BaseOperator

logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


class O2WebScrapeToGCSOperator(BaseOperator):

    def __init__(self, upload_prefix: str,
                 gcs_bucket: str, **kwargs):
        super().__init__(**kwargs)
        self.upload_prefix = upload_prefix
        self.gcs_bucket = gcs_bucket

        self.base_url = 'https://www.o2.co.uk'
        self.gcs_hook = GoogleCloudStorageHook()

    def execute(self, context):
        logger.info('Running execute')

        # Run pipeline
        logger.info('Running Pipeline')
        self.run_pipeline()
        logger.info('Task complete')

    def run_pipeline(self):
        # Scrape website and upload file
        today_date = datetime.today().strftime('%Y-%m-%d')

        upload_name = '_'.join(
            [
                self.upload_prefix, today_date
            ]
        ) + '.json'
        self.o2_web_scrape_and_upload(upload_name)

    def o2_web_scrape_and_upload(self, upload_name: str):
        baseurl = 'https://www.o2.co.uk'
        

        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
        }
        contract_types = ['paymonthly','payasyougo']
        productlinks = []
        productset = []
        for ct in contract_types:
            r = requests.get(f'https://www.o2.co.uk/shop/phones?contractType={ct}', headers=headers)

            soup = BeautifulSoup(r.content, 'lxml')

            productlist = soup.find_all('div', class_="device-tile-wrapper")
            for productgroup in productlist:
                for item in productgroup.find_all('a', href=True):
                    brand = item.find('span', class_='brand-name').text.strip()
                    model = item.find('div', class_='model-family').text.strip()
                    link = (baseurl + item['href'])
                    rating = item.find('span', itemprop='ratingValue').text.strip()
                    reviewcount = item.find('span', itemprop='bestRating').text.strip()

                    if ct == 'paymonthly':
                        upfront_cost = item.find('span', class_='headline').text.strip()
                        mp_pounds = item.find('span', class_='costVal').text.strip()
                        mp_pence = item.find('span', class_='costPence').text.replace('*','').strip()
                        monthly_price = mp_pounds + mp_pence
                        contract_type = 'Pay Monthly'
                    else:
                        upfront_cost = item.find('span', class_='headline').text.strip()
                        monthly_price = 'N/A'
                        contract_type = 'Pay As You Go'

                    productlinks.append(link)
                    product = {
                        'brand': brand,
                        'model': model,
                        'upfront_cost': upfront_cost,
                        'monthly_price': monthly_price,
                        'link': link,
                        'rating': round(float(rating),2),
                        'reviews': reviewcount + ' reviews',
                        'contract_type': contract_type
                    }
                    productset.append(product)

        spec_data = []
        for link in productlinks:
            index = productlinks.index(link)
            total = len(productlinks)
            print('Extracting spec data ' + str(index) + ' out of ' + str(total) + '...')
            df = pandas.read_html(link)

            spec_table = df[0].transpose() # transpose the spec data
            new_header = spec_table.iloc[0] #grab the first row for the header
            spec_table = spec_table[1:] #take the data less the header row
            spec_table.columns = new_header #set the header row as the df header
            spec_table['link'] = link # add the link as a column
            spec_table = spec_table.loc[:,~df.columns.duplicated()] # rudimentary handling of cases with duplicate columns
            spec_data.append(spec_table)

        productset_df = pandas.DataFrame(productset)
        spec_concat = pandas.concat(spec_data)

        final_df = pandas.merge(productset_df, spec_concat, how = 'left', on = 'link')

        with NamedTemporaryFile('w+') as upload_file:
            final_df.to_json(index="False")

        logger.info(f'Uploading file: {upload_name}')

        upload_file.seek(0)
        self.gcs_hook.upload(self.gcs_bucket, object_name=upload_name,
                             filename=upload_file.name)
        upload_file.close()