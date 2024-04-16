import requests
import pyDataverse.utils as utils
from pyDataverse.api import NativeApi, DataAccessApi
import json

import configparser
config = configparser.ConfigParser()
config.read('config.ini')
cfg = config['DATAVERSE']

api = NativeApi(cfg['base_url'], cfg['api_token'])
data_api = DataAccessApi(cfg['base_url'], cfg['api_token'])

def main():

    headers = {'X-Dataverse-key': cfg['api_token']}
    rows = 1000
    start = 0
    page = 1
    condition = True

    while (condition):
        url = cfg['base_url'] + "/api/search?q=*&type=file&subtree=" + cfg['dataverse_alias'] + "&&per_page=" + str(rows) + "&start=" + str(start)
        #print(url)
        resp = requests.get(url, headers=headers)
        total = resp.json()['data']['total_count']
        #print(total)
        for i in resp.json()['data']['items']:
            #print(i['file_id'])
            try:
                url_req = cfg['base_url'] + '/api/access/datafile/' + i['file_id'] +'/listRequests'
                resp_req = requests.get(url_req, headers=headers)
                if 'data' in resp_req.json():
                    users = resp_req.json()['data']
                    for user in users:
                        displayName = user['displayName']
                        email = user['email']
                        user_identifier = user['identifier']
                        doi = i['dataset_persistent_id']
                        dataset_name = i['dataset_name']
                        print("{},{},{},{},{},{}".format(user_identifier, displayName, email, i['file_id'], doi, dataset_name))
            except Exception as e:
                print(resp_req.json())
                print(e)


        start = start + rows
        page += 1
        condition = start < total
if __name__ == '__main__':
    main()