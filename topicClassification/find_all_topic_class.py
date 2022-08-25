import requests
import pyDataverse.utils as utils
from pyDataverse.api import NativeApi
import json

import configparser
print("hello")
config = configparser.ConfigParser()
config.read('config.ini')
cfg = config['DATAVERSE_APP']

api = NativeApi(cfg['base_url'])



def main():

    # https: // demo.dataverse.org / api /
    # print("Hi")
    rows = 1000
    start = 0
    page = 1
    condition = True  # emulate do-while
    while (condition):
        url = cfg['base_url'] + "/api/search?q=*&subtree=odesi&type=dataset&publicationStatus:Published&metadata_fields=citation:topicClassification&per_page=" + str(rows) + "&start=" + str(start)
        resp = requests.get(url)
        total = resp.json()['data']['total_count']
        page += 1
        condition = start < total
    # print(resp.json())
    # items = resp.json()['data']['items']
    # print(len(items))

if __name__ == '__main__':
    main()
