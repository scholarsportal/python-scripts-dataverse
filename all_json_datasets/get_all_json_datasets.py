import pyDataverse.utils as utils
from pyDataverse.api import NativeApi
import json

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

cfg = config['DATAVERSE_APP']
api = NativeApi(cfg['base_url'], cfg['api_token'])


def main():
    resp = api.get_children(cfg['dataverse_alias'], "dataverse", ['dataverses', 'datasets'])

    dataverses = utils.dataverse_tree_walker(resp)
    datasets = dataverses[1]


    for dataset in datasets:
        row = []
        resp = api.get_dataset(dataset['pid'])
        print(resp.json()['data'])
        fields = resp.json()['data']['latestVersion']['metadataBlocks']['citation']['fields']
        for f in fields:
            if f['typeName'] == 'title':
                filename = "dmti/" + f['value'] + ".json"
                with open(filename, "w") as fl:
                    fl.write(json.dumps(resp.json()['data']))

if __name__ == '__main__':
    main()
