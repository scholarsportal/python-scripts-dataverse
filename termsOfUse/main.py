import pyDataverse.utils as utils
from pyDataverse.api import NativeApi

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
        resp = api.get_dataset(dataset['pid'])
        if resp.status_code == 200: #success
            termsOfUse = ""
            license = ""
            data = resp.json()['data']
            if 'latestVersion' in data:
                latestVersion = data['latestVersion']
                if 'termsOfUse' in latestVersion:
                    termsOfUse = latestVersion['termsOfUse']
                if 'license' in latestVersion:
                    license = latestVersion['license']

            print('pid={0} termsOfUse="{1}" license="{2}"'.format(dataset['pid'], termsOfUse, license))

        else:
            print("Cannot get dataset {0}".format(dataset['pid']))

if __name__ == '__main__':
    main()
