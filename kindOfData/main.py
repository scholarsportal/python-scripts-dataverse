import pyDataverse.utils as utils
from pyDataverse.api import NativeApi
import csv

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

cfg = config['DATAVERSE_APP']
api = NativeApi(cfg['base_url'], cfg['api_token'])


def main():
    resp = api.get_children(cfg['dataverse_alias'], "dataverse", ['dataverses', 'datasets'])

    dataverses = utils.dataverse_tree_walker(resp)
    datasets = dataverses[1]
    print(len(datasets))

    rows = []
    for dataset in datasets:
        row = []
        resp = api.get_dataset(dataset['pid'])
        found = False
        if resp.status_code == 200: #success
            kindofData = ""
            data = resp.json()['data']
            if 'latestVersion' in data:
                latestVersion = data['latestVersion']
                if 'metadataBlocks' in latestVersion:
                    if 'citation' in latestVersion['metadataBlocks']:
                        citation = latestVersion['metadataBlocks']['citation']
                        if 'fields' in citation:
                            fields = citation['fields']
                            for field in fields:
                                if 'typeName' in field and field['typeName'] == 'kindOfData':
                                    if 'value' in field:
                                        kindOfData = field['value']
                                        found = True
                                        break
            row.append(dataset['pid'])
            row.append(kindOfData)
            rows.append(row)

            #print('pid={0} termsOfUse="{1}" license="{2}" date={3} title="{4}" depositor="{5}" distributionDate="{6}"'.format(dataset['pid'], termsOfUse, license, publicationDate, title, depositor, distributionDate))

        else:
            print("Cannot get dataset {0}".format(dataset['pid']))

    fields = ['PID', 'kindOfData']
    with open(config['OUTPUT']['filename'], 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        #Write headers
        csvwriter.writerow(fields)
        #Write data
        csvwriter.writerows(rows)
if __name__ == '__main__':
    main()
