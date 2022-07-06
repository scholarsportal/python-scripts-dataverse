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

    rows = []
    for dataset in datasets:
        row = []
        resp = api.get_dataset(dataset['pid'])
        if resp.status_code == 200: #success
            termsOfUse = ""
            license = ""
            publicationDate = ""
            title = ""
            depositor = ""
            distributionDate = ""
            data = resp.json()['data']
            if 'latestVersion' in data:
                latestVersion = data['latestVersion']
                if 'termsOfUse' in latestVersion:
                    termsOfUse = latestVersion['termsOfUse']
                if 'license' in latestVersion:
                    license = latestVersion['license']
                if 'publicationDate' in  data:
                    publicationDate = data['publicationDate']
                if 'distributionDate' in latestVersion:
                    distributionDate = latestVersion['distributionDate']
                if 'metadataBlocks' in latestVersion:
                    if 'citation' in latestVersion['metadataBlocks']:
                        citation = latestVersion['metadataBlocks']['citation']
                        if 'fields' in citation:
                            fields = citation['fields']
                            for field in fields:
                                if 'typeName' in field and field['typeName'] == 'title':
                                    if 'value' in field:
                                        title = field['value']
                                if 'typeName' in field and field['typeName'] == 'depositor':
                                    if 'value' in field:
                                        depositor = field['value']
            row.append(dataset['pid'])
            row.append(termsOfUse)
            row.append(license)
            row.append(publicationDate)
            row.append(title)
            row.append(depositor)
            row.append(distributionDate)
            rows.append(row)

            #print('pid={0} termsOfUse="{1}" license="{2}" date={3} title="{4}" depositor="{5}" distributionDate="{6}"'.format(dataset['pid'], termsOfUse, license, publicationDate, title, depositor, distributionDate))

        else:
            print("Cannot get dataset {0}".format(dataset['pid']))

    fields = ['PID', 'termsOfUse', 'License', 'publicationDate', 'Title', 'Depositor', 'distributionDate']
    with open(config['OUTPUT']['filename'], 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        #Write headers
        csvwriter.writerow(fields)
        #Write data
        csvwriter.writerows(rows)
if __name__ == '__main__':
    main()
