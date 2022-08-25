import requests
import json
import sys
from pyDataverse.api import NativeApi
import time
from datetime import datetime

import configparser
config = configparser.ConfigParser()
config.read('config.ini')
cfg = config['DATAVERSE_APP']

persistentId = cfg['persistentId']
url_base = cfg['base_url']
filename = cfg['filename']
headers = {'X-Dataverse-key': cfg['api_token']}

api = NativeApi(url_base, cfg['api_token'] )

def main():
    add_file(filename,"true", filename, "Desc")
    now = datetime.now()
    print("START LOCK ADDING FILE {0} time {1}".format(persistentId, now))
    check_lock(persistentId)
    now = datetime.now()
    print("END LOCK ADDING FILE {0} time {1}".format(persistentId, now))
    publish_version(0, persistentId)
    now = datetime.now()
    print("START LOCK PUBLISHING {0} time {1}".format(persistentId, now))
    check_lock(persistentId)
    now = datetime.now()
    print("END LOCK PUBLISHING FILE {0} time {1}".format(persistentId, now))


def add_file(file_name, tabIngest, type, desc):
    print("Start add_file")
    datafiles=[]
    #with zipfile.ZipFile(file_name, 'r') as zip_ref:
    #    files_names = zip_ref.namelist()
        #for file in files_names:
            #f = zip_ref.read(file)
    url = url_base + "/api/datasets/:persistentId/add?persistentId=" + persistentId

    index = file_name.rfind("/")
    filename = file_name[index + 1:]
    files = {'file': (filename, open(file_name,'rb'))}

    df = {}
    df['tabIngest'] = tabIngest
    df['description'] = desc
    if type != "":
        df['directoryLabel'] = type
    print(df)
    try:
                resp = requests.post(url, data={"jsonData": json.dumps(df)}, files=files, headers=headers)
                print(resp.json())
                if type == "":
                    datafiles.append(resp.json()['data']['files'][0]['dataFile']['id'])
    except Exception as e:
                print("add_file: {0}".format(e))
    return datafiles

def check_lock(DOI_NEW):
    try:
        lock = api.get_dataset_lock(DOI_NEW)
        if lock.status_code == 503:
            print("503 - Server {} is unavailable".format(config.base_url_target))
            sys.exit()
        a = 0
        while len(lock.json()['data']) > 0:# and a < 300:
            #print("Lock {} times {} {}".format(str(a), DOI_NEW, lock.json()))
            time.sleep(350)
            a = a + 1
            lock = api.get_dataset_lock(DOI_NEW)
            if lock.status_code == 503:
                print("503 - Server {} is unavailable".format(config.base_url_target))
                sys.exit()
            if lock.status_code != 200:
                print("check_lock func: lock status {} for {}".format(lock.status_code, DOI_NEW))
                return False
    except Exception as e:
        print("check_lock. Error {}, dataset {} ".format(e, DOI_NEW))
        return False
    return True

def publish_version(vm, DOI_NEW):
    check = check_lock(DOI_NEW)
    if check == False:
        return check
    try:
        if vm != 0:
            resp = api.publish_dataset(DOI_NEW,'minor')
        else:
            resp = api.publish_dataset(DOI_NEW, 'major')
        if resp.status_code == 200:
            return True
        else:
            print("publish_version func: Error publishing dataset {} status {}".format(DOI_NEW, resp.status_code))
            return False
    except Exception as e:
        print("publish_version error. Error {}, dataset {} ".format(e, DOI_NEW))
        return False

    return True
if __name__ == '__main__':
    main()