import requests
from pyDataverse.api import NativeApi, DataAccessApi
import configparser
import logging
import time
import sys
from datetime import datetime
import csv

import re

config = configparser.ConfigParser()
config.read('config_ARG.ini')

#ns = {
#    'ns': 'http://www.icpsr.umich.edu/DDI',
    # 'ns': "ddi:codebook:2_5",
#    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
#}
#test

cfg_dataverse = config['DATAVERSE']
api_token = cfg_dataverse['api_token']
url_base = cfg_dataverse['url_base']

headers = {'X-Dataverse-key': api_token}
api = NativeApi(url_base, api_token )
data_api = DataAccessApi(url_base , api_token)

def check_lock(dataset_id):
    time_start = datetime.now()
    logging.debug("Start check_lock")
    try:
        url = url_base + "/api/datasets/{0}/locks".format(dataset_id)
        lock = requests.get(url, headers)
        if lock.status_code == 503:
            logging.critical("503 - Server is unavailable")
            sys.exit()
        a = 0
        while len(lock.json()['data']) > 0:
            logging.debug("Lock {} times {} {}".format(str(a), dataset_id, lock.json()))
            logging.debug(lock.json())
            time.sleep(10)
            a = a + 1

            lock = requests.get(url, headers)
            if lock.status_code == 503:
                logging.critical("503 - Server is unavailable")
                sys.exit()
            if lock.status_code != 200:
                logging.error("check_lock func: lock status {} for {}".format(str(lock.status_code), dataset_id))
                return False
    except Exception as e:
        logging.error("check_lock. Error {}, dataset {} ".format(str(e), dataset_id))
        return False
    time_end = datetime.now()
    t = (time_end - time_start)
    logging.info("Dataset " + str(dataset_id) + " was locked " +  str(t.total_seconds()) + " sec")
    return True


def publish_version(DOI, dataset_id):
    logging.debug("Start version")
    check = check_lock(dataset_id)
    if check == False:
        return check
    try:
        resp = api.publish_dataset(DOI, 'minor')
        if resp.status_code == 200:
            check = check_lock(dataset_id)
            if check:
                return True
        else:
            logging.error("publish_version func: Error publishing dataset {} status {}".format(DOI, str(resp.status_code)))
            return False
    except Exception as e:
        logging.error("publish_version error. Error {}, dataset {} ".format(str(e), DOI))
        return False

    return True

if __name__ == '__main__':
    start_time = time.time()
    now = datetime.now()
    now_date = datetime.now()
    year = now_date.year
    month = now_date.strftime("%b")
    day = now_date.strftime("%d")

    filename_log = "publish_datasets_" + str(day) + "-" + month + "-" + str(year) + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("Program started")
    list_file=config['NESSTAR']['list_file']
    logging.debug(list_file)
    dir = config['NESSTAR']['dir']
    csv_file = open(list_file)
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')

    for dataset in reader:
        pid = dataset[2]
        try:
            resp = api.get_dataset(pid)
            if (resp.status_code == 200):
                id = resp.json()['data']['id']
                publish_version(pid, id)
            else:
                logging.error("Cannot get dataset {0}".format(pid))
        except Exception as e:
            logging.error(str(e))
