import configparser
import csv
import time
from datetime import datetime

import psycopg2
from psycopg2._psycopg import OperationalError
from pyDataverse.api import NativeApi, DataAccessApi
import logging
import requests

config = configparser.ConfigParser()
config.read('config.ini')

cfg_dataverse = config['DATAVERSE']
api_token1 = cfg_dataverse['api_token']
url_base = cfg_dataverse['url_base']

headers = {'X-Dataverse-key': api_token1}
api = NativeApi(url_base, api_token1 )
data_api = DataAccessApi(url_base , api_token1)


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except OperationalError as e:
        logging.error("The ERROR {0} occurred".format(str(e)))
    return connection

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        logging.error("The ERROR {0} occurred".format(str(e)))

def validate_datasets():
    csv_file = open(cfg_dataverse['list_file'])
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')


    csv_file_output = open('output.csv', 'w', newline='')
    csv_writer = csv.writer(csv_file_output, delimiter=',', escapechar='\\', quotechar='"')
    logging.info("Input file {0}".format(cfg_dataverse['list_file']))
    for dataset in reader:
        url = url_base + "/api/admin/validate/dataset/files/:persistentId/?persistentId=" + dataset[0]
        logging.info(url)
        resp = api.get_request(url)
        if resp.status_code == 200 or resp.status_code == 201:

            dataFiles = resp.json()["dataFiles"]
            logging.info("Hello")
            for file in dataFiles:
                logging.info(file)
                if (file["status"] != "valid"):
                    csv_writer.writerow([dataset, file["datafileId"], file["status"], file["errorMessage"]])
        else:
            logging.error("{0} with status {1}".format(resp.status_code, url))
            logging.error(resp.json())

    csv_file.close()
    csv_file_output.close()

def get_fileIds(pid, connection):
    query = "select dv.id, pid from dvobject as dv inner join " + \
    "(select concat(protocol, ':', authority, '/', identifier) as pid, id " + \
    "from dvobject where dtype = 'Dataset') datasets " + \
    "on(dv.owner_id = datasets.id ) where " + \
    "datasets.pid = '" + pid.strip() + "'"
    logging.info(query)
    result = execute_read_query(connection, query)
    return result

def main():
    api_token = cfg_dataverse['api_token']
    start = time.time()
    now_date = datetime.now()
    year = now_date.year
    month = now_date.strftime("%b")
    day = now_date.strftime("%d")
    filename_log = "update_checksum_" + str(day) + "-" + month + "-" + str(year) + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("Start checksum validation")
    csv_file = open(cfg_dataverse['list_file'])
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')
    #api/admin/validateDataFileHashValue/{fileId}

    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    number_of_files = 0
    for dataset in reader:
        datafiles = get_fileIds(dataset[0], connection)
        number_of_files = number_of_files + len(datafiles)
        logging.info("Number of datafiles in dataset is " + str(len(datafiles)))
        logging.info("Number of datasfiles total " + str(number_of_files))
        for file in datafiles:
            fileId = file[0]
            datasetPID = file[1]
            url = url_base + "/api/admin/validateDataFileHashValue/" + str(fileId)
            logging.info(url)
            end = time.time()
            duration = end - start
            if duration > 1200:
                url_token = url_base + "/api/users/token/recreate"
                header = {"X-Dataverse-key": api_token}
                resp_token = requests.post(url_token,  headers=header)
                if resp_token.status_code == 200 or resp.status_code == 201:
                    start = time.time()
                    message = resp_token.json()['data']['message']
                    m = message.split()
                    api_token = m[5]
                    logging.info(api_token)
                    header = {"X-Dataverse-key": api_token}
                    resp = requests.post(url, headers=header )
                    if resp.status_code == 200 or resp.status_code == 201:
                        logging.info(resp.json())
                    else:
                        logging.error(str(fileId) + " " + datasetPID + " " + str(resp.status_code))
                else:
                    logging.error("Cannot recreate api token " + str(resp_token.status_code) + " " + resp_token.json())
                    break
            else:
                header = {"X-Dataverse-key": api_token}
                resp = requests.post(url, headers=header)
                if resp.status_code == 200 or resp.status_code == 201:
                    logging.info(resp.json())
                else:
                    logging.error(str(fileId) + " " + datasetPID + " " + str(resp.status_code))


    csv_file.close()
    connection.close()
    logging.info("End of program")


if __name__ == '__main__':
    main()
