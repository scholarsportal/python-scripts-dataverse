import csv
import hashlib
from pyDataverse.api import NativeApi, DataAccessApi
import psycopg2
from psycopg2 import OperationalError
import configparser
from datetime import datetime
import os
import time
import requests
import logging

config = configparser.ConfigParser()
config.read('config.ini')

cfg_dataverse = config['DATAVERSE']
api_token = cfg_dataverse['api_token']
url_base = cfg_dataverse['url_base']

headers = {'X-Dataverse-key': api_token}
api = NativeApi(url_base, api_token )
data_api = DataAccessApi(url_base , api_token)

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
        logging.error(f"The ERROR '{e}' occurred")
    return connection

def execute_query(connection, query):
    #connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
    except OperationalError as e:
        logging.error(f"The ERROR '{e}' occurred")
        connection.rollback()


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        logging.error(f"The ERROR '{e}' occurred")

def find_identifier(owner_id):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    query = "select concat(authority, '/', identifier) from dvobject where id={0}".format(owner_id)
    result = execute_read_query(connection, query)
    connection.close()
    return result

def replace_file(result, identifier, replacement_file):
    split_strings = result[0][1].split(':')
    storage_id = split_strings[2]
    backet_name = split_strings[1][2::]
    logging.info("Backet name: " + backet_name)
    logging.info("Storage_id: " + storage_id)
    file_name = "s3://" + backet_name + "/" + identifier + "/" + storage_id
    logging.info("Dataverse file name: " + file_name)
    cmd = "aws --endpoint-url " + cfg_dataverse['s3_endpoint'] + " s3 cp " + replacement_file + " " + file_name
    os.system(cmd)
def find_storage_id(file_id):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    query = "select owner_id, storageidentifier from dvobject where id={0}".format(str(file_id))
    result = execute_read_query(connection, query)
    connection.close()
    return result

def update_file_metadata(file_id, filename):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    query = "update filemetadata set label ='{0}' where datafile_id={1}".format(filename, str(file_id))
    execute_query(connection, query)
    connection.close()

def update_checksum(file_id, checksumvalue, filesize, contenttype):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])

    query = "update datafile set checksumtype='MD5', checksumvalue='{1}', filesize={2} where id={0}".format(str(file_id), checksumvalue, filesize)
    execute_query(connection, query)
    connection.close()

def get_file_metadata(file_id):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])

    query = "select contenttype, ingeststatus from datafile where id={0}".format(str(file_id))
    result = execute_read_query(connection, query)
    connection.close()
    return result

def uningest_file(file_id, type):
    if type == "text/tab-separated-values":
        print(type)
        print(file_id)
        url = url_base + "/api/files/" + str(file_id) + "/uningest"
        logging.info(url)
        print("Starting uningest {0}".format(str(file_id)))
        resp = requests.post(url, headers=headers)
        if resp.status_code == 200 or resp.status_code == 201:
            print("Successfully ended uningest")
            return True
        else:
            print(resp.status_code)
            print("Uningest was unsuccessful")
            return False
    else:
        return True

def reingest_file(originalFileFormat, file_ending, file_id):
    original = originalFileFormat.lower()
    logging.info(original)
    if original == "text/tab-separated-values" or file_ending == 'sav' or \
            file_ending == 'dta' or file_ending == 'por' or file_ending == 'csv' or \
            file_ending == 'tsv' or file_ending == 'xlxs' or file_ending == 'xls':
        url = url_base + "/api/files/" + str(file_id) + "/reingest"
        logging.info(url)
        print("Starting reingest {0}".format(str(file_id)))
        resp=requests.post(url, headers=headers)
        if resp.status_code != 200:
            print("Reingest was unsuccessful")
            logging.error("Error reingesting file {0}".format(file_id))
            return False
        else:
            print("Successfully ended reingest")

    return True
def main():

    now_date = datetime.now()
    year = now_date.year
    month = now_date.strftime("%b")
    day = now_date.strftime("%d")

    filename_log = "update_checksum_" + str(day) + "-" + month + "-" + str(year) + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG,
                        format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("Program started")
    csv_file = open(cfg_dataverse['list_file'])
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')

    for file in reader:
        logging.info(file[0])
        print(str(file[0]))
        file_metadata = get_file_metadata(file[0])
        if file_metadata != None and len(file_metadata) > 0 and file_metadata[0] != None and len(file_metadata[0][0])>0:
            status_uningest=uningest_file(file[0], file_metadata[0][0])
            print(status_uningest)
            if status_uningest:
                #Get new file
                resp = requests.get(file[1], allow_redirects=True)
                time.sleep(20)
                if resp.status_code == 200:
                    open('temp_file', 'wb').write(resp.content)

                    #Replace file part
                    result = find_storage_id(file[0])
                    if result != None and len(result) > 0 and result[0] != None and len(result[0]) > 0:
                        owner_id = result[0][0]
                        identifier = find_identifier(owner_id)
                        if identifier != None and len(identifier) > 0 and identifier[0] != None and len(identifier[0]) > 0:
                            replace_file(result, identifier[0][0], "temp_file")

                            #Checksum
                            #type = magic.from_file('temp_file', mime=True)
                            #print(type)
                            readable_hash = hashlib.md5(resp.content).hexdigest()
                            logging.info("Hash : " + readable_hash)
                            filesize = os.path.getsize('temp_file')
                            logging.info("Filesize: " + str(filesize))
                            #resp_meta = api.get_datafile_metadata(file[0])
                            #print(resp_meta.json())
                            update_checksum(file[0], readable_hash, filesize, type)


                            #Update file metadata (label)
                            filename_split = file[1].split('/')
                            filename = filename_split[len(filename_split)-1]
                            update_file_metadata(file[0], filename)

                            file_ending_split = filename.split(".")
                            if file_ending_split != None and  len(filename_split) > 1:
                                file_ending = filename.split(".")[1]
                            else:
                                file_ending = None

                            reingest_file(file_metadata[0][0], file_ending, file[0])

                            #Delete temp file
                            os.system("rm temp_file")
                        else:
                            logging.error("Cannot find PID for file {0} and dataset {1}".format(str(file[0]), str(owner_id)))
                    else:
                        logging.error("Cannot find storage_id for file {0}".format(str(file[0])))
                else:
                    logging.error("Cannot get file {0}".format(file[1]))
            else:
                logging.error("Cannot uningest file {0}".format(str(file[0])))
        else:
            logging.error("Cannot get file metadata for file {0}".format(str(file[0])))

    csv_file.close()
    logging.info("Program ended")

if __name__ == '__main__':
    main()
