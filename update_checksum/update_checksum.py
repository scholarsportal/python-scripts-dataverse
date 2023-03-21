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
import magic

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
        logging.error("The ERROR {0} occurred".format(str(e)))
    return connection

def execute_query(connection, query):
    #connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
    except OperationalError as e:
        logging.error("The ERROR {0} occurred".format(str(e)))
        connection.rollback()


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        logging.error("The ERROR {0} occurred".format(str(e)))

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
    print("Start copying file..." )
    os.system(cmd)
def find_storage_id(file_id):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    query = "select owner_id, storageidentifier from dvobject where id={0}".format(str(file_id))
    result = execute_read_query(connection, query)
    connection.close()
    return result

def update_file_metadata(file_id, filename, check_date):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])

    now_date = datetime.now()
    year = now_date.year
    month = now_date.strftime("%b")
    day = now_date.strftime("%d")

    current_date = str(day) + "-" + month + "-" + str(year)

    if check_date != None:
        prov_freeform = "On {0}, the file was replaced with a backup copy as part of the remediation process " \
                        "for files that fail the fixity check as outlined in the Borealis Preservation Plan " \
                        "(borealisdata.ca/preservationplan). " \
                        "The failed fixity check was first reported on {1}.".format(current_date,check_date)

        query = "update filemetadata set label ='{0}', prov_freeform = '{2}' where datafile_id={1}".format(filename,
                                                                                                           str(file_id), prov_freeform)
    else:
        query = "update filemetadata set label ='{0}' where datafile_id={1}".format(filename, str(file_id))
    execute_query(connection, query)
    connection.close()

def update_checksum(file_id, checksumvalue, filesize, contenttype):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])

    query = "update datafile set checksumtype='MD5', checksumvalue='{1}', filesize={2}, contenttype='{3}' where id={0}".format(str(file_id), checksumvalue, filesize, contenttype)
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
        print("Starting uningest {0}".format(str(file_id)))
        url = url_base + "/api/files/" + str(file_id) + "/uningest"
        logging.info(url)

        resp = requests.post(url, headers=headers)
        if resp.status_code == 200 or resp.status_code == 201:
            print("Successfully ended uningest")
            return True
        else:
            print("status_code:" + str(resp.status_code))
            print("Uningest was unsuccessful")
            return False
    else:
        return True

def determine_type_for_ingest(type, file_ending):
    file_format = type.lower()
    file_ending = file_ending.lower()
    if file_format != "application/x-stata" \
            or file_format != "application/x-stata-13" \
            or file_format != "application/x-stata-14" \
            or file_format != "application/x-stata-15" \
            or file_format != "application/x-rlang-transport" \
            or file_format != "text/csv" \
            or file_format != "text/comma-separated-values" \
            or file_format != "text/tsv" \
            or file_format != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" \
            or file_format != "application/x-spss-sav" \
            or file_format != "application/x-spss-por":

        if file_ending == 'dta':
            type = "application/x-stata"
        elif file_ending == 'rdata':
            type = "application/x-rlang-transport"
        elif file_ending == 'csv':
            type = "text/csv"
        elif file_ending == 'tsv':
            type = "text/tsv"
        elif file_ending == 'xlxs' or file_ending == 'xls':
            type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif file_ending == "sav":
            type = "application/x-spss-sav"
        elif file_ending == 'por':
            type = "application/x-spss-por"

    return type

def reingest_file(fileFormat, file_id):
    file_format = fileFormat.lower()
    logging.info(file_format)
    #if original == "text/tab-separated-values" or file_ending == 'sav' or \
    #        file_ending == 'dta' or file_ending == 'por' or file_ending == 'csv' or \
    #        file_ending == 'tsv' or file_ending == 'xlxs' or file_ending == 'xls':

    if file_format == "application/x-stata" \
        or file_format == "application/x-stata-13" \
        or file_format == "application/x-stata-14" \
        or file_format == "application/x-stata-15" \
        or file_format == "application/x-rlang-transport" \
        or file_format == "text/csv" \
        or file_format == "text/comma-separated-values" \
        or file_format == "text/tsv" \
        or file_format == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" \
        or file_format == "application/x-spss-sav" \
        or file_format == "application/x-spss-por":


        url = url_base + "/api/files/" + str(file_id) + "/reingest"
        logging.info(url)
        print("Starting reingest {0}".format(str(file_id)))
        resp=requests.post(url, headers=headers)
        if resp.status_code != 200:
            print("status_code:" + resp.status_code)
            print("Reingest was unsuccessful")
            logging.error("Error reingesting file {0}".format(file_id))
            return False
        else:
            print("Successfully queued reingest")

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
        logging.info("file_id:" + file[0])
        print("=====================")
        print("file_id:" + str(file[0]))
        file_metadata = get_file_metadata(file[0])
        if file_metadata != None and len(file_metadata) > 0 and file_metadata[0] != None and len(file_metadata[0][0])>0:
            original_type = file_metadata[0][0]
            status_uningest=uningest_file(file[0], original_type )
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
                            type = magic.from_file('temp_file', mime=True)

                            readable_hash = hashlib.md5(resp.content).hexdigest()
                            logging.info("Hash : " + readable_hash)
                            filesize = os.path.getsize('temp_file')
                            logging.info("Filesize: " + str(filesize))
                            #resp_meta = api.get_datafile_metadata(file[0])
                            #print(resp_meta.json())

                            #Update file metadata (label)_
                            filename_split = file[1].split('/')
                            filename = filename_split[len(filename_split)-1]
                            if len(file) > 2 and file[2] != None:
                                check_date = file[2]
                            else:
                                check_date = None
                            update_file_metadata(file[0], filename,check_date)

                            file_ending_split = filename.split(".")
                            if file_ending_split != None and  len(filename_split) > 1:
                                file_ending = filename.split(".")[1]
                            else:
                                file_ending = None

                            type = determine_type_for_ingest(type, file_ending)
                            print("New file type: " + type)

                            update_checksum(file[0], readable_hash, filesize, type)

                            reingest_file(type, file[0])

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
