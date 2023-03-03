import csv
import hashlib
from pyDataverse.api import NativeApi, DataAccessApi
import psycopg2
from psycopg2 import OperationalError
import configparser
import sys
import os
import time
import requests

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
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")
    return connection

def execute_query(connection, query):
    #connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
        connection.commit()
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")
        connection.rollback()


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The ERROR '{e}' occurred")

def find_identifier(owner_id):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    query = "select concat(authority, '/', identifier) from dvobject where id={0}".format(owner_id)
    result = execute_read_query(connection, query)
    print(result)
    connection.close()
    return result

def replace_file(result, identifier, replacement_file):
    split_strings = result[0][1].split(':')
    storage_id = split_strings[2]
    backet_name = split_strings[1][2::]
    print(backet_name)
    print(storage_id)
    print(identifier)
    file_name = "s3://" + backet_name + "/" + identifier + "/" + storage_id
    print(file_name)
    cmd = "aws --endpoint-url " + cfg_dataverse['s3_endpoint'] + " s3 cp " + replacement_file + " " + file_name
    os.system(cmd)
def find_storage_id(file_id):
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    query = "select owner_id, storageidentifier from dvobject where id={0}".format(str(file_id))
    result = execute_read_query(connection, query)
    print(result)
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
        url = url_base + "/api/files/" + str(file_id) + "/uningest"
        resp = requests.post(url, headers=headers)
        print(resp.status_code)


def reingest_file(originalFileFormat, file_ending, file_id):
    original = originalFileFormat.lower()
    if original == "text/tab-separated-values" or file_ending == 'sav' or \
            file_ending == 'dta' or file_ending == 'por' or file_ending == 'csv' or \
            file_ending == 'tsv' or file_ending == 'xlxs' or file_ending == 'xls':
        url = url_base + "/api/files/" + str(file_id) + "/reingest"
        resp=requests.post(url, headers=headers)
        print(resp.status_code)
        print(resp.json())

def main():
    csv_file = open(cfg_dataverse['list_file'])
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')

    for file in reader:
        print(file[0])
        file_metadata = get_file_metadata(file[0])
        print(file_metadata)
        uningest_file(file[0], file_metadata[0][0])

        #Get new file
        resp = requests.get(file[1], allow_redirects=True)
        if resp.status_code == 200:
            open('temp_file', 'wb').write(resp.content)

            #Replace file part
            result = find_storage_id(file[0])
            owner_id = result[0][0]
            time.sleep(1)
            identifier = find_identifier(owner_id)
            replace_file(result, identifier[0][0], "temp_file")

            #Checksum
            #type = magic.from_file('temp_file', mime=True)
            #print(type)
            readable_hash = hashlib.md5(resp.content).hexdigest()
            print(readable_hash)
            filesize = os.path.getsize('temp_file')
            print(filesize)
            #resp_meta = api.get_datafile_metadata(file[0])
            #print(resp_meta.json())
            time.sleep(1)
            update_checksum(file[0], readable_hash, filesize, type)


            #Update file metadata (label)
            filename_split = file[1].split('/')
            filename = filename_split[len(filename_split)-1]
            #update_file_metadata(file[0], filename)

            file_ending = filename.split(".")[1]

            reingest_file(file_metadata[0][0], file_ending, file[0])

            #Delete temp file
            os.system("rm temp_file")

    csv_file.close()

if __name__ == '__main__':
    main()
