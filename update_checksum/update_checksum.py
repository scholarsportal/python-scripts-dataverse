import csv
import hashlib
from pyDataverse.api import NativeApi, DataAccessApi
import psycopg2
from psycopg2 import OperationalError
import configparser
import sys
import os
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

def find_identifier(connection, owner_id):
    query = "select concat(authority, '/', identifier) from dvobject where id={0}".format(owner_id)
    result = execute_read_query(connection, query)
    print(result)
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
    cmd = "aws --endpoint-url https://olrc2.scholarsportal.info s3 cp " + replacement_file + " " + file_name
    os.system(cmd)
def find_storage_id(connection,file_id):
    query = "select owner_id, storageidentifier from dvobject where id={0}".format(str(file_id))
    result = execute_read_query(connection, query)
    print(result)
    return result

def find_checksum(connection, file_id, checksumvalue, filesize):

    query = "update datafile set checksumtype='MD5', checksumvalue='{1}', filesize={2} where id={0}".format(str(file_id), checksumvalue, filesize)
    execute_query(connection, query)

def main():
    csv_file = open(cfg_dataverse['list_file'])
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')

    for file in reader:
        print(file[0])

        resp=data_api.get_datafile(file[0])
        if resp.status_code == 200:
            open('test.txt', 'wb').write(resp.content)
            type = magic.from_file('test.pdf', mime=True)
            print(type)
            readable_hash = hashlib.md5(resp.content).hexdigest()
            print(readable_hash)
            filesize = sys.getsizeof(resp.content)
            print(filesize)
            print(os.path.getsize('test.pdf'))
            connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'], cfg_dataverse['db_password'],
                                           cfg_dataverse['db_host'], cfg_dataverse['db_port'])
            result = find_storage_id(connection, file[0])
            connection.close()
            owner_id = result[0][0]

            connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                           cfg_dataverse['db_password'],
                                           cfg_dataverse['db_host'], cfg_dataverse['db_port'])
            identifier = find_identifier(connection, owner_id)
            #find_checksum(connection, file[0], readable_hash, filesize)
            connection.close()
            replace_file(result, identifier[0][0], "test.pdf")
    csv_file.close()

if __name__ == '__main__':
    main()
