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
import pyDataverse.utils as utils

config = configparser.ConfigParser()
config.read('config.ini')

cfg_dataverse = config['DATAVERSE']
api_token = cfg_dataverse['api_token']
url_base = cfg_dataverse['base_url']

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
    logging.info(query)
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        #connection.rollback()
        connection.commit()
        logging.info(result)
        return result
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

def get_field_type(connection, fieldtype):
    query = "select id from datasetfieldtype where name LIKE '{}';".format(fieldtype)
    logging.info(query)
    result = execute_read_query(connection, query)
    return result

def get_datasetfields(connection, type, dataset_version):
    query = "select id from datasetfield where datasetfieldtype_id = {} and datasetversion_id = {};".format(type, dataset_version)
    logging.info(query)
    result = execute_read_query(connection, query)
    return result

def insert_datasetfield_topic(connection, field_type, dataset_version):
    query = "INSERT INTO datasetfield (datasetfieldtype_id, datasetversion_id ) VALUES ({}, {}) RETURNING id;".format(field_type,dataset_version);
    result = execute_query(connection, query)
    return result

def insert_datasetfield_topic_parent(connection, field_type, compoundvalue_id):
    query = "INSERT INTO datasetfield (datasetfieldtype_id, parentdatasetfieldcompoundvalue_id ) VALUES ({}, {}) RETURNING id;".format(field_type,compoundvalue_id);
    result = execute_query(connection, query)
    return result

def insert_compound_value(connection,display_number,field_id):
    query = "INSERT INTO datasetfieldcompoundvalue (parentdatasetfield_id, displayorder) VALUES ({},{}) RETURNING id;".format(field_id,display_number)
    result = execute_query(connection, query)
    return result


def insert_dataset_field_value(connection, field_id_term, term):
    query = "insert into datasetfieldvalue (displayorder, value, datasetfield_id) VALUES (0, '{}', {}) RETURNING id;".format(term, field_id_term)
    result = execute_query(connection, query)
    return result

def main():
    logging.basicConfig(filename='topic.log', level=logging.INFO)
    now = datetime.now()
    logging.info("Program started {}".format(now))
    connection = create_connection(cfg_dataverse['db_name'], cfg_dataverse['db_user'],
                                   cfg_dataverse['db_password'],
                                   cfg_dataverse['db_host'], cfg_dataverse['db_port'])
    field_type = get_field_type(connection, 'topicClassification')[0][0]
    logging.info(field_type)
    field_type_term = get_field_type(connection,'topicClassValue')[0][0]
    logging.info(field_type_term)

    resp = api.get_children(cfg_dataverse['dataverse_alias'], "dataverse", ['dataverses', 'datasets'])

    dataverses = utils.dataverse_tree_walker(resp)
    datasets = dataverses[1]
    #print(datasets)

    # for ds in datasets:
    #     try:
    #         pid = ds['pid']
    #         #print(pid)
    #         resp_dataset = api.get_dataset(pid)
    #         #print(resp_dataset.json())
    #         fields = resp_dataset.json()['data']['latestVersion']['metadataBlocks']['citation']['fields']
    #         dataset_version = resp_dataset.json()['data']['latestVersion']['id']
    #         datasetfields = get_datasetfields(connection, field_type, dataset_version)
    #         logging.info(datasetfields)
    #         display_number = 0
    #         found = False
    #         for f in fields:
    #             if f['typeName'] == 'topicClassification':
    #                 values = f['value']
    #                 display_number = len(values)
    #                 logging.info(f)
    #                 logging.info(display_number)
    #                 for v in values:
    #                     if v['topicClassValue']['value'] == 'Public Opinion Polls':
    #                         found = True
    #         if not found and len(datasetfields) == 0:
    #             field_id_array = insert_datasetfield_topic(connection, field_type, dataset_version)
    #             if len(field_id_array) > 0:
    #                 field_id = field_id_array[0][0]
    #                 compoundvalue_id_array = insert_compound_value(connection,display_number,field_id)
    #                 if len(compoundvalue_id_array) > 0:
    #                     compoundvalue_id = compoundvalue_id_array[0][0]
    #                     field_id_term_array = insert_datasetfield_topic_parent(connection, field_type_term, compoundvalue_id)
    #                     if len(field_id_term_array) > 0:
    #                         field_id_term = field_id_term_array[0][0]
    #                         insert_dataset_field_value(connection,  field_id_term,'Public Opinion Polls')
    #         elif (not found):
    #             field_id = datasetfields[0][0]
    #             compoundvalue_id_array = insert_compound_value(connection, display_number, field_id)
    #             if len(compoundvalue_id_array) > 0:
    #                 compoundvalue_id = compoundvalue_id_array[0][0]
    #                 field_id_term_array = insert_datasetfield_topic_parent(connection, field_type_term, compoundvalue_id)
    #                 if len(field_id_term_array) > 0:
    #                     field_id_term = field_id_term_array[0][0]
    #                     insert_dataset_field_value(connection, field_id_term, 'Public Opinion Polls')
    #
    #         #     #'INSERT INTO datasetfield (datasetfieldtype_id, datasetversion_id ) VALUES (25, 64) RETURNING id;'
    #         #     #'INSERT INTO datasetfieldcompoundvalue (parentdatasetfield_id) VALUES (1107) RETURNING id'
    #         #     #'insert into datasetfield (datasetfieldtype_id, parentdatasetfieldcompoundvalue_id) VALUES (26, 300) RETURNING id;'
    #         #     #"insert into datasetfieldvalue (value, datasetfield_id) VALUES ('Public Opinion Polls2', 1109) RETURNING id;"
    #     except Exception as e:
    #         logging.error(e)

    connection.close()
    now = datetime.now()
    logging.info("Program ended {}".format(now))

if __name__ == '__main__':
    main()


