import json
from os.path import exists
import xml.etree.ElementTree as ET
import requests
from pyDataverse.api import NativeApi, DataAccessApi
import time
import sys
import logging
import configparser
import csv
from datetime import datetime

import re

config = configparser.ConfigParser()
config.read('config.ini')

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

def get_var_metadata(ddi_file):
    logging.debug("Start get_var_metadata")

    tree = ET.parse(ddi_file)

    root = tree.getroot()
    result = re.search('\{(.*)\}', root.tag)
    if result == None:
        ns = ''
    else:
        ns = result.group(0)
    dataDscr = tree.find(ns + "dataDscr")
    return dataDscr

def map_label_var(dataDscr, ns):
    logging.debug("Start map_label_var")
    map_name_id = {}
    
    vars = dataDscr.findall(ns + "var")
    
    for var in vars:
        #label = var.find("ns:labl",ns)
        ID = var.attrib.get("ID")
        name = var.attrib.get("name")
        map_name_id[name] = ID
    return map_name_id

def var_ids_correspondence(map_nesstar, map_dataverse ):
    logging.debug("Start var_ids_correspondence")
    
    map_ids = {}
    map_ids_n_d = {}
    for item in map_dataverse:
        if item in map_nesstar:
            map_ids[map_dataverse[item]] = map_nesstar[item]
        elif item.lower() in map_nesstar:
            map_ids[map_dataverse[item]] = map_nesstar[item.lower()]
        elif item.upper() in map_nesstar:
            map_ids[map_dataverse[item]] = map_nesstar[item.upper()] 

    for item in map_nesstar:
        if item in map_dataverse:
            map_ids_n_d[map_nesstar[item]] = map_dataverse[item]
        elif item.lower() in map_dataverse:
            map_ids_n_d[map_nesstar[item]] = map_dataverse[item.lower()]
        elif item.upper() in map_dataverse:
            map_ids_n_d[map_nesstar[item]] = map_dataverse[item.upper()]
        
    return map_ids, map_ids_n_d

def update_var_ddi(map_ids, map_ids_n_d, dataDscr_dataverse, dataDscr_nesstar, ns_var, ns):
    logging.debug("Start update_var_ddi")
    grps = dataDscr_nesstar.findall(ns + 'varGrp')

    #groups
    for var_grp in grps:
        grp_id = var_grp.get("ID")
        F1 = grp_id[-2:]
        if F1 == "F1":
            lng = len(grp_id)
            grp_id = grp_id[0:lng-2]
            var_grp.set('ID', grp_id)
        wgt_var = var_grp.get("var")
        #if wgt_var == None:
        #    wgt_var = var_grp.get("varGrp")
        if wgt_var != None:
            w_var = wgt_var.split(" ")
            w_str = ""
            for w in w_var:
                if w in  map_ids_n_d:
                    if w_str == "":
                        w_str = map_ids_n_d[w].strip()
                    else:
                        w_str = w_str + " " + map_ids_n_d[w].strip()
                else:
                    print(w)
            if w_str != "":
                var_grp.set('var', w_str)

        dataDscr_dataverse.append(var_grp)
    logging.info("Finished groups")
    #variables
    vars = dataDscr_dataverse.findall(ns_var + 'var')
    logging.info("Start variables")
    for var in vars:
       
        id_dataverse = var.attrib.get("ID")
        
        if id_dataverse in map_ids:
            id_nesstar = map_ids[id_dataverse]
           
            #weights
            
            var_nesstar = dataDscr_nesstar.find(ns + 'var[@ID="'+id_nesstar+'"]')
            wgt_var = var_nesstar.get("wgt-var")
            if wgt_var != None:
                
                w_var = wgt_var.split(" ")
                w_str = ""
                for w in w_var:
                    if w_str == "":
                        w_str = map_ids_n_d[w].strip()
                    else:
                        w_str = w_str + " " + map_ids_n_d[w].strip()
                if w_str != "":
                    var.set('wgt-var',w_str)
            wgt = var_nesstar.get("wgt")
            if wgt != None:
                var.set("wgt", wgt)
            

        #qstn
            qstn_nesstar = var_nesstar.find(ns + 'qstn')
            if qstn_nesstar != None:
               var.append(qstn_nesstar)
            notes_nesstar = var_nesstar.findall(ns + 'notes')
            for note in notes_nesstar:
               var.append(note)
        #frequency weight
            catgry_nesstar = var_nesstar.findall(ns + 'catgry')
            catgrys = var.findall(ns_var +"catgry")

            for catgry in catgry_nesstar:
                catStat_nesstar = catgry.find(ns + 'catStat[@wgtd="wgtd"]')
        #     #catStat_nesstar_type = catgry.find('ns:catStat[@type="freq"]',ns)
                if catStat_nesstar!=None:
                     catValu = catgry.find(ns + 'catValu')
                     if  catValu != None:
                         text = catValu.text
                         for ct in catgrys:
                             if ct.find(ns_var + 'catValu').text.strip() == text.strip():
                                 ct.append(catStat_nesstar)
                                 break
    logging.info("End update_var_ddi")
    return dataDscr_dataverse

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

def get_var_metadata_dataverse(dataset_id, datafile_id):
    logging.debug("Start get_var_metadata_dataverse")
    lock = check_lock(dataset_id)
    url = url_base
    if lock:
        url = url + "/api/access/datafile/{0}/metadata".format(datafile_id)
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            tree = ET.fromstring(resp.content)
            return tree
        else:
            logging.error("get_var_metadata_dataverse: dataset_id = {0} datafile_id = {1} url = {2}".format(dataset_id, datafile_id, url))
            return False
    else:
        logging.error("get_var_metadata_dataverse: dataset_id = {0} datafile_id = {1} url ={2} locking problem".format(dataset_id, datafile_id,url))
        return False
def var_update_dataset(dataset_id, datafile_id, xml):
    logging.debug("Start var_update_dataset")
    #curl -H "X-Dataverse-key:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" -X PUT https://demo.dataverse.org/api/edit/24 --upload-file dct.xml
    f = open("demofile2.xml", "wb")
    f.write(xml)
    f.close()
    url = url_base + "/api/edit/" + str(datafile_id)
    logging.info(url)
    check = check_lock(dataset_id)
    if check == False:
        return check
    try:
        resp = requests.put(url, headers=headers, data=xml)
        if resp.status_code != 200:
            logging.error(resp.json())
            return False
    except Exception as e:
        logging.error("var_update_dataset: {0} {1}".format(str(e),url))
        return False
    return True

def publish_version(DOI, dataset_id):
    logging.debug("Start version")
    check = check_lock(dataset_id)
    if check == False:
        return check
    try:
        resp = api.publish_dataset(DOI, 'major')
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

def update_dataset(id, file_id, ddi_file):
    tree = ET.parse(ddi_file)
    root = tree.getroot()
    result = re.search('\{(.*)\}', root.tag)
    if result == None:
        ns = ''
    else:
        ns = result.group(0)

    var_xml = get_var_metadata_dataverse(id, file_id)
    if var_xml:
        result = re.search('\{(.*)\}', var_xml.tag)
        if result == None:
            ns_var = ''
        else:
            ns_var = result.group(0)
        # dataDscr_dataverse = get_var_metadata(tree)
        dataDscr_dataverse = var_xml.find(ns_var + "dataDscr")
        dataDscr_nesstar = get_var_metadata(ddi_file)
        map_name_id_dataverse = map_label_var(dataDscr_dataverse, ns_var)
        map_name_id_nesstar = map_label_var(dataDscr_nesstar, ns)
        ids_maps = var_ids_correspondence(map_name_id_nesstar, map_name_id_dataverse)
        logging.debug("Before update_var_ddi")
        dataDscr_dv_updated = update_var_ddi(ids_maps[0], ids_maps[1], dataDscr_dataverse, dataDscr_nesstar, ns_var,
                                             ns)
        logging.debug("After update var ddi")
        xml_updated = ET.ElementTree(dataDscr_dv_updated)
        # xml_updated.write("sample.xml",  encoding='utf8', xml_declaration=None, default_namespace=None, method='xml')
        xml_string = ET.tostring(dataDscr_dv_updated, encoding='utf8', method='xml')
        if var_update_dataset(id, file_id, xml_string):
            return True

    return False

if __name__ == '__main__':
    start_time = time.time()
    now = datetime.now()
    now_date = datetime.now()
    year = now_date.year
    month = now_date.strftime("%b")
    day = now_date.strftime("%d")

    filename_log = "dataverse-import-variable-meta_" + str(day) + "-" + month + "-" + str(year) + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info("Program started")
    list_file=config['NESSTAR']['list_file']
    logging.debug(list_file)
    dir = config['NESSTAR']['dir']
    csv_file = open(list_file)
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')

    for dataset in reader:
        id_xml = dataset[0]
        file_id = dataset[3]
        pid = dataset[2]
        ddi_file = dir + id_xml + ".xml"
        logging.info(pid)
        logging.info(ddi_file)
        logging.info("=====Started {0}======".format(pid))
        try:
            resp = api.get_dataset(pid)
            if resp.status_code == 200:
                #logging.info(resp.json()['data'])
                id = resp.json()['data']['id']
                if update_dataset(id, file_id, ddi_file):
                    #publish_version(pid,id)
                    logging.info("Updated {0}".format(pid))
                else:
                    logging.error("Cannot update vars {0}".format(pid))
            else:
                logging.error("Cannot get dataset:{0}".format(pid))
        except Exception as e:
            logging.error("Get dataset {0} exception:{1}".format(pid,str(e)))
        logging.info("=====Ended {0}======".format(pid))
    csv_file.close()
    logging.info("Program ended")
