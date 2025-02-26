import configparser
from datetime import datetime
import sys
import time
import xml.etree.ElementTree as ET
import re
import io

import requests
import pyDataverse.utils as utils
from pyDataverse.api import NativeApi, DataAccessApi
import csv
import json

config = configparser.ConfigParser()
config.read('config.ini')

cfg_dataverse = config['DATAVERSE']
api_token_origin = cfg_dataverse['api_token_origin']
url_base_origin = cfg_dataverse['url_base_origin']

headers_origin = {'X-Dataverse-key': api_token_origin}
api_origin = NativeApi(url_base_origin, api_token_origin )
data_api_origin = DataAccessApi(url_base_origin , api_token_origin)

def map_label_var(dataDscr, ns):
    print("Start map_label_var")
    map_name_id = {}
    vars = dataDscr.findall(ns + "var")
    for var in vars:
        #label = var.find("ns:labl",ns)
        ID = var.attrib.get("ID")
        name = var.attrib.get("name")
        map_name_id[name] = ID
    return map_name_id

def var_ids_correspondence(map_nesstar, map_dataverse ):
    print("Start var_ids_correspondence")
    print(map_nesstar)
    print(map_dataverse)
    map_ids = {}
    map_ids_n_d = {}
    for item in map_dataverse:
        if item in map_nesstar:
            map_ids[map_dataverse[item]] = map_nesstar[item]
        else:
            if item.upper() in map_nesstar:
                map_ids[map_dataverse[item]] = map_nesstar[item.upper()]
            else:
                if item.lower() in map_nesstar:
                    map_ids[map_dataverse[item]] = map_nesstar[item.lower()]

    for item in map_nesstar:
        if item in map_dataverse:
            map_ids_n_d[map_nesstar[item]] = map_dataverse[item]
        else:
            if item.lower() in map_dataverse:
                map_ids_n_d[map_nesstar[item]] = map_dataverse[item.lower()]
            else:
                if item.upper() in map_dataverse:
                    map_ids_n_d[map_nesstar[item]] = map_dataverse[item.upper()]
    return map_ids, map_ids_n_d

def update_var_ddi(map_ids, map_ids_n_d, dataDscr_dataverse, dataDscr_nesstar, ns_var, ns):
    print("Start update_var_ddi")
    print(map_ids)
    print(map_ids_n_d)
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
    #variables
    vars = dataDscr_dataverse.findall(ns_var + 'var')
    print(vars)
    for var in vars:
        id_dataverse = var.attrib.get("ID")
        print("Dataverse ID: ", id_dataverse)
        if id_dataverse in map_ids:
            id_nesstar = map_ids[id_dataverse]
            print("Nesstar ID: ", id_nesstar)
        else:
            continue

        #weights
        var_nesstar = dataDscr_nesstar.find(ns + 'var[@ID="'+id_nesstar+'"]')
        wgt_var = var_nesstar.get("wgt-var")
        if wgt_var != None:
            w_var = wgt_var.split(" ")
            w_str = ""
            for w in w_var:
                if w_str == "":
                    print(w)
                    if w in map_ids_n_d:
                        w_str = map_ids_n_d[w].strip()
                else:
                    if w in map_ids_n_d:
                        w_str = w_str + " " + map_ids_n_d[w].strip()
            if w_str != "":
                var.set('wgt-var',w_str)
        wgt = var_nesstar.get("wgt")
        if wgt != None:
            var.set("wgt", wgt)

        #qstn
        qstn_nesstar = var_nesstar.find(ns + 'qstn')
        if id_nesstar == 'v144829':
            print(ET.tostring(var_nesstar, encoding='unicode'))
            print("-----")
            print(ET.tostring(qstn_nesstar, encoding='unicode'))
        if qstn_nesstar != None:
            var.append(qstn_nesstar)
        notes_nesstar = var_nesstar.findall(ns + 'notes')
        for note in notes_nesstar:
            var.append(note)

        #universe
        universe_nesstar = var_nesstar.find(ns + 'universe')
        if universe_nesstar != None:
            var.append(universe_nesstar)

        #frequency weight
            catgry_nesstar = var_nesstar.findall(ns + 'catgry')
            catgrys = var.findall(ns_var + "catgry")

            for catgry in catgry_nesstar:
                catStat_nesstar = catgry.find(ns + 'catStat[@wgtd="wgtd"]')
                #     #catStat_nesstar_type = catgry.find('ns:catStat[@type="freq"]',ns)
                if catStat_nesstar != None:
                    catValu = catgry.find(ns + 'catValu')
                    if catValu != None:
                        text = catValu.text
                        for ct in catgrys:
                            if ct.find(ns_var + 'catValu').text.strip() == text.strip():
                                ct.append(catStat_nesstar)
                                break
    return dataDscr_dataverse

def get_var_metadata_dataverse(dataset_id, datafile_id):
    print("Start get_var_metadata_dataverse")
    lock = check_lock(dataset_id)
    url = url_base_origin
    if lock:
        url = url + "/api/access/datafile/{0}/metadata".format(datafile_id)
        resp = requests.get(url, headers=headers_origin)
        if resp.status_code == 200:
            tree = ET.fromstring(resp.content)
            return tree
        else:
            print("get_var_metadata_dataverse: dataset_id = {0} datafile_id = {1} url = {2}".format(dataset_id, datafile_id, url))
            return False
    else:
        print("get_var_metadata_dataverse: dataset_id = {0} datafile_id = {1} url ={2} locking problem".format(dataset_id, datafile_id,url))
        return False

def var_update_dataset(dataset_id, datafile_id, xml):
    print("Start var_update_dataset")
    #curl -H "X-Dataverse-key:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" -X PUT https://demo.dataverse.org/api/edit/24 --upload-file dct.xml

    url = url_base_origin + "/api/edit/" + str(datafile_id)
    check = check_lock(dataset_id)
    if check == False:
        return check
    try:
        resp = requests.put(url, headers=headers_origin, data=xml)
        if resp.status_code != 200:
            print(resp.json())
            return False
        else:
            print("Updated")
            print(resp.json())
    except Exception as e:
        print("var_update_dataset: {0} {1}".format(str(e),url))
        return False
    return True

def publish_version(DOI, dataset_id):
    print("Start version")
    check = check_lock(dataset_id)
    if check == False:
        return check
    try:
        resp = api_origin.publish_dataset(DOI, 'major')
        if resp.status_code == 200:
            check = check_lock(dataset_id)
            if check:
                return True
        else:
            print("publish_version func: Error publishing dataset {} status {}".format(DOI, str(resp.status_code)))
            return False
    except Exception as e:
        print("publish_version error. Error {}, dataset {} ".format(str(e), DOI))
        return False

    return True

def check_lock(dataset_id):
    time_start = datetime.now()
    print("Start check_lock")
    try:
        url = url_base_origin + "/api/datasets/{0}/locks".format(dataset_id)
        lock = requests.get(url, headers_origin)
        if lock.status_code == 503:
            print("503 - Server is unavailable")
            sys.exit()
        a = 0
        while len(lock.json()['data']) > 0:
            print("Lock {} times {} {}".format(str(a), dataset_id, lock.json()))
            print(lock.json())
            time.sleep(10)
            a = a + 1

            lock = requests.get(url, headers_origin)
            if lock.status_code == 503:
                print("503 - Server is unavailable")
                sys.exit()
            if lock.status_code != 200:
                print("check_lock func: lock status {} for {}".format(str(lock.status_code), dataset_id))
                return False
    except Exception as e:
        print("check_lock. Error {}, dataset {} ".format(str(e), dataset_id))
        return False
    time_end = datetime.now()
    t = (time_end - time_start)
    print("Dataset " + str(dataset_id) + " was locked " +  str(t.total_seconds()) + " sec")
    return True

def update_dataset(id, file_id_old, file_id_new):
    # tree = ET.parse(ddi_file)
    # root = tree.getroot()
    # result = re.search('\{(.*)\}', root.tag)
    # if result == None:
    #     ns = ''
    # else:
    #     ns = result.group(0)
    print("Draft id {0}".format(file_id_new))
    var_xml = get_var_metadata_dataverse(id, file_id_new)
    f = open("draft.xml", "w")
    f.write(ET.tostring(var_xml, 'unicode'))
    f.close()

    if var_xml != False and var_xml != None:
        result = re.search(r'{(.*)}', var_xml.tag)
        if result == None:
            ns_var = ''
        else:
            ns_var = result.group(0)
        # dataDscr_dataverse = get_var_metadata(tree)
        dataDscr_dataverse = var_xml.find(ns_var + "dataDscr")
        print("Published id {0}".format(file_id_old))
        var_xml_nesstar = get_var_metadata_dataverse(id, file_id_old)
        print(var_xml_nesstar)
        f = open("published.xml", "w")
        f.write(ET.tostring(var_xml_nesstar, encoding='unicode'))
        f.close()
        if var_xml_nesstar != False and var_xml_nesstar != None:
            result = re.search(r'{(.*)}', var_xml_nesstar.tag)
            if result == None:
                ns = ''
            else:
                ns = result.group(0)
        else:
            return False

        dataDscr_nesstar = var_xml_nesstar.find(ns + "dataDscr")
        map_name_id_dataverse = map_label_var(dataDscr_dataverse, ns_var)
        map_name_id_nesstar = map_label_var(dataDscr_nesstar, ns)
        ids_maps = var_ids_correspondence(map_name_id_nesstar, map_name_id_dataverse)
        print("Before update_var_ddi")
        dataDscr_dv_updated = update_var_ddi(ids_maps[0], ids_maps[1], dataDscr_dataverse, dataDscr_nesstar, ns_var,
                                             ns)
        print("After update var ddi")
        xml_updated = ET.ElementTree(dataDscr_dv_updated)
        xml_updated.write("sample.xml",  encoding='utf8', xml_declaration=None, default_namespace=None, method='xml')
        xml_string = ET.tostring(dataDscr_dv_updated, encoding='utf8', method='xml')

        if var_update_dataset(id, file_id_new, xml_string):
            return True

    return False

def update_citation(latest_version, row, doi):

    updated_fields = {}
    updated_fields['fields'] = []
    metadataBlocks = latest_version['metadataBlocks']
    fields = metadataBlocks['citation']['fields']
    access_to_sources = False
    notes = False
    note = row['Variable Revision in Metadata: Citation > Notes'].strip()
    for field in fields:
        if field['typeName'] == 'title':
            field['value'] = field['value'] + " " + row['Title > Additions']
            updated_fields['fields'].append(field)
        if field['typeName'] == 'dsDescription':  # Description is multiple
            value = field['value']
            for v in value:
                if 'dsDescriptionValue' in v:
                    print(v['dsDescriptionValue'])
                    print(v['dsDescriptionValue']['value'])
                    # v['dsDescriptionValue']['value'] = v['dsDescriptionValue']['value'] + " " + row[
                    #     'Revision Additions: Citation > Descriptions']
                    v['dsDescriptionValue']['value'] = row['Revision Additions: Citation > Descriptions']

            updated_fields['fields'].append(field)
        if field['typeName'] == 'notesText':
            notes = True
            if note != None and note != '':
                #print('notesText')
                #field['value'] = field['value'] + " " + row['Descriptions > Revision Additions']
                field['value'] = notes
                updated_fields['fields'].append(field)

        # if field['typeName'] == 'accessToSources':
        #     print('accessToSources')
        #     field['value'] = row['NOTE: Will not be adding this comment any more ']
        #     updated_fields['fields'].append(field)
        #     access_to_sources = True

    # if not access_to_sources:
    #     # acc = ('{"typeName": "accessToSources", "multiple": false, "typeClass": "primitive", "value": "' +
    #     #        row['NOTE: Will not be adding this comment any more '] + '"}')
    #     #
    #     # b = json.loads(acc)
    #     print('No access to sources')
    #     field_access = {}
    #     field_access['typeName'] = "accessToSources"
    #     field_access['multiple'] = False
    #     field_access['typeClass'] = "primitive"
    #     field_access['value'] = row['NOTE: Will not be adding this comment any more ']
    #     updated_fields['fields'].append(field_access)

    if not notes and note != None and note != '':
        print('No notes')
        field_notes = {}
        field_notes['typeName'] = "notesText"
        field_notes['multiple'] = False
        field_notes['typeClass'] = "primitive"
        field_notes['value'] = row['Variable Revision in Metadata: Citation > Notes']
        updated_fields['fields'].append(field_notes)

    print(json.dumps(updated_fields))
    url = url_base_origin + "/api/datasets/:persistentId/editMetadata?persistentId=" + doi + "&replace=true"
    print(url)

    print(updated_fields)

    resp = requests.put(url, data=json.dumps(updated_fields), headers=headers_origin)
    print(resp.json())
    print(resp.status_code)
    if resp.status_code == 200:
        return True
    else:
        return False
def update_file_metadata(tab_id_new, description):
    metadata_file = '{"description":' + '"' + description + '"}'
    url = url_base_origin + '/api/files/' + str(tab_id_new) + '/metadata'
    print(url)
    print(metadata_file)

    resp = api_origin.update_datafile_metadata(tab_id_new, metadata_file, False)
    print(resp)

def main():
    dir_prefix = "/mnt/c/Users/lubitchv/OneDrive - University of Toronto/Desktop/"
    with (open('/mnt/c/Users/lubitchv/OneDrive - University of Toronto/Desktop/LFS Rebasing Project V2.csv', newline='') as csvfile):
        reader = csv.DictReader(csvfile)

        for row in reader:
            print(row)

            doi = row['DOI in Dataverse (2022 backwards to 2006)']
            print(doi)
            filename = row['Replace with /path ']

            print(filename)
            #replace
            filename = filename.replace("I:\\Ethan\\",dir_prefix)
            print(filename)
            filename = filename.replace("\\", "/")
            print(filename)

            print(doi)
            doi = doi.split("=")[1]
            print(doi)

            resp = api_origin.get_dataset(doi, version="1.1")


            print(resp.json())
            tab_id_old = 0
            tab_file_old = {}
            tab_id_new = 0
            tab_file_new = {}
            if resp.status_code == 200:
                id = resp.json()['data']['id']
                latest_version = resp.json()['data']['latestVersion']
                print(latest_version)
                files = latest_version['files']
                dataset_id = latest_version['datasetId']
                print(files)
                print(latest_version)


                for file in files:
                    print(file)
                    #curl -H "X-Dataverse-key:$API_TOKEN" -X DELETE "$SERVER_URL/api/files/$ID"
                    dataFile = file['dataFile']
                    print(dataFile)
                    if dataFile['contentType'] == 'text/tab-separated-values':
                        tab_id_old = dataFile['id']
                        tab_file_old = file
                    delete_file_url = url_base_origin + "/api/files/" + str(dataFile['id'])
                    print(delete_file_url)
                    req = requests.delete(delete_file_url, headers=headers_origin)
                    print(req.status_code)
                    if not check_lock(dataset_id):
                        print("Could not delete dataset " + doi)
                        exit(0)


                print(doi)
                print(filename)
                resp = api_origin.upload_datafile(doi, filename)
                if resp.status_code == 200:
                    print(resp.json())
                    if check_lock(dataset_id):
                        print("Upload success")
                        resp = api_origin.get_dataset(doi)
                        draft = resp.json()['data']['latestVersion']
                        files = draft['files']
                        for file in files:
                            print(file)
                            # curl -H "X-Dataverse-key:$API_TOKEN" -X DELETE "$SERVER_URL/api/files/$ID"
                            dataFile = file['dataFile']
                            print(dataFile)

                            if dataFile['contentType'] == 'text/tab-separated-values':
                                tab_id_new = dataFile['id']
                                tab_file_new = file
                        print(id)
                        print(tab_id_old)
                        print(tab_id_new)
                        update_dataset(id, tab_id_old, tab_id_new)
                        update_file_metadata(tab_id_new, row['File Descrtiption'])

                    else:
                        print("Upload failed")
                        continue
                else:
                    continue

                update_citation(latest_version, row, doi)
                exit(0)
            else:
                exit(0)

if __name__ == '__main__':
    main()

