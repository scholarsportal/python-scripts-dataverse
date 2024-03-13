import requests
import csv
import os

url_base = 'https://borealisdata.ca'

def main():
    NUMBER_EMPTY_FILES = 0
    csvfile = open('datasets_from_files.csv', 'w', newline='')
    my_writer = csv.writer(csvfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    my_writer.writerow(['FileId', 'DOI', 'Dataset Name', 'Dataverse Alias', 'Dataverse Name'])
    fileIds = set()
    my_file = open('fileIds.csv', mode='r', encoding='utf-8-sig')
    for line in my_file:
        file_id = line.rstrip()
        if file_id.isnumeric():
            fileIds.add(int(file_id))
            url = url_base + '/api/files/' + file_id
            resp = requests.get(url)
            if resp.status_code == 200:
                filename = resp.json()['data']['dataFile']['filename']
                url_search = url_base + '/api/search?q=' + '"' + filename + '"' + '&type=file&per_page=1000'
                resp_search = requests.get(url_search)
                if resp_search.status_code == 200:
                    data = resp_search.json()
                    total_count = data['data']['total_count']
                    foundFile = None
                    if total_count > 0:
                        for file in data['data']['items']:
                            if file['file_id'] == file_id:
                                foundFile = file
                                break
                        if foundFile != None:
                            dataset_id = foundFile['dataset_id']
                            dataset_name = foundFile['dataset_name']
                            global_id = foundFile['dataset_persistent_id']
                            url_dataset = url_base + '/api/search?q=' + '"' + global_id + '"' + '&type=dataset&per_page=1000'
                            resp_dataset = requests.get(url_dataset)
                            if resp_dataset.status_code == 200:
                                data = resp_dataset.json()
                                total_count = data['data']['total_count']
                                foundDataset = None
                                if total_count > 0:
                                    for dataset in data['data']['items']:
                                        if dataset['global_id'] == global_id:
                                            foundDataset = dataset
                                            break
                                    if foundDataset != None:
                                        dataverse_alias = foundDataset['identifier_of_dataverse']
                                        dataverse_name = foundDataset['name_of_dataverse']
                                        my_writer.writerow([file_id, global_id, dataset_name, dataverse_alias, dataverse_name])
                                            #print([file_id, global_id, dataset_name, dataverse_alias, dataverse_name])
                                    else:
                                        print("Cannot find dataset for fileId {0}".format(file_id))
                                else:
                                    print("Not found dataset {0} for fileId {1}".format(global_id,file_id))
                            else:
                                print("Not found dataset {0} for fileId {1}".format(global_id, file_id))
                                #print(resp_dataset.json())
                        else:
                            print("Cannot find fileId {0}".format(file_id))
                            #print(data)
                    else:
                        print("Not found filename {0} for fileId {1}".format(filename,file_id))
                        #print(resp_search.json())
            else:
                print("Cannot get fileId {0}".format(file_id))
                #print(resp.json())

        else:
            if file_id == '':
                NUMBER_EMPTY_FILES = NUMBER_EMPTY_FILES + 1
            else:
                print("FileId is not a number {0}".format(file_id))
    if NUMBER_EMPTY_FILES > 0:
        print("There are {0} number of emty file ids".format(NUMBER_EMPTY_FILES))
    csvfile.close()
    my_file.close()

if __name__ == "__main__":
    main()