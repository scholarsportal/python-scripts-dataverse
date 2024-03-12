import requests
url_base = 'https://borealisdata.ca'
rows = 1000
start = 0
condition = True  # emulate do-while

def get_file(file_id):
    url = url_base + 'api/file/' + file_id
    requests.get(url)

def main():
    fileIds = set()
    my_file = open('fileIds.csv')
    for line in my_file:
        file_id = line.strip()
        #print(line)
        if file_id.isnumeric():
            fileIds.add(int(file_id))
            url = url_base + 'api/file/' + file_id
            resp = requests.get(url)
            print(resp.json())



    # while (condition):
    #     url = url_base + '/api/search?q=*&type=file&per_page=1000' + "&start=" + str(start)
    #     resp = requests.get(url)
    #     data = resp.json()
    #     total = data['data']['total_count']
    #     for item in data['data']['items']:
    #         file_id = int(item['file_id'].strip())
    #         print(file_id)
    #         if file_id in fileIds:
    #             doi = item['dataset_persistent_id']
    #             dataset_name = item['dataset_name']
    #             dataset_id = item['dataset_id']
    #             #print(url_base + '/api/datasets/' + str(dataset_id))
    #             resp_dataset = requests.get(url_base + '/api/datasets/' + str(dataset_id))
    #             #print(resp_dataset.json())
    #     start = start + rows
    #     print(start)
    #     print(total)
    #     condition = start < total





if __name__ == "__main__":
    main()