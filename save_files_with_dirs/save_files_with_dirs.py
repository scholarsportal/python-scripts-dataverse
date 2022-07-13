import requests
import os

API_TOKEN = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
SERVER_URL = "https://borealisdata.ca"
PERSISTENT_ID = "doi:10.5683/SP3/PSWY62"

def main():
    print(f"Start downloading files for {PERSISTENT_ID}")
    headers = {'X-Dataverse-key': API_TOKEN}
    url = SERVER_URL + "/api/datasets/:persistentId//versions/:latest/files?persistentId=" + PERSISTENT_ID
    resp = requests.get(url, headers=headers)
    if not os.path.isdir("dataset"):
        os.mkdir("dataset")
    if resp.status_code == 200:
        data = resp.json()['data']
        for dataset in data:
            id = dataset["dataFile"]["id"]
            filename = dataset["dataFile"]["filename"]
            directoryLabel = dataset["directoryLabel"]
            dr = "dataset/" + directoryLabel
            name = dr + "/" + filename
            if not os.path.isdir(dr):
                os.makedirs(dr)

            url_file = SERVER_URL + "/api/access/datafile/" + str(id)
            r = requests.get(url_file, headers=headers)
            if r.status_code != 200:
                print(f"Cannot download {directoryLabel}/{filename} ")
                continue
            else:
                print(name)

            with open(name, 'wb') as f:
                f.write(r.content)
    else:
        print("Cannot get the dataset")


if __name__ == '__main__':
    main()
