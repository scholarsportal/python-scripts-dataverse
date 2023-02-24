import csv
import hashlib
list_file = "files.csv"


def get_checksum(filename, hash_function):

    hash_function = hash_function.lower()

    with open(filename, "rb") as f:
        bytes = f.read()  # read file as bytes
        if hash_function == "md5":
            readable_hash = hashlib.md5(bytes).hexdigest()
        elif hash_function == "sha256":
            readable_hash = hashlib.sha256(bytes).hexdigest()

    return readable_hash

def main():
    csv_file = open(list_file)
    reader = csv.reader(csv_file, delimiter=',', escapechar='\\', quotechar='"')

    for file in reader:


    csv_file.close()
if __name__ == '__main__':
    main()
