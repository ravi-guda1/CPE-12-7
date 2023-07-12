import csv
import json
import os
def csv_to_json(csv_file_path, json_file_path):
    with open(csv_file_path, 'r') as csv_file:
        csv_data = csv.DictReader(csv_file)
        json_data = []
        for row in csv_data:
            json_data.append(row)

    with open(json_file_path, 'w') as json_file:
        json_file.write(json.dumps(json_data, indent=4))
    print("Data has conveted from csv to json")

def get_files_path(files_path,json_path):
    op=os.listdir(files_path)
    for each in op:
        temp_path=os.path.join(files_path,each)
        json_path1=each.split(".")[0]+".json"
        json_path2=os.path.join(json_path,json_path1)
        csv_to_json(temp_path,json_path2)

if __name__ == '__main__':
    get_files_path(files_path="C:/Users/ravikumar.g/Downloads/salesdataset",json_path="C:/Users/ravikumar.g/Downloads/sales_jsonset")
