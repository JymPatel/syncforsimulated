from enum import auto
import sys
import csv
import json
import cloud_functions as cf

# TODO remove it if no need at end
# # tmp variables 
# ignored_files = ['transactions.json', 'synced_data.json']

# global variables
keyname_json = sys.argv[1]
run_type = sys.argv[2]
data_folder_id = '1r8_0_JwiBZ7V_pCSSr08nhrPLHzFl-Ir'
synced_data_id = '10M77XlvS4F6hVhxpSTX-ajNn1XqbWefJ'
automations_file_id = '10UW3wdS6myrvCxmO8B_4GyuyKEUJxYeS'


def main():
    # load data
    automations = load_data(automations_file_id)
    synced_data = load_data(synced_data_id)

    synced_transactions = synced_data["transactions"]
    synced_files = synced_data["files"]


    for automation_no in automations.keys():
        print(f'working on automation {automation_no}')


        for x in ["transactions", "files"]:
            if automation_no not in synced_data[x].keys():
                synced_data[x][automation_no] = []
        
        automation = automations[automation_no]
        if automation['status'] != run_type:
            continue
        
        query="'"+ automation['data_folder_id'] + "' in parents and mimeType='text/csv'"
        files = cf.search_file(keyname_json, query=query)

        for file in files:
            if file['id'] in synced_files[automation_no]:
                print(f"skipping file with filename {file['name']} and fileId {file['id']} as is already synced")
                continue
            print(f"working on file {file['id']}")
            # code here

            synced_data['files'][automation_no].append(file['id'])

        print('done!!')


    # save data
    save_data(synced_data, synced_data_id)


def load_data(file_id):
    # get automations
    # TODO create algo efficient to directlty convert binary_file to dict using json.load()
    binary_file = cf.download_file(keyname_json, file_id)
    with open('tmpfile', 'wb') as f:
        f.write(binary_file)
        f.close()
    with open('tmpfile') as f:
        dict = json.load(f)
        f.close()
    
    return dict

def save_data(dict, file_id):
    with open('tmpfile', 'w') as f:
        json.dump(dict, f)
        f.close()
    cf.update_file(keyname_json, 'tmpfile', file_id)

if __name__ == "__main__":
    main()