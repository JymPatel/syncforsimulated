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
            
            # get file
            file_list = load_data(file['id'], type='csv')
            if 'id' in file_list[0]:
                csv_type = 'cash recipts'
            elif 'NetIncome' in file_list[0]:
                csv_type = 'income statement'
            
            
            new_sheet_data = []

            if csv_type == 'cash recipts':
                for row in file_list:
                    if automation['sync_data']['restaurant'] == []:
                        break
                    newRow = []
                    for item in automation['sync_data']['restaurant']:
                        newRow.append(get_restaurant(row, item))
                    new_sheet_data.append(newRow)
            elif csv_type == 'income statement':
                pass #TODO
            
            # update sheet
            cf.update_sheet(keyname_json, automation['sheet_id'], automation['worksheet_name'], new_sheet_data)

            synced_data['files'][automation_no].append(file['id'])

        print('done!!')


    # save data
    save_data({"transactions": synced_transactions, "files": synced_files}, synced_data_id)


def update_sheet(file_id, worksheet_name, array):
    for row in array:
        pass

def get_restaurant(row, item):
    if item == 'id':
        return row[0]
    if item == 'timestamp':
        return row[1]
    if item == 'category':
        return row[2]
    if item == 'revenue':
        return row[3]
    if item == 'description':
        return row[4]


def load_data(file_id, type='json'):
    # get automations
    # TODO create algo efficient to directlty convert binary_file to dict using json.load()
    binary_file = cf.download_file(keyname_json, file_id)
    with open('tmpfile', 'wb') as f:
        f.write(binary_file)
        f.close()
    with open('tmpfile', 'r') as f:
        if type == 'json':
            dict = json.load(f)
            f.close()
            return dict
        if type == 'csv':
            dict = csv.reader(f)
            list = [row for row in dict]
            f.close()
            return list

    
    return dict

def save_data(dict, file_id):
    with open('tmpfile', 'w') as f:
        json.dump(dict, f)
        f.close()
    cf.update_file(keyname_json, 'tmpfile', file_id)

if __name__ == "__main__":
    main()