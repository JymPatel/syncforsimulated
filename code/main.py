import sys
import csv
import json
import cloud_functions as cf


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

    # start automations
    for automation_no in automations.keys():
        print(f'working on automation {automation_no}')

        # check for synced data
        for x in ["transactions", "files"]:
            if automation_no not in synced_data[x].keys():
                synced_data[x][automation_no] = []
        
        # get automation
        automation = automations[automation_no]
        if automation['status'] not in run_type:
            continue
        
        # remove all synced data if automation is of 'testing'
        if automation['status'] == 'testing':
            synced_transactions[automation_no] = []
            synced_files[automation_no] = []

        # search for csv files
        query="'"+ automation['data_folder_id'] + "' in parents and mimeType='text/csv'"
        files = cf.search_file(keyname_json, query=query)


            
        # create new sheet data
        new_sheet_data = []

        # sync data for every csv file
        for file in files:

            if file['id'] in synced_files[automation_no]:
                print(f"skipping file with filename {file['name']} and fileId {file['id']} as is already synced")
                continue
            print(f"working on file {file['id']}")
            
            # download file
            file_list = load_data(file['id'], type='csv')
            if 'id' in file_list[0].keys():
                csv_type = 'cash recipts'
            elif 'NetIncome' in file_list[0].keys():
                csv_type = 'income statement'
            elif 'Retained Earnings' in file_list[0].keys():
                csv_type = 'balance sheet'

            # cash receipt csv file
            if csv_type == 'cash recipts':
                # restaurant
                for row in file_list:
                    if automation['sync_data']['restaurant'] == []:
                        break
                    if ( 'restaurant revenue' != row['Description'].lower() ) or ( row['id'] in synced_transactions[automation_no] ):
                        continue
                    newRow = []
                    for item in automation['sync_data']['restaurant']:
                        newRow.append(get_restaurant(row, item.lower()))
                    new_sheet_data.append(newRow)
                    synced_transactions[automation_no].append(row['id'])
            
            # income statement csv file
            elif csv_type == 'income statement':
                pass

            # balance sheet csv file
            elif csv_type == 'balance sheet':
                for row in file_list:
                    if automation['sync_data']['balance_sheet'] == []:
                        break
                    if row['Timestamp'] in synced_transactions[automation_no]:
                        continue
                    newRow = []
                    for item in automation['sync_data']['balance_sheet']:
                        newRow.append(get_balance(row, item.lower()))
                    new_sheet_data.append(newRow)
                    synced_transactions[automation_no].append(row['Timestamp'])
            
    

            synced_files[automation_no].append(file['id'])
        
        # update sheet
        cf.update_sheet(keyname_json, automation['sheet_id'], automation['worksheet_name'], new_sheet_data)
        print('done!!')


    # save data
    save_data({"transactions": synced_transactions, "files": synced_files}, synced_data_id)



def get_restaurant(row, item):
    if item == 'id':
        return row['id']
    # covert to date and time
    if item == 'date':
        return row['Timestamp'].split('T')[0]
    if item == 'time':
        return row['Timestamp'].split('T')[1].split('.')[0]
    # whole timestamp
    if item == 'timestamp':
        return row['Timestamp']
    # useful restaurant data
    details = json.loads(row['Details'])
    if item == 'revenue':
        return row['Money']
    if item == 'cogs':
        return details['COGS']
    if item == 'profit':
        return details['profit']
    if item == 'occupancy':
        return details['occupancy']
    if item == 'wages':
        return details['wages']
    if item == 'rating':
        return details['rating']
    # extras
    if item == 'category':
        return row['Category']
    if item == 'description':
        return row['Description']
    if item.split(' ')[0] == '':
        return '-'

def get_balance(row, item):
    if item == 'date':
        return row['Timestamp'].split('T')[0]
    if item == 'company_value':
        return int(row['Contributed Capital']) + int(row['Retained Earnings'])


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
            list = csv.DictReader(f)
            new_list = [row for row in list]
            f.close()
            return new_list

    
    return dict

def save_data(dict, file_id):
    with open('tmpfile', 'w') as f:
        json.dump(dict, f)
        f.close()
    cf.update_file(keyname_json, 'tmpfile', file_id)

if __name__ == "__main__":
    main()