from curses import keyname
import sys
import csv
import json
import cloud_functions as cf


# tmp variables 
usedfiles = []
# global variables
keyname_json = sys.argv[1]
data_folder = '1r8_0_JwiBZ7V_pCSSr08nhrPLHzFl-Ir'


def main():
    print('\nfinding useful files ...')
    folders = cf.search_file(keyname_json, query=f'mimeType = "application/vnd.google-apps.folder"')
    internal_files = cf.search_file(keyname_json, query=f"'{data_folder}' in parents")

    for folder in folders:
        print(f'\nworking on {folder["name"]} with id {folder["id"]} ...')
        print('finding settings file ...')

        for file in internal_files:
            if str(folder['id']) in file['name']:
                print('found!!')
                with open('tmpfile', 'wb') as f:
                    f.write(cf.download_file(keyname_json, real_file_id=file['id']))
                    f.close()
                with open('tmpfile') as f:
                    settings = json.load(f)
                print('collected automation settings!!')
        
            
        print('getting all your csv files ...')
        files = cf.search_file(keyname_json, query=f"'{folder['id']}' in parents")

        transactions_file_id = None
        for file in files:
            if file['name'] == 'transactions.json':
                with open('tmpfile', 'wb') as f:
                    f.write(cf.download_file(keyname_json, real_file_id=file['id']))
                    f.close()
                with open('tmpfile') as f:
                    transactions = json.load(f)
                print('collected automation settings!!')
                transactions_file_id = file['id']
                break
        else:
            transactions = {
                'synced': []
            }


        for file in files:
            if '.csv' not in file['name']:
                continue
            if file['id'] in usedfiles:
                continue
            usedfiles.append(file['id'])

            tmpfile = cf.download_file(keyname_json, file['id'])
            with open('tmpfile', 'wb') as f:
                f.write(tmpfile)
            print(file)
            with open('tmpfile', 'r') as f:
                tmpfile = csv.reader(f)
            transactions['synced'] = cf.update_data(keyname_json, 'tmpfile', settings['sheet_id'], transactions['synced'])

        # update transactions file
        if transactions_file_id:
            cf.delete_file(keyname_json, transactions_file_id)
        with open('tmpfile', 'w') as f:
            json.dump(transactions, f)
            f.close()

        cf.upload_basic(keyname_json, 'transactions.json', [folder['id']], 'application/json', 'tmpfile')

if __name__ == "__main__":
    main()