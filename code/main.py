import sys
import json
import time
import gspread
import csv
from datetime import datetime
import drive_functions as df
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials


def update_data(input_file, sheet_id, sheet_sync_json):
    # authentication
    print('authenticating ...')
    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

    creds = Credentials.from_service_account_file(sheet_sync_json, scopes=scope)
    client = gspread.authorize(creds)

    # open sheet
    print('opening google sheet ...')
    sheet = client.open_by_key(sheet_id)
    try:
        worksheet = sheet.worksheet('synced_data')
        sheet.del_worksheet(worksheet) #TODO add something
        
        print('creating new worksheet ...')
        worksheet = sheet.add_worksheet('synced_data', 2, 10)
        issueMessage = 'if you have any issues please contact me jympatel@yahoo.com OR "SIMULATED COMPANY" in simcompanies.com'
        worksheet.append_row(['id', 'date', 'time', 'revenue', 'COGS', 'wages', 'occupancy', 'rating', 'profit', issueMessage], 2)

    except gspread.exceptions.WorksheetNotFound:
        print('creating new worksheet ...')
        worksheet = sheet.add_worksheet('synced_data', 2, 10)
        issueMessage = 'if you have any issues please contact me jympatel@yahoo.com OR "SIMULATED COMPANY" in simcompanies.com'
        worksheet.append_row(['id', 'date', 'time', 'revenue', 'COGS', 'wages', 'occupancy', 'rating', 'profit', issueMessage], 2)

    # get data
    print('adding data')
    csv_file = input_file
    with open(csv_file, 'r') as file:
        list = csv.reader(file)
        i = 0
        for row in list:
            insertRow = []
            if 'restaurant revenue' in row[4].lower():
                insertRow.append(row[0])
                insertRow.append(row[1].split('T')[0])
                insertRow.append(row[1].split('T')[1].split('.')[0])
                insertRow.append(row[3])
                insertRow.append(row[-1].split('", "')[0].split('{"COGS": "$')[-1])
                insertRow.append(row[-1].split('", "')[1].split('wages": "$')[-1])
                insertRow.append(row[-1].split('", "')[2].split('occupancy": "')[-1].split('%')[0])
                insertRow.append(row[-1].split('", "')[3].split('rating": "')[-1])
                insertRow.append(row[-1].split('", "')[4].split('profit": "$')[-1].split('"')[0])
                worksheet.append_row(insertRow, 2)
                time.sleep(1)
                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")
                print("Current Time =", current_time, i)
                i += 1


# tmp variables
usedfiles = []

drive_sync_json = '../local_secrets/drive-sync.json' 
sheet_sync_json = '../local_secrets/s4s.json'
# variables
# drive_sync_json = sys.argv[1]
# sheet_sync_json = sys.argv[2]
data_folder = '1r8_0_JwiBZ7V_pCSSr08nhrPLHzFl-Ir'

# get every folder we have
print('\nsearching folders ...')
folders = df.search_file(drive_sync_json, query='mimeType = "application/vnd.google-apps.folder"')
print('\ngetting internal files ...')
internal_files = df.search_file(drive_sync_json, query=f"'{data_folder}' in parents")
print(internal_files)

def main():
    # sync data for every folder
    for folder in folders:
        print(f"\nstarting sync for {folder['name']} {folder['id']} ...")

        print('finding settings for current folder ...')
        for file in internal_files:
            if str(folder['id']) in file['name']:
                print('found!!')
                with open('tmpfile', 'wb') as f:
                    f.write(df.download_file(drive_sync_json, real_file_id=file['id']))
                    f.close()
                with open('tmpfile') as f:
                    settings = json.load(f)
                print('collected automation settings!!')

        print('getting all your csv files ...')
        files = df.search_file(drive_sync_json, query=f"'{folder['id']}' in parents")
        for file in files:
            if '.csv' not in file['name']:
                continue
            if file['id'] in usedfiles:
                continue
            usedfiles.append(file['id'])

            tmpfile = df.download_file(drive_sync_json, file['id'])
            with open('tmpfile', 'wb') as f:
                f.write(tmpfile)
            print(file)
            with open('tmpfile', 'r') as f:
                tmpfile = csv.reader(f)
            update_data('tmpfile', settings['sheet_id'], sheet_sync_json)


if __name__ == '__main__':
    main()