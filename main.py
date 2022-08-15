import sys
import time
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
from datetime import datetime

# authentication
print('authenticating ...')
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(sys.argv[1], scope)
client = gspread.authorize(creds)

# open sheet
print('opening google sheet ...')
sheet = client.open_by_key(sys.argv[2])
try:
    worksheet = sheet.worksheet('synced_data')
    sheet.del_worksheet(worksheet)
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
csv_file = sys.argv[3]
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