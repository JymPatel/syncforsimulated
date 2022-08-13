import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint

# authentication
print('authenticating ...')
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("s4s-datasyncer.json", scope)
client = gspread.authorize(creds)

# open sheet
print('opening google sheet ...')
sheet = client.open_by_key(input("KEY : "))
try:
    worksheet = sheet.worksheet('synced_data')
except gspread.exceptions.WorksheetNotFound:
    worksheet = sheet.add_worksheet('synced_data', 2, 9)

# get data
print('adding data')
csv_file = input('PATH TO CSV : ')
with open(csv_file, 'r') as file:
    list = csv.reader(file)
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