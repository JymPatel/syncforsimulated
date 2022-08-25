from __future__ import print_function
import io
import csv
import gspread
import time
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials


def search_file(jsonfile, query=None):

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(jsonfile, scopes=scopes)

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)
        files = []
        page_token = None
        while True:
            # pylint: disable=maybe-no-member
            response = service.files().list(q=query).execute()
            for file in response.get('files', []):
                # Process change
                print(F'        found file: {file.get("name")}, {file.get("id")}')
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    except HttpError as error:
        print(F'An error occurred: {error}')
        files = None

    return files


def download_file(jsonfile, real_file_id):

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(jsonfile, scopes=scopes)

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        file_id = real_file_id

        # pylint: disable=maybe-no-member
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(F'        Download {int(status.progress() * 100)}.')

    except HttpError as error:
        print(F'An error occurred: {error}')
        file = None

    return file.getvalue()


def update_data(jsonfile, input_file, sheet_id, synced_transactions, building):

    # authentication
    print('authenticating ...')
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(jsonfile, scopes=scope)
    client = gspread.authorize(creds)

    # open sheet
    print('opening google sheet ...')
    sheet = client.open_by_key(sheet_id)
    try:
        worksheet = sheet.worksheet('synced_data')

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
        if building == 'restaurant':
            for row in list: 
                if row[0] in synced_transactions:
                    continue
                synced_transactions.append(row[0])
                insertRow = []
                if 'restaurant revenue' in row[4].lower():
                    insertRow.append(row[0])
                    insertRow.append(row[1].split('T')[0])
                    insertRow.append(row[1].split('T')[1].split('.')[0])
                    insertRow.append(row[3])
                    insertRow.append(row[-1].split('", "')[0].split('{"COGS": "$')[-1])
                    insertRow.append(row[-1].split('", "')[1].split('wages": "$')[-1])
                    insertRow.append(
                        row[-1].split('", "')[2].split('occupancy": "')[-1].split('%')[0])
                    insertRow.append(row[-1].split('", "')[3].split('rating": "')[-1])
                    insertRow.append(row[-1].split('", "')[4].split('profit": "$')[-1].split('"')[0])
                    worksheet.append_row(insertRow, 2)
                    time.sleep(1)
        if building == 'sales_office':
            for row in list:
                if row[0] in synced_transactions:
                    continue
                insertRow = []

    return synced_transactions


def delete_file(jsonfile, file_id):

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(jsonfile, scopes=scopes)

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        # delete file
        service.files().delete(fileId=file_id).execute()

    except HttpError as error:
        print(F'An error occurred: {error}')


def update_file(jsonfile, file_name, file_id):
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(jsonfile, scopes=scopes)

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        file = service.files().get(fileId=file_id).execute()
        print(file)

        file_metadata = {'name': file['name']}
        media = MediaFileUpload(file_name ,mimetype=file['mimeType'])
        # pylint: disable=maybe-no-member
        file = service.files().update(fileId=file_id, media_body=media, fields='id').execute()
        print(F'File ID: {file.get("id")}')

    except HttpError as error:
        print(F'An error occurred: {error}')
        file = None

    return file.get('id')

def update_sheet(jsonfile, sheet_id, worksheet_name, rows):
    # authentication
    print('authenticating ...')
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(jsonfile, scopes=scope)
    client = gspread.authorize(creds)
    
    # open google sheet
    sheet = client.open_by_key(sheet_id)
    try:
        worksheet = sheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet.add_worksheet(worksheet_name, 1, 26)
        worksheet = sheet.worksheet(worksheet_name)
    worksheet.append_rows(rows)


def upload_basic(jsonfile, name, parent_id, mime_type, file_name):

    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(jsonfile, scopes=scopes)


    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {'name': name, 'parents': parent_id}
        media = MediaFileUpload(file_name ,mimetype=mime_type)
        # pylint: disable=maybe-no-member
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(F'File ID: {file.get("id")}')

    except HttpError as error:
        print(F'An error occurred: {error}')
        file = None

    return file.get('id')