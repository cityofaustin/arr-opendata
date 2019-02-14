import os
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import datetime


from emailing import *
import pandas as pd

scope = ['https://spreadsheets.google.com/feeds']
DIRNAME = os.path.dirname(__file__)
credentials = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(DIRNAME, 'credentials.json'), scope)

docid = '1jMTKQcsE8SmvwAfQMFtwCsr1ucDY6FJMr3wlKgD0Eio'
socrata_asset = 'https://data.austintexas.gov/resource/uic2-id33.json'
headers = {'Host': 'data.austintexas.gov',
            'Accept': """*/*""",
            'Authorization': '({}, {})'.format(os.environ['socrata_user'], os.environ['socrata_pass']),
            'Content-Length': '6000',
            'Content-Type': 'application/json',
            'X-App-Token': os.environ['socrata_app_token']}

schema_list = ['Property ID', 'Building Area (sqft)', 'Type I', 'Type II', 'UNITS (RP)',
                  'Year First Affected ; (Oct. 1, 201X)', 'Property Name', 'Street Address (TCAD) DO NOT EDIT',
                  'Situs Zip', 'Owner Name', 'Owner Address', 'Owner Address Line 2', 'Owner Address Line 3',
                  'Owner City', 'Owner State', 'Owner Zip+4']


def synchronize_gsheet():
    """This function downloads the google sheet, manipulates the data, and replaces the socrata asset"""
    # initialize google client
    gclient = gspread.authorize(credentials)
    # open spreadsheet using google client
    spreadsheet = gclient.open_by_key(docid)
    for i, worksheet in enumerate(spreadsheet.worksheets()):
        # we want the first sheet out of the workbook. Stops after the first (0)
        if i == 0:
            filename = docid + '-worksheet' + str(i) + '.csv'
            # encode as utf-8
            with open(filename, 'w', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(worksheet.get_all_values())
            # convert to spreadsheet pandas dataframe
            df = pd.read_csv(filename)

    columns = list(df)
    for c in columns:
        if c not in schema_list and c[:15] != 'FY19 ADP Status':
            df.drop(c, axis=1, inplace=True)
        else:
            continue

    df.columns = ['Property ID', 'Building Area (sqft)', 'Type I', 'Type II', 'UNITS (RP)',
                  'Year First Affected ; (Oct. 1, 201X)', 'Property Name', 'Street Address (TCAD) DO NOT EDIT',
                  'Situs Zip', 'Owner Name', 'Owner Address', 'Owner Address Line 2', 'Owner Address Line 3',
                  'Owner City', 'Owner State', 'Owner Zip+4', 'FY19 ADP Status']
    # fill NaN (not a number) in dataframe with ''
    df.fillna('', inplace=True)
    # convert to dictionary for requests payload
    data = df.to_dict('records')
    # save to csv for initial manual upload of data to socrata. Manually uploading to set columns/schema of asset
    df.to_csv('master-recycling-list.csv')

    # perform replace using a put request. Authentication is socrata username and password with ownership of asset in question
    r = requests.put(socrata_asset, json=data, auth=(os.environ['socrata_user'], os.environ['socrata_pass']), headers=headers)
    return r.json()


def notify_complete(timer, response):
    """Sends email notification to confirm sheet has been synchronized."""
    sender = 'ARR_Automation@austintexas.gov'
    receiver = ['Thomas.Montgomery@austintexas.gov', 'Nathan.Shaw-Meadow@austintexas.gov']
    # receiver = ['Thomas.Montgomery@austintexas.gov']
    now = datetime.datetime.now()
    time = str((now - timer))
    body = ("""This message was sent to confirm that the socrata asset:
                 \r\n - {}\r\n 
               Was synchronized with the google sheet docID:
                 \r\n - {}\r\n
               The Socrata API response was:
                 \r\n - {}\r\n
               The synchronization was completed in: 
                 \r\n - {}\r\n""".format(socrata_asset, docid, response, time))

    msg = MIMEText(body)
    msg['From'] = sender
    msg['To'] = ', '.join(receiver)
    msg['Subject'] = 'Open Data Synchronization'

    s = smtplib.SMTP('coamroute.austintexas.gov')

    s.sendmail(sender, receiver, msg.as_string())
    s.quit()


if __name__ == "__main__":
    try:
        start = datetime.datetime.now()
        response = synchronize_gsheet()
        notify_complete(start, response=response)

    except Exception as e:
        from traceback import format_exc
        tb = format_exc()
        print(tb)
        error_email(str(tb), 'Automation Error Alert', ['Thomas.Montgomery@austintexas.gov'],
                    os.path.basename(__file__),
                    str(datetime.datetime.now()))
