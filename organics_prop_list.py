import os
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import datetime


from emailing import *
import pandas as pd

# authenticate google cloud API credentials
scope = ['https://spreadsheets.google.com/feeds']
DIRNAME = os.path.dirname(__file__)
credentials = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(DIRNAME, 'credentials.json'), scope)

# google docid and socrata asset publisher endpoint
docid = os.environ['organics_docid']
socrata_asset = 'https://data.austintexas.gov/resource/vpcu-8r94.json'


# headers for the socrata request. App token is system environmental variable
headers = {'Host': 'data.austintexas.gov',
            'Accept': """*/*""",
            'Content-Length': '6000',
            'Content-Type': 'application/json',
            'X-App-Token': os.environ['socrata_app_token']}

# Columns to drop from the google sheet download
drops = ['Associated Food Permit #',
                 'Property Tax ID',
                 'Alt Address (Customer Provided)',
                 'Biz City',
                 'Biz State',
                 'Actual Sq. Ftg.',
                 'OWNER MAILING CITY',
                 'OWNER MAILING STATE',
                 'OWNER MAILING ZIP',
                 'EMAIL ADDRESS (Food Permit)',
                 '2016 Food Permit',
                 'CBD?',
                 'Contact Name',
                 'Contact Position',
                 'Contact Address',
                 'Contact City',
                 'Contact State',
                 'Contact Zip',
                 'Contact Phone',
                 'Contact Email',
                 'Other Contact Info',
                 'ZWS Notes',
                 'ARR Notes',
                 '2017 ODP Contact Name',
                 '2017 ODP Contact Phone',
                 '2017 ODP Contact Email',
                 'Property RSN (DO NOT TOUCH)']


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
            # rename columns
            df.rename(index=str, columns={'Food Permit #': 'PERMIT NUMBER (FP)',
                                          'Reported Sq. Ftg.': 'Reported SQ FT (PERMIT)',
                                          'Business Name': 'PROPERTY NAME',
                                          'Biz Address': 'Physical ADDRESS',
                                          'Biz Zip': 'Physical ZIP',
                                          'OWNER MAILING ADDRESS': 'OWNER ADDRESS',
                                          'First Year Affected; (Oct. 1, 201X)': 'YEAR AFFECTED'}, inplace=True)
            # add string 'FP' to the permit number values
            df['PERMIT NUMBER (FP)'] = df['PERMIT NUMBER (FP)'].astype(str) + ' FP'
            # drop columns
            for d in drops:
                try:
                    df.drop(d, axis=1, inplace=True)
                except KeyError as e:
                    continue
            # gettin' janky, renaming again for some reason. Do as I say not as I do.
            df.columns = ['PERMIT NUMBER (FP)', 'PROPERTY NAME', 'Physical ADDRESS', 'Physical ZIP',
                          'Reported SQ FT (PERMIT)', 'YEAR AFFECTED', 'OWNER NAME', 'OWNER ADDRESS',
                          'ODP STATUS', 'Establisment Type']
    # last three rows of this sheet are not valuable
    df = df[:-3]
    # fill NaN (not a number) in dataframe with ''
    df.fillna('', inplace=True)
    # convert to dictionary for requests payload
    data = df.to_dict('records')
    # save to csv for initial manual upload of data to socrata. Manually uploading to set columns/schema of asset
    df.to_csv('master-list.csv')

    # perform replace using a put request. Authentication is socrata username and password with ownership of asset in question
    r = requests.put(socrata_asset, json=data, auth=(os.environ['socrata_user'], os.environ['socrata_pass']), headers=headers)
    return r.json()


def notify_complete(timer, response):
    """Sends email notification to confirm sheet has been synchronized."""
    sender = 'ARR_Automation@austintexas.gov'
    receiver = ['Thomas.Montgomery@austintexas.gov']
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

    s = smtplib.SMTP(os.environ['coa_mail_server'])

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
