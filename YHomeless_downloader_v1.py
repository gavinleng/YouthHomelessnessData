__author__ = 'G'

import sys
import urllib
import pandas as pd
import re
import argparse
import json

# url = "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/463076/Detailed_LA_Level_Tables_201506.xlsx"
# output_path = "tempYHomeless.csv"
# sheet = "Section 1"
# required_indicators = ["e1b1a"]


def download(url, sheet, reqFields, outPath):
    homeReq = reqFields

    if len(homeReq) != 1:
        sys.exit("Requested data " + str(homeReq).strip(
            '[]') + " don't match the excel file. This code is only for extracting data from filed 'e1b1a'. Please check the file at: " + url)

    dName = outPath

    col = ['ecode', 'name', 'year', 'Quarter', 'Count']

    try:
        socket = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        sys.exit('excel download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        sys.exit('excel download URLError = ' + str(e.args))
    except Exception:
        print('excel file download error')
        import traceback
        sys.exit('generic exception: ' + traceback.format_exc())

    # operate this excel file
    xd = pd.ExcelFile(socket)
    df = xd.parse(sheet)

    listurl = (url.split('_'))
    iYQ = listurl[len(listurl)-1]
    iYQ = (iYQ.split('.'))[0]
    iYear = iYQ[:4]
    iQuarter = int(int(iYQ[4:])/3)

    print('indicator checking------')
    for i in range(df.shape[0]):
        numCol = []
        for k in homeReq:
            for j in range(df.shape[1]):
                if df.iloc[i][j] == k:
                    numCol.append(j)
                    restartIndex = i + 1

        if len(numCol) == len(homeReq):
            break

    if len(numCol) != len(homeReq):
        sys.exit("Requested data " + str(homeReq).strip(
            '[]') + " don't match the excel file. Please check the file at: " + url)

    raw_data = {}
    for j in col:
        raw_data[j] = []

    print('data reading------')
    for i in range(restartIndex, df.shape[0]):
        print('reading row ' + str(i))
        for k in numCol:
            if re.match(r'E\d{8}$', str(df.index[i][0])):
                raw_data[col[0]].append(df.index[i][0])
                raw_data[col[1]].append(df.index[i][1])
                raw_data[col[2]].append(iYear)
                raw_data[col[3]].append(iQuarter)
                raw_data[col[4]].append(df.iloc[i][k])

    # save csv file
    print('writing to file ' + dName)
    dfw = pd.DataFrame(raw_data, columns=col)
    dfw.to_csv(dName, index=False)
    print('Requested data has been extracted and saved as ' + dName)
    print("finished")

parser = argparse.ArgumentParser(description='Extract online Youth Homelessness Data Excel file Section 1 to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_YHomeless.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "url": "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/463076/Detailed_LA_Level_Tables_201506.xlsx",
        "outPath": "tempYHomeless.csv",
        "sheet": "Section 1",
        "reqFields": ["e1b1a"]
    }

    with open("config_YHomeless.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_YHomeless.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)
    print("read config file")

download(oConfig["url"], oConfig["sheet"], oConfig["reqFields"], oConfig["outPath"])
