__author__ = 'G'

import sys
import urllib
import pandas as pd
import re
import argparse
import json
import datetime
import hashlib

# url = "https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/463076/Detailed_LA_Level_Tables_201506.xlsx"
# output_path = "tempYHomeless.csv"
# sheet = "Section 1"
# required_indicators = ["e1b1a"]


def download(url, sheet, reqFields, outPath):
    homeReq = reqFields

    if len(homeReq) != 1:
        errfile.write(str(now()) + " Requested data " + str(homeReq).strip(
            '[]') + " don't match the excel file. This code is only for extracting data from filed 'e1b1a'. Please check the file at: " + str(
            url) + " . End progress\n")
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit("Requested data " + str(homeReq).strip(
            '[]') + " don't match the excel file. This code is only for extracting data from filed 'e1b1a'. Please check the file at: " + url)

    dName = outPath

    col = ['ecode', 'name', 'year', 'quarter', 'count', 'pkey']

    # open url
    socket = openurl(url)

    # operate this excel file
    logfile.write(str(now()) + ' excel file loading\n')
    print('excel file loading------')
    xd = pd.ExcelFile(socket)
    df = xd.parse(sheet)

    # find year and quarter
    listurl = (url.split('_'))
    iYQ = listurl[len(listurl) - 1]
    iYQ = (iYQ.split('.'))[0]
    iYear = iYQ[:4]
    iQuarter = int(int(iYQ[4:]) / 3)

    # indicator checking
    logfile.write(str(now()) + ' indicator checking\n')
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
        errfile.write(str(now()) + " Requested data " + str(homeReq).strip(
            '[]') + " don't match the excel file. Please check the file at: " + str(url) + " . End progress\n")
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit("Requested data " + str(homeReq).strip(
            '[]') + " don't match the excel file. Please check the file at: " + url)

    raw_data = {}
    for j in col:
        raw_data[j] = []

    # data reading
    logfile.write(str(now()) + ' data reading\n')
    print('data reading------')
    for i in range(restartIndex, df.shape[0]):
        for k in numCol:
            if re.match(r'E\d{8}$', str(df.index[i][0])):
                raw_data[col[0]].append(df.index[i][0])
                raw_data[col[1]].append(df.index[i][1])
                raw_data[col[2]].append(iYear)
                raw_data[col[3]].append(iQuarter)
                raw_data[col[4]].append(df.iloc[i][k])
    logfile.write(str(now()) + ' data reading end\n')
    print('data reading end------')

    # create primary key by md5 for each row
    logfile.write(str(now()) + ' create primary key\n')
    print('create primary key------')
    keyCol = [0, 2, 3]
    raw_data[col[-1]] = fpkey(raw_data, col, keyCol)
    logfile.write(str(now()) + ' create primary key end\n')
    print('create primary key end------')

    # save csv file
    logfile.write(str(now()) + ' writing to file\n')
    print('writing to file ' + dName)
    dfw = pd.DataFrame(raw_data, columns=col)
    dfw.to_csv(dName, index=False)
    logfile.write(str(now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now()) + ' finished\n')
    print("finished")

def openurl(url):
    try:
        socket = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        errfile.write(str(now()) + ' file download HTTPError is ' + str(e.code) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        errfile.write(str(now()) + ' file download URLError is ' + str(e.args) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('file download URLError = ' + str(e.args))
    except Exception:
        print('file download error')
        import traceback
        errfile.write(str(now()) + ' generic exception: ' + str(traceback.format_exc()) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('generic exception: ' + traceback.format_exc())

    return socket

def fpkey(data, col, keyCol):
    mystring = ''
    pkey = []
    for i in range(len(data[col[0]])):
        for j in keyCol:
            mystring += str(data[col[j]][i])
        mymd5 = hashlib.md5(mystring.encode()).hexdigest()
        pkey.append(mymd5)

    return pkey

def now():
    return datetime.datetime.now()


parser = argparse.ArgumentParser(
    description='Extract online Youth Homelessness Data Excel file Section 1 to .csv file.')
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

    logfile = open("log_tempYHomeless.log", "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open("err_tempYHomeless.err", "w")

    with open("config_tempYHomeless.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempYHomeless.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["sheet"], oConfig["reqFields"], oConfig["outPath"])
