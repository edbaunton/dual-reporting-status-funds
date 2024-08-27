#!/usr/bin/env python3
import sys
import requests
import time
import json
import tempfile

import pandas_ods_reader

def read_ods_to_df():
    # return a pandas dataframe for the spreadsheet
    tmpfile = tempfile.mktemp()
    # Probably need to make sure that this correctly updates when this updates https://www.gov.uk/government/publications/offshore-funds-list-of-reporting-funds
    res = requests.get("https://assets.publishing.service.gov.uk/media/66c44db32e8f04b086cdf40b/approved-offshore-reporting-funds-list.ods", stream=True)
    res.raw.decode_content = True

    with open(tmpfile,"wb") as f:
        for chunk in res.iter_content(chunk_size=1024):
            f.write(chunk)
        f.close()
    f=pandas_ods_reader.read_ods(tmpfile)
    return f

# just yankee funds
res = read_ods_to_df().query('`Ceased to be an RF on` == "—" & `ISIN No`.str.startswith("US")', engine='python')
isins = list(res["ISIN No"])

def send_request(isins):
    URL = "https://api.openfigi.com/v3/mapping"
    req = []
    for isin in isins:
        req.append({"idType":"ID_ISIN","idValue":isin,"exchCode" :"US"}) # just composites
    resp_text = requests.post(url=URL,json=req).text
    resp = json.loads(resp_text) # TODO check code

    assert(len(resp) == len(req))
    output = {}
    failures = {}
    for idx, isin in enumerate(isins):
        if "warning" in resp[idx] or "error" in resp[idx]:
            failures[isin] = "Failed: " + str(resp[idx]) # TODO: log these at some point
        if "data" in resp[idx]:
            output[isin] = resp[idx]["data"][0]

    return output, failures

idx = 0
output = {}
failures = {}
while idx < len(isins):
    chunk = isins[idx:idx+10]
    idx+=10
    print(f"{idx}/{len(isins)}", file=sys.stderr)

    chunk_res, chunk_failures = send_request(chunk)
    output = output | chunk_res
    failures = failures | chunk_failures
    time.sleep(3)

for i in output.values():
    print(f"{i['ticker']} US Equity,{i['name']}")