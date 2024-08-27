#!/usr/bin/env python3
import sys
import requests
import time
import json
import tempfile
from pandas_ods_reader import read_ods

def download_ods(url):
    """Download the ODS file and return the path to the temporary file."""
    tmpfile = tempfile.mktemp(suffix=".ods")
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Ensure we notice bad responses

    with open(tmpfile, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)

    return tmpfile

def read_ods_to_df(file_path):
    """Read the ODS file and return a DataFrame."""
    return read_ods(file_path)

def filter_us_isins(df):
    """Filter the DataFrame to get only US ISINs."""
    return df.query('`Ceased to be an RF on` == "—" & `ISIN No`.str.startswith("US")', engine='python')

def send_request(isins):
    """Send requests to the OpenFIGI API and return results and failures."""
    URL = "https://api.openfigi.com/v3/mapping"
    req = [{"idType": "ID_ISIN", "idValue": isin, "exchCode": "US"} for isin in isins]
    headers = {"Content-Type": "application/json"}

    response = requests.post(url=URL, json=req, headers=headers)
    response.raise_for_status()  # Ensure we notice bad responses

    resp = response.json()
    assert len(resp) == len(req), "Response length does not match request length"

    output = {}
    failures = {}
    for idx, isin in enumerate(isins):
        if "warning" in resp[idx] or "error" in resp[idx]:
            failures[isin] = f"Failed: {resp[idx]}"
        elif "data" in resp[idx]:
            output[isin] = resp[idx]["data"][0]

    return output, failures

def main():
    ods_url = "https://assets.publishing.service.gov.uk/media/66c44db32e8f04b086cdf40b/approved-offshore-reporting-funds-list.ods"
    tmpfile = download_ods(ods_url)
    df = read_ods_to_df(tmpfile)
    df_filtered = filter_us_isins(df)
    isins = list(df_filtered["ISIN No"])

    output = {}
    failures = {}
    batch_size = 10
    for idx in range(0, len(isins), batch_size):
        chunk = isins[idx:idx + batch_size]
        print(f"Processing chunk {idx // batch_size + 1}/{(len(isins) + batch_size - 1) // batch_size}", file=sys.stderr)

        chunk_res, chunk_failures = send_request(chunk)
        output.update(chunk_res)
        failures.update(chunk_failures)
        time.sleep(3)  # Rate limit sleep

    for isin_data in output.values():
        print(f"{isin_data['ticker']} US Equity,{isin_data['name']}")

if __name__ == "__main__":
    main()