#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 17 16:19:58 2024

@author: skaiser
"""


import requests
import pandas as pd 
from bs4 import BeautifulSoup
import json
import time
import random
import datetime
import re
#%% paths

path_input = '/home/skaiser/Documents/03_AWI/03_ContNA/01_input_data'
path_interim = '/home/skaiser/Documents/03_AWI/03_ContNA/02_interim_data'
path_results = '/home/skaiser/Documents/03_AWI/03_ContNA/03_results'
path_figures = '/home/skaiser/Documents/03_AWI/03_ContNA/04_figures'

#%% set current date

today = datetime.date.today()

#%% Load CS sample reports

reports_sample_df = pd.read_csv(path_input + '/CS_AK/2024-05-17_CS-sample.csv')
#%%
# Function to fetch the HTML content with random delay
def fetch_html_with_delay(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'} # Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0
    delay = random.uniform(1, 5)  # Random delay between 1 to 5 seconds
    time.sleep(delay)
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.text

# Step 3: Convert the extracted HTML to JSON format
def html_to_json(html_string, div_ids):
    soup = BeautifulSoup(html_string, 'html.parser')
    data = []
    div_elements = soup.find_all('div', id=div_ids)

    # Iterate over the found div elements
    for idx, div in enumerate(div_elements):
        # Access the div element's content
        content = div.get_text(separator = ' | ', strip = True)
        print(f"ID {idx}: {div['id']}, Content: {content}")
        if content:
            data.append({"id": idx, "div_id": div['id'], "content": content})
    return data


#%%

# Adding columns for entry_html and entry_json if they don't exist
if 'entry_html' not in reports_sample_df.columns:
    reports_sample_df['entry_html'] = None
if 'entry_json' not in reports_sample_df.columns:
    reports_sample_df['entry_json'] = None
    
#%%

div_ids = ["site-chronology", "ic-colsure-details", "documents", "associated-sites"]

# Loop through each row in the DataFrame and process the URL
for idx, row in reports_sample_df.iterrows():
    url = row['Site_Repor']
    print(f"Processing URL: {url}")
    
    # Fetch the HTML content
    html_content = fetch_html_with_delay(url)
    
    # Parse the HTML and extract the relevant section
    soup = BeautifulSoup(html_content, 'html.parser')
    site_report_section = soup.find('div', id='SiteReportSections')
    extracted_html = str(site_report_section)

    # If site_report_section is None, skip this row
    if site_report_section is None:
        print(f"No 'SiteReportSections' found for URL: {url}")
        continue

    # Save the extracted HTML to the DataFrame
    reports_sample_df.at[idx, 'entry_html'] = extracted_html

    # Convert the extracted HTML to JSON and save it to the DataFrame
    json_data = html_to_json(extracted_html, div_ids)
    reports_sample_df.at[idx, 'entry_json'] = json.dumps(json_data, indent=2)
#%%
# Display and save the updated DataFrame
print(reports_sample_df)
reports_sample_df.to_csv(path_interim + "/CS_AK/%s_CS_sample_webscraped.csv" %str(today))

#%%


    

        