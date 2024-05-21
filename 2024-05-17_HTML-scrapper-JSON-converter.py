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

#%% Load CS sample reports

reports_sample_df = pd.read_csv('/home/skaiser/file/path/here/01_input_data/CS_AK/2024-05-17_CS-sample.csv')
#%%
# Function to fetch the HTML content with random delay
def fetch_html_with_delay(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    delay = random.uniform(1, 5)  # Random delay between 1 to 5 seconds
    time.sleep(delay)
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.text

# Step 3: Convert the extracted HTML to JSON format
def html_to_json(html_string):
    soup = BeautifulSoup(html_string, 'html.parser')
    data = []
    for idx, tag in enumerate(soup.find_all(['p', 'div', 'span'])):  # Adjust tags as needed
        text = tag.get_text(strip=True)
        if text:
            data.append({"id": idx, "text": text})
    return data

#%%

# Adding columns for entry_html and entry_json if they don't exist
if 'entry_html' not in reports_sample_df.columns:
    reports_sample_df['entry_html'] = None
if 'entry_json' not in reports_sample_df.columns:
    reports_sample_df['entry_json'] = None
    
#%%

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
    json_data = html_to_json(extracted_html)
    reports_sample_df.at[idx, 'entry_json'] = json.dumps(json_data, indent=2)
#%%
# Display the updated DataFrame
print(reports_sample_df)
