import numpy as np
import pandas as pd
import requests
import io

"""
start_date: str - YYYY-MM-DD
end_date: str - YYYY-MM-DD
os_names: ios,android
kpis: list
param group: list
"""

class Adjust:
    start_date = None
    end_date = None
    kpis = None
    grouping = None
    app_token = None
    user_token = None
    string_groups = ['adgroup', 'network', 'creative', 'campaign']
    
    def __init__(self, app_token, user_token):
        self.app_token = app_token
        self.user_token = user_token
        
    def set_params(self, start_date, end_date, kpis, grouping):
        self.start_date = start_date
        self.end_date = end_date
        self.kpis = kpis
        self.grouping = grouping

    def get_params(self):
        url = 'http://api.adjust.com/kpis/v1/' + self.app_token
        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "kpis": ",".join(self.kpis),
            "user_token": self.user_token,
            "utc_offset":'+00:00',
            "grouping": ",".join(self.grouping),
            "attribution_type": "all"
        }
        return url, params

    def fetch_deliverables(self, save_raw):
        url, params = self.get_params()
        response = requests.get(url, params=params, headers = {'Accept': 'text/csv'})
        if save_raw:
            file_name = str(self.start_date) + '_to_' + str(self.end_date) + '_raw_deliverables_data.csv'
            csv_file = open(file_name, 'wb')
            csv_file.write(response.content)
            csv_file.close()
        data = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        
        for i in data.columns:
            if i in self.string_groups:
                data[i] = data[i].astype(str)
        return data

    def fetch_events(self, save_raw):
        base_url, params = self.get_params()
        url = base_url + "/events"
        response = requests.get(url, params=params, headers = {'Accept': 'text/csv'})
        if save_raw:
            file_name = str(self.start_date) + '_to_' + str(self.end_date) + '_raw_events_data.csv'
            csv_file = open(file_name, 'wb')
            csv_file.write(response.content)
            csv_file.close()
        data = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        
        for i in data.columns:
            if i in self.string_groups:
                data[i] = data[i].astype(str)
        return data

    def fetch_cohorts(self, save_raw):
        base_url, params = self.get_params()
        url = base_url + "/cohorts"
        response = requests.get(url, params=params, headers = {'Accept': 'text/csv'})
        if save_raw:
            file_name = str(self.start_date) + '_to_' + str(self.end_date) + '_raw_cohorts_data.csv'
            csv_file = open(file_name, 'wb')
            csv_file.write(response.content)
            csv_file.close()
        data = pd.read_csv(io.StringIO(response.content.decode('utf-8')))

        for i in data.columns:
            if i in self.string_groups:
                data[i] = data[i].astype(str)
        return data