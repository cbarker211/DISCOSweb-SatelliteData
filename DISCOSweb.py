from pprint import pprint
import requests
from datetime import datetime, timedelta

""" Script to request data from the DISCOSweb database.
NB: Ech request is limited to 30 results, and there is a maximum limit of requests in a certain timeframe (unknown).
To solve this, the script makes multiple requests over different filters."""

#Should add functionality for different request types. 
#The server_request function should be as simple as possible with the params entered into the function.

#Define the URL for the data, and the authorisation token (get a new token here if needed https://discosweb.esoc.esa.int/tokens).
URL = 'https://discosweb.esoc.esa.int'
token = 'IjNiYzUyZjNkLThmNzEtNDVkZC1hMTliLWNiNDgxODkyZjVmOCI.0yAE_9jpGAvf2lCadU85mWWMKkE'

#Setup the program 
start_year = 2022
end_year = 2022

def server_request(user_params):
    response = requests.get(
            #Request information from the launches database.
            f'{URL}/api/launches',
            #Set up authorization.
            headers={
                'Authorization': f'Bearer {token}',
                'DiscosWeb-Api-Version': '2',
            },
            #Use the user-specified parameters.
            params=user_params
        )    
    return response
 

def launches_per_year(start_year,end_year):
    for year in range(start_year,end_year+1):
        full_launch_list = []
        start_epoch = f'{str(year)}-01-01'
        end_epoch = f'{str(year+1)}-01-01'
        while True:
            #Add the filtering here. Currently set up to show successful and unsuccessful launches per year.
            params={
                    'filter': f"ge(epoch,epoch:'{start_epoch}')&lt(epoch,epoch:'{end_epoch}')",
                    'sort' : 'epoch'
                }
            response = server_request(params)
            #First extract the required data and append it to the full list.
            doc = response.json()
            temp_launch_list = doc['data']
            full_launch_list.extend(temp_launch_list)
            #Add a single second onto the start_epoch so we don't double count when the next query starts.
            if len(temp_launch_list) == 30:
                old_start_epoch = temp_launch_list[-1]["attributes"]["epoch"][0:19]
                old_start_epoch = datetime.strptime(old_start_epoch, '%Y-%m-%dT%H:%M:%S')
                start_epoch = old_start_epoch + timedelta(0,3) # days, seconds, then other fields.
                start_epoch = start_epoch.isoformat() + "+00:00"
                continue
            else:
                break
        #Count the launches for each year.
        launch_success = 0
        launch_failure = 0
        for launch in full_launch_list:
            if launch["attributes"]["failure"] == True:
                launch_failure+=1
            elif launch["attributes"]["failure"] == False:
                launch_success+=1
            else:
                print("Error")   
                
        print(f'In {year}, there were {launch_success} successful launches and {launch_failure} unsuccessful launches.')
        
        
launches_per_year(start_year,end_year)