from pprint import pprint
import requests
from datetime import datetime, timedelta
import time
import sys

""" Script to request data from the DISCOSweb database.
NB: Ech request is limited to 30 results, and there is a maximum limit of requests in a certain timeframe (unknown).
To solve this, the script makes multiple requests over different filters."""

#Define the URL for the data, and the authorisation token (get a new token here if needed https://discosweb.esoc.esa.int/tokens).
URL = 'https://discosweb.esoc.esa.int'
token = 'IjNiYzUyZjNkLThmNzEtNDVkZC1hMTliLWNiNDgxODkyZjVmOCI.0yAE_9jpGAvf2lCadU85mWWMKkE'

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

def server_request_objects(user_params,id):
    response = requests.get(
            #Request information from the launches database.
            f'{URL}/api/launches/{id}/objects',
            #Set up authorization.
            headers={
                'Authorization': f'Bearer {token}',
                'DiscosWeb-Api-Version': '2',
            },
            #Use the user-specified parameters.
            params=user_params
        )    
    return response
 
def get_yearly_launches(year): 
    full_launch_list = []
    start_epoch = f'{str(year)}-01-01'
    end_epoch = f'{str(year+1)}-01-01'
    while True:
        #Add the filtering here. Currently set up to show successful and unsuccessful launches per year.
        params={
                'filter': f"ge(epoch,epoch:'{start_epoch}')&lt(epoch,epoch:'{end_epoch}')",
                'sort' : 'epoch'
            }
        #Now we need a while loop to deal with the rate limit. 
        #If the response is ok (code <400), then it breaks the while loop, and if not then the while loop continues after a 60 second delay.
        while True:
            response = server_request(params)
            if response.ok:
                break
            else:
                for i in range(60,0,-1):
                    print(f"Too many requests, retrying in {str(i)}s.", end="\r", flush=True)
                    time.sleep(1)
                continue
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
    return full_launch_list

def launches_per_year(start_year,end_year):
    for year in range(start_year,end_year+1):
        full_launch_list = get_yearly_launches(year)
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
        
def payloads_launched_per_year(start_year,end_year):
    for year in range(start_year,end_year+1):
        full_launch_list = get_yearly_launches(year)
        num_payloads = 0
        for launch in full_launch_list:
            #First filter only the successful launches.
            if launch["attributes"]["failure"] == False:
                #Need to get the object information for each specific launch, and then sum the payloads.
                params={          
                    }
                while True:
                    response = server_request_objects(params,launch["id"])
                    if response.ok:
                        break
                    else:
                        for i in range(60,0,-1):
                            print(f"Too many requests, retrying in {str(i)}s.", end="\r", flush=True)
                            time.sleep(1)
                        continue
                doc = response.json()
                object_list = doc['data']
                for launch_object in object_list:
                    if launch_object["attributes"]["objectClass"] == "Payload":
                        num_payloads += 1
        print(f'In {year}, there were {num_payloads} payloads successfully launched.')        
     
start_year = 2017
end_year = 2023 
#launches_per_year(start_year,end_year)
payloads_launched_per_year(start_year,end_year)