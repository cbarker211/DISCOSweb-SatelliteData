from pprint import pprint
import requests
from datetime import datetime, timedelta
import time
import sys
import numpy as np

""" Script to request data from the DISCOSweb database. 
NB: Each request is limited to 30 results, and there is a maximum limit of requests in a certain timeframe (20 requests per 60s).
To solve this, the script makes multiple requests over different filters, and automatically calculates the time required to wait."""

#Define the URL for the data, and the authorisation token (get a new token here if needed https://discosweb.esoc.esa.int/tokens).
URL = 'https://discosweb.esoc.esa.int/api'
token = np.loadtxt("token.txt", dtype=str)
filename = 'output.txt'

def server_request(user_params,url_mod):
    response = requests.get(
            #Request information from the launches database.
            f'{URL}{url_mod}',
            #Set up authorization.
            headers={
                'Authorization': f'Bearer {token}',
                'DiscosWeb-Api-Version': '2',
            },
            #Use the user-specified parameters.
            params=user_params
        ) 
    return response

def wait_function(message,wait_time):
    for i in range(wait_time,0,-1):
        #print(f"{message}Too many requests, retrying in {str(i)}s.")
        print(f"{message}Too many requests, retrying in {str(i)}s.", end="\r", flush=True)
        time.sleep(1)
 
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
            response = server_request(params, '/launches')
            if response.ok:
                break
            else:
                message = ""
                wait_time=(int(response.headers["X-Ratelimit-Reset"])-int(time.time()))+1
                wait_function(message,wait_time)
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
        print(f'\nIn {year}, there were {launch_success} successful launches and {launch_failure} unsuccessful launches.')
        
def payloads_launched_per_year(start_year,end_year):
    for year in range(start_year,end_year+1):
        full_launch_list = get_yearly_launches(year)
        num_payloads = 0
        for launch in full_launch_list:
            full_object_list = []
            #First filter only the successful launches.
            if launch["attributes"]["failure"] == False:
                #Need to get the object information for each specific launch, and then sum the payloads.
                params={
                        'filter' : "eq(objectClass,Payload)",
                        'sort' : 'id'         
                    }
                while True:
                    response = server_request(params,f'/launches/{launch["id"]}/objects')
                    if response.ok:
                        doc = response.json()
                        temp_object_list = doc['data']
                        full_object_list.extend(temp_object_list)
                        if len(temp_object_list) > 0:
                            final_object_id = str(int(temp_object_list[-1]["id"]) + 1)
                        if len(temp_object_list) == 30:
                            params={ 
                                    'filter' : f"eq(objectClass,Payload)&ge(id,{final_object_id})",
                                    'sort' : 'id'         
                                }
                            continue
                        break
                    elif response.status_code == 400:
                        print("Client Error")
                        break
                    elif response.status_code == 429:
                        message = f"Currently found {num_payloads} payloads. "
                        wait_time=(int(response.headers["X-Ratelimit-Reset"])-int(time.time()))+1
                        wait_function(message,wait_time)
                        continue
                #print(launch["id"],len(full_object_list))
                num_payloads += len(full_object_list)
        with open(filename, 'a') as f:
            f.write(f'In {year}, there were {num_payloads} payloads successfully launched.\n')       

def propellant_per_year(start_year,end_year):
    
    # Each should have associated launch vehicle, which should have oxidiser and fuel masses.
    
    # Start similar to the payloads function by pulling the data for all successful launches.
    for year in range(start_year,end_year+1):
        full_launch_list = get_yearly_launches(year)
        oxidiser_mass = np.float32(0)
        fuel_mass = np.float32(0)
        solid_mass = np.float32(0)
        total_mass = np.float32(0)
        
        #For only testing first launch
        first_launch = [full_launch_list[0]]
        #for launch in first_launch:
        
        for count, launch in enumerate(full_launch_list):
            #First filter only the successful launches.
            if launch["attributes"]["failure"] == False:
                #First identify the vehicle
                while True:
                    response = server_request({},f'/launches/{launch["id"]}/vehicle')
                    if response.ok:
                        doc = response.json()
                        launch_info = doc['data']
                        #Then loop over all stages
                        while True:
                            response = server_request({},f'/launch-vehicles/{launch_info["id"]}/stages')
                            if response.ok:
                                doc = response.json()
                                vehicle_info = doc['data']
                                # Usually 2-5 stages, so won't need to account for the maximum of 30 items in a list.
                                for stage in vehicle_info:
                                    
                                    temp_fuel_mass = stage["attributes"]["fuelMass"]
                                    if temp_fuel_mass == None:
                                        temp_fuel_mass = 0
                                    fuel_mass += temp_fuel_mass
                                    
                                    temp_oxidiser_mass = stage["attributes"]["oxidiserMass"]
                                    if temp_oxidiser_mass == None:
                                        temp_oxidiser_mass = 0
                                    oxidiser_mass += temp_oxidiser_mass
                                    
                                    temp_solid_mass = stage["attributes"]["solidPropellantMass"]
                                    if temp_solid_mass == None:
                                        temp_solid_mass = 0
                                    solid_mass += temp_solid_mass
                                    
                                    total_mass = fuel_mass + solid_mass + oxidiser_mass
                                break
                            elif response.status_code == 400:
                                print("Client Error")
                                break
                            elif response.status_code == 429:
                                message = f"On launch {count} of {len(full_launch_list)} in {year}. Currently found {total_mass} kg of propellant. "
                                wait_time=(int(response.headers["X-Ratelimit-Reset"])-int(time.time()))+1
                                wait_function(message,wait_time)
                                continue
                        break
                    elif response.status_code == 400:
                        print("Client Error")
                        break
                    elif response.status_code == 429:
                        message = f"On launch {count} of {len(full_launch_list)} in {year}. Currently found {total_mass} kg of propellant. "
                        wait_time=(int(response.headers["X-Ratelimit-Reset"])-int(time.time()))+1
                        wait_function(message,wait_time)
                        continue
        with open(filename, 'a') as f:
            f.write(f'{year},{fuel_mass},{oxidiser_mass},{solid_mass},{total_mass}\n') 
         
start_year = 2010
end_year = 2023
#launches_per_year(start_year,end_year)
#payloads_launched_per_year(start_year,end_year)       
propellant_per_year(start_year,end_year)