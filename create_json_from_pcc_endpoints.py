import requests
import pandas as pd
import json
from tqdm import tqdm

def fetch_and_save_pcc_data(cert_path, key_path, facId, endpoint_name, orgId, encodedCredentials, file_name=None):
    """

    Args:
        cert_path: The certificate file needed for running PCC endpoint
        key_path: The certificate file needed for running PCC endpoint
        facId: The facility ID you want to run the query on . None  if its to fetch facilities.
        endpoint_name: Name of the endpoint used by pcc possible options- "rooms", "beds", "patients", 'units','floors', "facs"
        file_name:file path to save the response to
        orgId: orginization ID in PCC
        encodedCredentials: The credentials given by PCC

    Returns:

    """
    # Define the token URL and payload
    url_token = "https://connect2.pointclickcare.com/auth/token"
    payload = 'grant_type=client_credentials'

    # Define the headers for the token request
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {encodedCredentials}'
    }

    # Make the token request
    response = requests.request("POST", url_token, headers=headers, data=payload, cert=(cert_path, key_path)).json()
    access_token = response["access_token"]
    #print("token fetched")
    if endpoint_name== 'patients':
        url = f"https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/patients?facId={facId}&patientStatus=Current&page=1&pageSize=200"
    elif endpoint_name == 'facs':
        url = f'https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/facs'
    else:
        # Define the URL for fetching data
        url = f"https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/facs/{facId}/{endpoint_name}?pageSize=200"

    # Define headers with the access token for the floors request
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # Make the request
    response = requests.get(url, headers=headers, cert=(cert_path, key_path))

    if response.status_code == 200:
        data = response.json()  # Parse the response as JSON
        # data=json.load
        more_pages=data["paging"]["hasMore"]
        if more_pages == True:
            page=2

            while more_pages==True:
                #print(f"Saving page number {page}")
                if endpoint_name == 'patients':
                    url = f"https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/patients?facId={facId}&patientStatus=Current&page={page}&pageSize=200"
                elif endpoint_name== 'facs':
                    url=f'https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/facs'
                else:
                    # Define the URL for fetching data
                    url = f"https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/facs/{facId}/{endpoint_name}?page={page}&pageSize=200"
                response2 = requests.get(url, headers=headers, cert=(cert_path, key_path))
                data2 = response2.json()
                # Merge the "data" from the second JSON into the "data" of the first JSON
                data["data"].extend(data2["data"])
                more_pages=data2["paging"]["hasMore"]

                page+=1

        # Serialize the data to JSON format
        json_data = json.dumps(data, indent=4)

        # Write the JSON data to the file
        if file_name:
         with open(f"{file_name}.json", "w") as json_file:
            json_file.write(json_data)
            #print("saved data in ")
        return json_data
    else:
        print(f"Failed to fetch data from the API. Status code: {response.status_code}")
        # return pd.DataFrame()




def fetch_all_data(cert_path, key_path,orgId,encodedCredentials ,directory_path):
    facilities=fetch_and_save_pcc_data(cert_path, key_path, None, 'facs', orgId,encodedCredentials,f"{directory_path}/facs_{orgId}.json")
    facIds=[]
    for facility in facilities["data"]:
        facIds.append(facility["facId"])
    endpoints=["rooms", "beds", "patients", 'units','floors']
    for facId in tqdm(facIds,desc="Fetching data from facilities"):
        for endpoint in tqdm(endpoints,desc=f"fetching data from facility  with ID {facId}"):
            fetch_and_save_pcc_data(cert_path, key_path, facId, endpoint, orgId,f"{directory_path}/{endpoint}_{facId}")

