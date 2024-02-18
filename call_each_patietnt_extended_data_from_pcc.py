from general_code_to_call_pcc_endpoint import fetch_and_save_data
import json
import requests
from tqdm import tqdm
import time

def read_json_data(json_file_path):
    with open(json_file_path, 'r') as file:
        try:
            data = json.load(file)['data']
            # print(data)
        except Exception as e:
            print("Error:", e)
        return data

def fetch_patient_extended_data_of_facility(path_of_patients,cert_path, key_path,  encodedCredentials , orgId, file_name=None):

    patients =read_json_data(path_of_patients)
    array_of_patients=[]

    for patient_data in tqdm(patients):
       url=f'https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/patients/{patient_data["patientId"]}'
       patient_data = fetch_and_save_data(cert_path, key_path, url, encodedCredentials)
       array_of_patients.append(patient_data)
       time.sleep(0.05)


    # Create a dictionary with 'data' as the key and your array as the value
    my_dict = {'data': array_of_patients}

    # Convert the dictionary to a JSON string
    json_string = json.dumps(my_dict)

    if file_name:
        with open(f'{file_name}.json', 'w') as json_file:
          json_file.write(json_string)


#test
cert_path= 'fullchain.pem'
key_path= 'privkey.pem'
encodedCredentials ='value here'
orgId='value here'
path = 'value here'
fetch_patient_extended_data_of_facility(path, cert_path, key_path, encodedCredentials, orgId, file_name="ResponsesEndpointsPCC/patients_extended_45")


