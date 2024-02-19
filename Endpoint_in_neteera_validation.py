import requests
import json
from create_json_from_pcc_endpoints import fetch_and_save_pcc_data

orgId='Add value here' #Orginization ID needs to be given by pcc
facId='Add value here' #Each tenant in pcc has an id wich is saved in the tenants table in the emrID coulmn
#login info
data = {

        "username": "",
        "password": "",
        "code": "string"
    }
base_url ="https://app.stg.neteerahealthbridge.neteera.com/"
def get_token_for_neteera_api(data_for_login, env):
    """

    Args:
        data_for_login: dictionary with username and password
        env: environment to run the API in. stg, fda, or prod

    Returns:

    """

    res = requests.post(f'https://api.{env}.neteerahealthbridge.neteera.com/ums/v2/users/login', json=data_for_login).json()


    token = res['accessJwt']['token']

    my_headers = {'Authorization': f'Bearer {token}'}

    return my_headers


#environment I am using
env='stg'
#token for api
my_headers= get_token_for_neteera_api(data, env)
path=""

def get_response(url):

    response = requests.get(url, headers=my_headers)

    if response.status_code == 200:
        patient_response=response.json()
        return response
    else:
        print(f"Request failed with status code {response.status_code}")
        return None
    



def verify_patients(base_url, path):
    try:
        # Get patient response
        patient_response = get_response(f"{base_url}/organization/v1/patients?limit=1000000")

        # Check if 'data' exists in patient response
        if 'data' in patient_response:
            with open(path, 'r') as file:
                try:
                    data = json.load(file)['data']
                except Exception as e:
                    print("Error:", e)

            # Create sets of patient IDs and names from the response data
            response_patient_ids = set(patient['patientId'] for patient in patient_response['data'])
            response_patient_names = set(
                f"{patient['firstName']} {patient['lastName']}" for patient in patient_response['data'])

            # Find patients not present in the response data based on 'patientId' or the combination of 'firstName' and 'lastName'
            not_found = [patient for patient in data if (patient['patientId'] not in response_patient_ids) and (
                    f"{patient['firstName']} {patient['lastName']}" not in response_patient_names)]

            if not_found:
                print(f"{len(not_found)} patients not found")
            else:
                print("All patients found")
        else:
            print("No 'data' found in patient response")
    except Exception as e:
        print("Error:", e)

verify_patients(base_url, path)
import requests

#Run endpoint to fetch the log of all sessions of each patient in the tenant
sessions =requests.get(
            f'https://api.{env}.neteerahealthbridge.neteera.com/telemetry/v1/patients/sessions/latest?filterMissing=false',
            headers=my_headers).json()
running_sessions = [session for session in sessions["sessions"] if session['status'] == 'RUNNING']


room_response=requests.get(
            f'https://api.{env}.neteerahealthbridge.neteera.com/organization/v2/room',
            headers=my_headers).json()
# Extract "deviceId" and "emrId" of beds with non-null values for both fields
device_to_emr_mapping = {}

for room in room_response["rooms"]:
    for bed in room["beds"]:
        device_id = bed["deviceId"]
        emr_id = bed["emrId"]
        if device_id is not None and emr_id is not None:
            device_to_emr_mapping[device_id] = emr_id

running_sessions_with_emr=[]
for session in running_sessions:
    deviceId = session.get("deviceId")
    if deviceId in device_to_emr_mapping:
        # Create a new session dictionary with "emrId" added
        new_session = session.copy()
        new_session["BedEmrId"] = device_to_emr_mapping[deviceId]

        running_sessions_with_emr.append(new_session)
    else:
        running_sessions_with_emr.append(session)  #


url_patients=f'https://api.{env}.neteerahealthbridge.neteera.com/organization/v1/patients?limit=1000000'
patient_response = requests.get(url_patients, headers=my_headers)

if patient_response.status_code == 200:
    patient_response=patient_response.json()["data"]
else:
    print(f"Request failed with status code {patient_response.status_code}")

running_sessions_with_bed_and_patient_id=[]
for patient_data in patient_response:
    for session in running_sessions_with_emr:
      if patient_data.get("id") == session['patientId']:


        new_session = session.copy()
        new_session["pid"] = patient_data.get("patientId")
        running_sessions_with_bed_and_patient_id.append(new_session)


print(running_sessions_with_bed_and_patient_id)


cert_path= 'fullchain.pem'
key_path= 'privkey.pem'
failed=[]
unique_identifiers = set()
patients_pcc=  json.loads(fetch_and_save_pcc_data(cert_path, key_path, 45, "patients", orgId))
# Collect the unique identifiers from patients_pcc
for patient_data_pcc in patients_pcc["data"]:
    patient_id = patient_data_pcc.get("patientId")
    bed_emr_id = patient_data_pcc.get("bedId")
    if patient_id is not None and bed_emr_id is not None:
        unique_identifiers.add((patient_id, bed_emr_id))
# Filter running_sessions to find sessions that are not in patients_pcc
failed = [session for session in running_sessions_with_bed_and_patient_id if
          (session['pid'], session['BedEmrId']) not in unique_identifiers]

if failed:
    print(f'Not all sessions have correct patients in them.\n {len(failed)} of {len(running_sessions)} sessions have a patient in the wrong bed:')
    for session in failed:
        print(f"{session['id']}")
else:
    print('All rooms with running sessions have correct patients')



