import requests
import json

def fetch_and_save_data_from_PCC(cert_path, key_path, url, EncodedCredentials, file_name=None):
    """
    Args:
        cert_path: The certificate file needed for running the endpoint.
        key_path: The certificate file needed for running the endpoint.
        EncodedCredentials: The credentials given by PCC
        url: The URL of the endpoint you want to fetch data from.
        file_name: File path to save the response to (optional).

    Returns:
        data: The JSON data fetched from the endpoint.
    """
    # Define the token URL and payload
    url_token = "https://connect2.pointclickcare.com/auth/token"
    payload = 'grant_type=client_credentials'

    # Define the headers for the token request
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {EncodedCredentials}'
    }

    # Make the token request
    response = requests.request("POST", url_token, headers=headers, data=payload, cert=(cert_path, key_path)).json()
    access_token = response["access_token"]

    # Define headers with the access token for the data request
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    # Make the request to fetch data
    response = requests.get(url, headers=headers, cert=(cert_path, key_path))

    if response.status_code == 200:
        data = response.json()  # Parse the response as JSON

        # If there are more pages, continue fetching and merging data
        more_pages = data.get("paging", {}).get("hasMore")
        page = 2

        while more_pages:
            # Update the URL for the next page if applicable
            if "&page=" in url:
                next_url = url.replace(f"&page=1", f"&page={page}")
            else:
                next_url = f"{url}&page={page}"

            response2 = requests.get(next_url, headers=headers, cert=(cert_path, key_path))
            data2 = response2.json()
            data["data"].extend(data2.get("data", []))
            more_pages = data2.get("paging", {}).get("hasMore")
            page += 1

        # Serialize the data to JSON format
        json_data = json.dumps(data, indent=4)

        # Write the JSON data to the file if a file_name is provided
        if file_name:
            with open(f"{file_name}.json", "w") as json_file:
                json_file.write(json_data)

        return data
    else:
        print(f"Failed to fetch data from the API. Status code: {response.status_code}")
        return None

if __name__ == "__main__":
    encodedCredentials =''
    orgId=''
    url=f'https://connect2.pointclickcare.com/api/public/preview1/orgs/{orgId}/patients/7792'
    cert_path= 'fullchain.pem'
    key_path= 'privkey.pem'
    fetch_and_save_data_from_PCC(cert_path, key_path, url, encodedCredentials , file_name='savedtest')