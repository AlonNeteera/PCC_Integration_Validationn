import pandas as pd
from sqlalchemy import create_engine
import requests
import pymysql
import warnings
from tabulate import tabulate
import json
import datetime
from datetime import datetime
import csv
from tqdm import tqdm


def execute_query_to_dataframe(query, connection):
    """
    Execute a SQL query and return the results as a Pandas DataFrame.

    Args:
    query (str): The SQL query to be executed.
    connection: The database connection (already established).

    Returns:
    pd.DataFrame: A Pandas DataFrame containing the query results.
    """
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query)

    # Fetch the results
    result = cursor.fetchall()

    # Get the column names from the cursor's description
    columns = [desc[0] for desc in cursor.description]

    # Create a Pandas DataFrame
    df = pd.DataFrame(result, columns=columns)

    # Close the cursor
    cursor.close()

    return df
def compare_floors_in_db_and_pcc(file_path_json, cnx, save_file_path):
    with open(file_path_json, 'r') as file:
        try:
            data = json.load(file)['data']
        except Exception as e:
            print("Error:", e)
            return
        query = f"select * from OrganizationService.Floors where tenantId=(select id from OrganizationService.Tenants where emrId={data[0]['facId']})"
        df=execute_query_to_dataframe(query, cnx)
        bad_data = []
        for floor_data in tqdm(data, desc="Comparing floors in db and PCC"):  # Add tqdm here
            # Check if any row has the specified values
            condition = (df["name"] == floor_data["floorDesc"]) & (df["emrId"] == floor_data["floorId"])

            result = df[condition]


            if result.empty:
                bad_data.append(floor_data)

        # Save bad data as JSON
        if bad_data:
                print(f"Not all floors synced correctly in DB. {len(bad_data)} of {len(data)} floors not synced correctly. ")
                with open(f'{save_file_path}.json', 'w') as jsonfile:
                    json.dump(bad_data, jsonfile, indent=4)
        else:
            print("All floors synced correctly.")



def compare_rooms_in_db_and_pcc(file_path, cnx, save_file_path):
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)['data']
        except Exception as e:
            print("Error:", e)
            return

        query = f"select * from OrganizationService.Rooms where tenantId=(select id from OrganizationService.Tenants where emrId={data[0]['facId']})"
        df = execute_query_to_dataframe(query, cnx)
        bad_data = []
        for room_data in tqdm(data, desc="Comparing rooms in db and PCC"):  # Add tqdm here
            # Check if any row has the specified values
            condition = (df["name"] == room_data["roomDesc"]) & (df["emrId"] == room_data["roomId"])

            result = df[condition]

            if result.empty:
                bad_data.append(room_data)

        # Save bad data as JSON
        if bad_data:
                print(f"Not all rooms synced correctly in DB. {len(bad_data)} of {len(data)} rooms not synced correctly. ")
                with open(f'{save_file_path}.json', 'w') as jsonfile:
                    json.dump(bad_data, jsonfile, indent=4)
        else:
            print("All rooms found in DB.")



def compare_units_pcc_and_db(pcc_json_file_path, cnx, save_file_path):

    with open(pcc_json_file_path, 'r') as file:
        try:
            data = json.load(file)['data']
        except Exception as e:
            print("Error:", e)
            return


        query = f"select * from OrganizationService.Units where tenantId=(select id from OrganizationService.Tenants where emrId={data[0]['facId']})"
        df = execute_query_to_dataframe(query, cnx)
        bad_data = []
        for room_data in tqdm(data, desc="Comparing units in db and PCC"):  # Add tqdm here
            # Check if any row has the specified values
            condition = (df["name"] == room_data["unitDesc"]) & (df["emrId"] == room_data["unitId"])

            result = df[condition]

            if result.empty:
                bad_data.append(room_data)

        # Save bad data as JSON
        # Save bad data as CSV
        if bad_data:
            print(f"Not all data synced correctly. {len(bad_data)} of {len(data)} units weren't synced correctly")
            with open(f'{save_file_path}.json', 'w') as jsonfile:
                json.dump(bad_data, jsonfile, indent=4)
        else:
            print("All units synced correctly.")



def compare_beds_in_pcc_and_db(json_pcc_file_path, cnx, save_file_path):
    with open(json_pcc_file_path, 'r') as file:
        try:
            data = json.load(file)['data']
        except Exception as e:
            print("Error:", e)
            return

        bad_data = []
        query_beds=     f"""select * from OrganizationService.Beds
                            where tenantId=(select id from OrganizationService.Tenants where emrId={data[0]['facId']})"""
        df_beds=execute_query_to_dataframe(query_beds,cnx)
        query_rooms = f"""select * from OrganizationService.Rooms
                                    where tenantId=(select id from OrganizationService.Tenants where emrId={data[0]['facId']})"""
        df_rooms = execute_query_to_dataframe(query_rooms, cnx)

        for room in tqdm(data, desc="Scanning beds in each room"):
            for bed in room['beds']:
                cursor = cnx.cursor()

                query = f"""select * from OrganizationService.Beds 
                            where name='{bed['description']}'                 
                            and emrId='{bed['id']}'
                            and roomId=(select id from OrganizationService.Rooms where emrId='{room['roomId']}')"""


                # Execute a query
                cursor.execute(query)

                # Fetch the results
                results = cursor.fetchall()
                if not results:
                    bad_data.append(bed)

                # Save bad data as JSON
        if bad_data:
                    print(f"Not all data synced correctly.")
                    with open(f'{save_file_path}.json', 'w') as jsonfile:
                        json.dump(bad_data, jsonfile, indent=4)

        else:
            print("All beds synced correctly.")


def compare_patients_in_pcc_and_db(pcc_json_file_path, cnx, save_file_path):
    with open(pcc_json_file_path, 'r') as file:
        try:
            data = json.load(file)['data']
            # print(data)
        except Exception as e:
            print("Error:", e)
            return

        bad_data = []
        partially_bad_data=[]
        cursor = cnx.cursor()
        for patient_data in tqdm(data, desc="Comparing patients in pcc endpoint and in the DB"):  # Add tqdm here


                if 'bedId' in patient_data and 'birthDate' in patient_data:
                 query = f"""select * from OrganizationService.Users 
                            where dateOfBirth='{patient_data['birthDate']}' 
                            and firstName='{patient_data['firstName']}' 
                            and lastName='{patient_data['lastName']}' 
                            and emrBedId='{patient_data['bedId']}'  
                            and emrRoomId='{patient_data['roomId']}' 
                            and patientId='{patient_data['patientId']}'"""
                elif 'birthDate' in patient_data:

                    query = f"""select * from OrganizationService.Users 
                                where dateOfBirth='{patient_data['birthDate']}' 
                                and firstName='{patient_data['firstName']}' 
                                and lastName='{patient_data['lastName']}' 
                                and  patientId='{patient_data['patientId']}'"""
                else:
                    query = f"""select * from OrganizationService.Users 
                                                  where firstName='{patient_data['firstName']}' 
                                                  and lastName='{patient_data['lastName']}' 
                                                  and  patientId='{patient_data['patientId']}'"""
                # Execute a query

                cursor.execute(query)

                # Fetch the results
                results = cursor.fetchall()
                if not results:
                    # query2= f"""select * from OrganizationService.Users
                    #                               where firstName='{patient_data['firstName']}'
                    #                               and lastName='{patient_data['lastName']}' """
                    # # Execute a query
                    #
                    # cursor.execute(query2)
                    #
                    # # Fetch the results
                    # partialy_saved= cursor.fetchall()
                    # if not partialy_saved:
                        bad_data.append(patient_data)
                    # else:
                    #     partially_bad_data.append(patient_data)
                # Save bad data as JSON
    if bad_data or partially_bad_data:
        print(f"Not all data found! {len(bad_data)} of {len(data)} patients not synced at all")
        # print(f"{len(partially_bad_data)} of {len(data)} patients not synced correctly")
        with open(f'{save_file_path}.json', 'w') as jsonfile:
            json.dump(bad_data, jsonfile, indent=4)

    else:
        print("All patients synced correctly")


# Usage example
# You need to pass your JSON data, file_path, database connection (cnx), and query to the check function.
def compare_facilities_pcc_and_db(file_path, cnx, save_file_path):
    def format_time(hours):
        if isinstance(hours, str):
            if hours.startswith('+'):
                # The input is in the "+X" format, so remove the "+" sign and convert to int
                hours = int(hours[1:])
            elif hours.startswith('-'):
                # The input is in the "-X" format, so remove the "-" sign, convert to int, and preserve the sign
                hours = int(hours[1:])
            else:
                # Invalid input, return it as is
                return hours

        if hours >= 0:
            sign = "+"
            formatted_hours = hours
        else:
            sign = "-"
            formatted_hours = abs(hours)

        formatted_time = f"{sign}{formatted_hours:02d}:00"
        return formatted_time

    with open(file_path, 'r') as file:
        try:
            data = json.load(file)['data']
        except Exception as e:
            print("Error:", e)
            return

        bad_data = []
        for facility in tqdm(data, desc="Checking Facilities"):  # Add tqdm here

                cursor = cnx.cursor()

                country=facility['country']
                if country=='USA':
                    country='US'

                query = f"""select * from OrganizationService.Tenants 
                            where name='{facility['facilityName']}'                 
                            and emrId='{facility['facId']} '
                            and state='{facility['state']}'
                            and countryCode='{country}'
                            and state ='{facility['state']}'
                            and city = '{facility['city']}'
                            and address1 = '{facility["addressLine1"]}'
                           
                            and timeZoneId ='{facility['timeZone']}'
                            and timeZoneOffset = '{format_time(facility['timeZoneOffset'])}'
                            and zipcode= '{facility['postalCode']}'"""
                # and address2 = '{facility["addressLine2"]}'

                # Execute a query
                cursor.execute(query)

                # Fetch the results
                results = cursor.fetchall()
                if not results:
                    bad_data.append(facility)

        # Save bad data as JSON
        if bad_data:
            print("Not all facilities synced correctly.")
            with open(f'{save_file_path}.json', 'w') as jsonfile:
                json.dump(bad_data, jsonfile, indent=4)
        else:
            print("All facilities synced correctly.")



def compare_emails_in_pcc_and_db(pcc_json_file_path, cnx, save_file_path):
    with open(pcc_json_file_path, 'r') as file:
        try:
            data = json.load(file)['data']
            # print(data)
        except Exception as e:
            print("Error:", e)
            return

        bad_data = []

        cursor = cnx.cursor()
        query = f"""select * from OrganizationService.Users 
                               where tenantId =(select id from OrganizationService.Tenants where emrId= '{data[0]['facId']}')
                                                                """

        # Execute a query

        cursor.execute(query)

        # Fetch the results
        result = cursor.fetchall()

        # Get the column names from the cursor's description
        columns = [desc[0] for desc in cursor.description]

        # Create a Pandas DataFrame
        df = pd.DataFrame(result, columns=columns)




        # Close the cursor and the database connection
        cursor.close()


        # Get the column names from the cursor's description
        columns = [desc[0] for desc in cursor.description]
        for patient_data in tqdm(data, desc="Comparing patients in pcc endpoint and in the DB"):  # Add tqdm here


         # if 'email' in patient_data:
         if 'email' in patient_data:
            # Check if any row has the specified values
            condition = (df["firstName"] == patient_data["firstName"]) & (df["lastName"] == patient_data["lastName"]) & (df["email"] == patient_data["email"])

            result = df[condition]

            if result.empty:


                        bad_data.append(patient_data)

        if bad_data:
                print(f"Not all data found! {len(bad_data)} of {len(data)} patients not synced at all")
                # print(f"{len(partially_bad_data)} of {len(data)} patients not synced correctly")
                with open(f'{save_file_path}.json', 'w') as jsonfile:
                    json.dump(bad_data, jsonfile, indent=4)

        else:
                print("All patients synced correctly")











directory_with_responses='ResponsesEndpointsPCC'
def compare_all_synced_data_in_db_to_PCC(orgId,directory_with_responses,directory_with_unsynced_data, cnx, dict_with_file_paths=None):
    facilities_file_path = f"{directory_with_responses}/facs_{orgId}.json"

    with open(facilities_file_path, 'r') as file:
        try:
            facilities = json.load(file)['data']
        except:
            "erorr reading facilities file"
    print("Comparing data of each facility in PCC with datat in DB")

    compare_facilities_pcc_and_db(facilities_file_path, cnx, directory_with_unsynced_data)

    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%m-%d_%H%M')
    for facility in facilities:
        print(f"\n #####################################\nChecking data for facility {facility['facilityName']}")

        compare_patients_in_pcc_and_db(f"{directory_with_responses}/patients_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/not_synced_patients_facId_{facility['facId']}_{formatted_datetime}")
        compare_rooms_in_db_and_pcc(f"{directory_with_responses}/rooms_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_rooms_facId_{facility['facId']}_{formatted_datetime}")
        compare_beds_in_pcc_and_db(f"{directory_with_responses}/beds_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_beds_facId_{facility['facId']}_{formatted_datetime}")
        compare_units_pcc_and_db(f"{directory_with_responses}/units_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_units_facId_{facility['facId']}_{formatted_datetime}")
        compare_floors_in_db_and_pcc(f"{directory_with_responses}/floors_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_floors_facId_{facility['facId']}_{formatted_datetime}")

def compare_single_facility_synced_data_in_db_to_PCC(orgId,directory_with_responses,directory_with_unsynced_data, cnx, facId):
    facilities_file_path = f"{directory_with_responses}/facs_{orgId}.json"

    with open(facilities_file_path, 'r') as file:
        try:
            facilities = json.load(file)['data']
        except:
            "erorr reading facilities file"
    print("Comparing data in facility in PCC with data in DB")

    compare_facilities_pcc_and_db(facilities_file_path, cnx, directory_with_unsynced_data)

    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%m-%d_%H%M')
    for facility in facilities:
     if facility['facId']==facId:
        print(f"\n #####################################\nChecking data for facility {facility['facilityName']}")

        compare_patients_in_pcc_and_db(f"{directory_with_responses}/patients_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/not_synced_patients_facId_{facility['facId']}_{formatted_datetime}")
        compare_rooms_in_db_and_pcc(f"{directory_with_responses}/rooms_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_rooms_facId_{facility['facId']}_{formatted_datetime}")
        compare_beds_in_pcc_and_db(f"{directory_with_responses}/beds_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_beds_facId_{facility['facId']}_{formatted_datetime}")
        compare_units_pcc_and_db(f"{directory_with_responses}/units_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_units_facId_{facility['facId']}_{formatted_datetime}")
        compare_floors_in_db_and_pcc(f"{directory_with_responses}/floors_{facility['facId']}.json", cnx, f"{directory_with_unsynced_data}/unsynced_floors_facId_{facility['facId']}_{formatted_datetime}")




def main():
    """
    main function
    for testing purposes
    """
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%m-%d_%H%M')

    cnx = pymysql.connect(
        host='',
        user='',
        password='',
        port= 0
    )


    compare_floors_in_db_and_pcc('ResponsesEndpointsPCC/floors_45.json', cnx, f'badData/test_floors_{formatted_datetime}')

if __name__ == "__main__":
    main()
