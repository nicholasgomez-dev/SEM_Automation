import sys
import argparse
import datetime
import gspread
import boto3
from boto3.dynamodb.conditions import Key
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def main(ads_client, sheets_client, customer_id):
    
    # Intialize Arrays
    
    update_budgets = []
    new_budgets = []
    program_errors = []
    
    # Select Google Sheet File
    sem_automation_sheet = sheets_client.open("SEM Automation Sheet")
    # Get all clients.
    launch_control_list = sem_automation_sheet.worksheet("Launch Control").get_all_records()
    # Filter out clients who are not ready to update.
    update_list = list(filter(lambda client: client['Update'] == 'TRUE', launch_control_list))
    # Get all data from client worksheets.
    for update in update_list:
        update_budgets.extend(sem_automation_sheet.worksheet(update['Sheet Name']).get_all_records())
    
    # Get most recent transaction for each client from DynamoDB from Transactions Table.
    past_transactions = []

    for budget in update_budgets:
        dynamodb_client = boto3.client('dynamodb', region_name="us-west-1")
        TABLE_NAME = "SEM_Automation_Transactions"
        client_id = str(budget['Client ID'])
        response = dynamodb_client.query(
            TableName=TABLE_NAME,
            KeyConditionExpression='ClientID = :clientID',
            ExpressionAttributeValues={
                ':clientID': {'S': client_id}
            },
            Limit=1
        )
        # If client has no transactions, remove from update_budgets array and add to new_budgets.
        if len(response['Items']) == 0:
            new_budgets.append(budget)
            update_budgets.remove(budget)
        elif (budget.budget / response['Items'][0]) > .1:
            program_errors.append(budget)
            update_budgets.remove(budget)
        else:
            past_transactions.extend(response['Items'])

    
        





# Intialize connections & pass arguments to main()
if __name__ == "__main__":
    #  Init Google Ads & Google Sheets client
    googleads_client = GoogleAdsClient.load_from_storage("./credentials/gads.yaml")
    googlesheets_client = gspread.service_account(filename='./credentials/gspread.json')

    # Program Description
    parser = argparse.ArgumentParser(
        description=(
            "Updates budgets in Google Ads from data in Google Sheets."
        )
    )
    # The following arguments should be provided to run the program.
    parser.add_argument(
        "-c",
        "--customer_id",
        type=str,
        required=True,
        help="The Google Ads customer ID.",
    )

    # Set arguments to variables.
    args = parser.parse_args()

    try:
        main(googleads_client, googlesheets_client, args.customer_id)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
    except gspread.exceptions.APIError as ex:
        print(
            f'Error with message "{ex.message}".'
        )
        sys.exit(1)

