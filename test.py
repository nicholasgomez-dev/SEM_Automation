import sys
import argparse
import datetime
import gspread
import boto3
from boto3.dynamodb.conditions import Key
from google.api_core import protobuf_helpers
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def handleBudgetUpdates(update_budgets_list, client_id):
    # Initialize services
    client = GoogleAdsClient.load_from_storage("./credentials/gads.yaml")
    campaign_budget_service = client.get_service("CampaignBudgetService")
    campaign_service = client.get_service('CampaignService')

    # Initialize operations
    campaign_budget_operation = client.get_type("CampaignBudgetOperation")

    for update in update_budgets_list:
        # Mutate budget
        usd_to_micros = update.budget * 1000000
        campaign_budget = campaign_budget_operation.update
        campaign_budget.resource_name = 'customers/4714359917/campaignBudgets/11725282046'
        campaign_budget.amount_micros = usd_to_micros
        # Create field mask
        field_mask = protobuf_helpers.field_mask(None, campaign_budget._pb)
        # Copy field mask to operation
        client.copy_from(campaign_budget_operation.update_mask, field_mask)
        # Mutate budget request
        campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id="4714359917", operations=[campaign_budget_operation]
        )
        # If response is error, add to budget_errors array.
        # If response is success, add item to transaction to DynamoDB SEM_Automation_Transactions Table.


def main(customer_id):
    
    # Intialize Arrays
    update_budgets = []
    new_budgets = []
    budget_errors = []

    #  Init Google Ads & Google Sheets client
    googlesheets_client = gspread.service_account(filename='./gspread/gspread.json')
    # Select Google Sheet File
    sem_automation_sheet = googlesheets_client.open("SEM Automation Sheet")
    # Get all clients.
    launch_control_list = sem_automation_sheet.worksheet("Launch Control").get_all_records()
    # Filter out clients who are not ready to update.
    update_list = list(filter(lambda client: client['Update'] == 'TRUE', launch_control_list))
    # Get all data from client worksheets.
    for update in update_list:
        update_budgets.extend(sem_automation_sheet.worksheet(update['Sheet Name']).get_all_records())
    
    # Init DynamoDB client
    dynamodb_client = boto3.client('dynamodb', region_name="us-west-1")
    past_transactions = []
    # Get most recent transaction for each client from DynamoDB from Transactions Table.
    for budget in update_budgets:
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
        
        # If new budget is 20% higher than old budget, add to budget_errors array and remove from update_budgets array.
        # if new budget is less than 10% of the old budget, just add to budget_errors array and still update.

    print('Update Budgets: ', update_budgets)
    print('New Budgets: ', new_budgets)



    
        





# Intialize connections & pass arguments to main()
if __name__ == "__main__":
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
        main(args.customer_id)
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

