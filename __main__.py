from gads_operations.gads_operations import *
from gspread_operations.gspread_operations import *
from aws_operations.aws_operations import *


# Handle budget updates
def handleUpdates(update_list):
    # Update Google Ads budgets
    updateBudgetsResponse = updateBudgets(update_list)
    if (updateBudgetsResponse['status'] == 'error'):
        raise Exception(updateBudgetsResponse['data'])
    print(updateBudgetsResponse['data'])

    # Handle Successful Updates
    # Handle Errors that occured during updates
    
    return updateBudgetsResponse

# Run the main function
def main():
    # Read worksheet data
    readSheetDataResponse = readSheetData()
    if (readSheetDataResponse['status'] == 'error'):
        raise Exception(readSheetDataResponse['data'])

    # Read DynamoDB & sort data
    readDynamoDBResponse = readDynamoDB(readSheetDataResponse['data'])
    if (readDynamoDBResponse['status'] == 'error'):
        raise Exception(readDynamoDBResponse['data'])

    handleUpdatesResponse = handleUpdates(readDynamoDBResponse['data']['update'])


# Run main function
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Error: ' + str(e))