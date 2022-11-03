from gads_operations.gads_operations import *
from gspread_operations.gspread_operations import *
from aws_operations.aws_operations import *


# Handle budget updates
def handleBudgetUpdates(update_list):
    # Update Google Ads budgets
    updateBudgetsResponse = updateBudgets(update_list)
    if (updateBudgetsResponse['status'] == 'error'):
        return updateBudgetsResponse
    
    # Log successfull transactions
    logTransactionsResponse = logTransactions(updateBudgetsResponse['data']['successful'])
    if (logTransactionsResponse['status'] == 'error'):
        return logTransactionsResponse

    # Return any errors
    return {
        'status': 'success',
        'data': {
            'errors': updateBudgetsResponse['data']['failed'] + logTransactionsResponse['data']['errors']
        }
    }


# Handle new budgets
def handleNewBudgets(new_budgets):
    # Create new budgets
    createNewBudgetsResponse = createNewBudgets(new_budgets)
    if (createNewBudgetsResponse['status'] == 'error'):
        return createNewBudgetsResponse
    
    # Assign budgets to campaigns
    assignBudgetsResponse = assignBudgets(createNewBudgetsResponse['data']['successful'])
    if (assignBudgetsResponse['status'] == 'error'):
        return assignBudgetsResponse
    
    # Log successful transactions
    logTransactionsResponse = logTransactions(assignBudgetsResponse['data']['successful'])
    if (logTransactionsResponse['status'] == 'error'):
        return logTransactionsResponse
    
    # Return any errors
    return {
        'status': 'success',
        'data': {
            'errors': createNewBudgetsResponse['data']['failed'] + assignBudgetsResponse['data']['failed'] + logTransactionsResponse['data']['errors']
        }
    }


# Handle budget errors
def handleBudgetErrors(budget_errors):
    print(budget_errors)
    

# Main function
def main():
    # Read worksheet data
    readSheetDataResponse = readSheetData()
    if (readSheetDataResponse['status'] == 'error'):
        raise Exception(readSheetDataResponse['data'])

    # Read DynamoDB & sort data
    readDynamoDBResponse = readDynamoDB(readSheetDataResponse['data'])
    if (readDynamoDBResponse['status'] == 'error'):
        raise Exception(readDynamoDBResponse['data'])

    # Handle budget updates
    if (len(readDynamoDBResponse['data']['update']) > 0):
        handleBudgetUpdatesResponse = handleBudgetUpdates(readDynamoDBResponse['data']['update'])
        if (handleBudgetUpdatesResponse['status'] == 'error'):
            raise Exception(handleBudgetUpdatesResponse['data'])

    # Handle new budgets
    if (len(readDynamoDBResponse['data']['new']) > 0):
        handleNewBudgetsResponse = handleNewBudgets(readDynamoDBResponse['data']['new'])
        if (handleNewBudgetsResponse['status'] == 'error'):
            raise Exception(handleNewBudgetsResponse['data'])
        print(handleNewBudgetsResponse)
    # # Handle new budgets
    # handleBudgetErrors(readDynamoDBResponse['data']['errors'] + handleBudgetUpdatesResponse['data']['errors'] + handleNewBudgetsResponse['data']['errors'])


# Run main function
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Error: ' + str(e))