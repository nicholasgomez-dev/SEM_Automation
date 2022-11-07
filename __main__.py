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
    # Log errors to DynamoDB
    logErrorsResponse = logErrors(budget_errors)
    if (logErrorsResponse['status'] == 'error'):
        return logErrorsResponse
    if (len(logErrorsResponse['data']['errors']) > 0):
        logErrorsResponse['data'] = 'The following error objects could not be logged to the errors table: '.join([repr(error) for error in logErrorsResponse['data']['errors']])
        return logErrorsResponse
    
    # Get error contacts
    getErrorContactsResponse = getErrorContacts()
    if (getErrorContactsResponse['status'] == 'error'):
        return getErrorContactsResponse

    # Send error emails to contacts
    sendErrorEmailsResponse = sendErrorEmails(getErrorContactsResponse['data']['contacts'], budget_errors)
    if (sendErrorEmailsResponse['status'] == 'error'):
        return sendErrorEmailsResponse
    

# Main function
def main():
    # Read worksheet data
    readSheetDataResponse = readSheetData()
    if (readSheetDataResponse['status'] == 'error'):
        raise Exception(readSheetDataResponse['data'])

    # Read DynamoDB & sort data
    getTransacationsResponse = getTransacations(readSheetDataResponse['data'])
    if (getTransacationsResponse['status'] == 'error'):
        raise Exception(getTransacationsResponse['data'])

    # Handle budget updates
    if (len(getTransacationsResponse['data']['update']) > 0):
        handleBudgetUpdatesResponse = handleBudgetUpdates(getTransacationsResponse['data']['update'])
        if (handleBudgetUpdatesResponse['status'] == 'error'):
            raise Exception(handleBudgetUpdatesResponse['data'])

    # Handle new budgets
    if (len(getTransacationsResponse['data']['new']) > 0):
        handleNewBudgetsResponse = handleNewBudgets(getTransacationsResponse['data']['new'])
        if (handleNewBudgetsResponse['status'] == 'error'):
            raise Exception(handleNewBudgetsResponse['data'])

    # Handle budget errors
    handleBudgetErrorsResponse = handleBudgetErrors(getTransacationsResponse['data']['errors'] + handleBudgetUpdatesResponse['data']['errors'] + handleNewBudgetsResponse['data']['errors'])
    if (handleBudgetErrorsResponse['status'] == 'error'):
        raise Exception(handleBudgetErrorsResponse['data'])
    
    if (handleBudgetErrorsResponse['status'] == 'success'):
        return print('SEM Budgets Updated Successfully')

# Run main function
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Error: ' + str(e))