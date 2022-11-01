from logging import error
import boto3
from boto3.dynamodb.conditions import Key

def readDynamoDB(update_budgets):
    try:
        # Init DynamoDB client
        dynamodb_client = boto3.client('dynamodb', region_name="us-west-1")
        # Initialize return array
        return_data = {
            'status': 'success',
            'data': {
                'new': [],
                'update': [],
                'errors': []
            }
        }
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
            # If client has no transactions
            if len(response['Items']) == 0:
                return return_data['data']['new'].append(budget)

            # Add budget resource name
            budget['Resource Name'] = response['Items'][0]['ResourceName']['S']

            # Get tolerance levels
            max_tolerance =  (float(response['Items'][0]['Budget']['N']) * int(20) / int(100)) + float(response['Items'][0]['Budget']['N'])
            min_tolerance =  (float(response['Items'][0]['Budget']['N']) * int(10) / int(100))
            
            # If new budget is 20% higher or more than last budget
            if float(response['Items'][0]['Budget']['N']) > max_tolerance:
                error_obj = {
                    'Client ID': budget['Client ID'],
                    'Campaign ID': budget['Campaign ID'],
                    'Proposed Budget': float(budget['Campaign Budget']),
                    'Last Budget': float(response['Items'][0]['Budget']['N']),
                    'Error Type': 'Budget Increase',
                    'Error Message': 'Budget increase is 20% higher than the last budget.'
                }
                return return_data['data']['errors'].append(error_obj)

            # If new budget is lower than 10% of the last budget
            if float(response['Items'][0]['Budget']['N']) < min_tolerance:
                error_obj = {
                    'Client ID': budget['Client ID'],
                    'Campaign ID': budget['Campaign ID'],
                    'Proposed Budget': float(budget['Campaign Budget']),
                    'Last Budget': float(response['Items'][0]['Budget']['N']),
                    'Error Type': 'Budget Decrease',
                    'Error Message': 'Budget decrease is lower than 10% of the last budget.'
                }
                return_data['data']['errors'].append(error_obj)

            # Add budget to update array
            return_data['data']['update'].append(budget)

        return return_data
    
    except Exception as e:
        return_data = {
            'status': 'error',
            'data': 'Error reading DynamoDB data.'
        }

        print(e)

        return return_data
