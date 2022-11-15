import datetime
import boto3
from botocore.exceptions import ClientError

def getTransacations(update_budgets):
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
            # Query DynamoDB
            response = dynamodb_client.query(
                TableName="SEM_Automation_Transactions",
                KeyConditionExpression='ClientID = :clientID',
                ExpressionAttributeValues={
                    ':clientID': {'S': str(budget['Client ID'])}
                },
                ScanIndexForward=False,
                Limit=1
            )

            # If client has no transactions
            if len(response['Items']) < 1:
                return_data['data']['new'].append(budget)
                continue

            # Set all properties to string values & add resource name
            budget['Resource Name'] = response['Items'][0]['ResourceName']['S']
            budget['Client ID'] = response['Items'][0]['ClientID']['S']
            budget['Campaign ID'] = response['Items'][0]['CampaignID']['S']
            # Get tolerance levels
            max_tolerance =  (float(response['Items'][0]['Budget']['N']) * int(20) / int(100)) + float(response['Items'][0]['Budget']['N'])
            min_tolerance =  (float(response['Items'][0]['Budget']['N']) * int(10) / int(100))
            
            # If new budget is 20% higher or more than last budget
            if (float(response['Items'][0]['Budget']['N']) > max_tolerance):
                error_obj = {
                    'Client ID': budget['Client ID'],
                    'Campaign ID': budget['Campaign ID'],
                    'Error Type': 'Budget_Increase',
                    'Error Message': 'Budget increase is 20% higher than the last budget.',
                    'Error Object': repr(budget)
                }
                return_data['data']['errors'].append(error_obj)
                continue

            # If new budget is lower than 10% of the last budget
            if (float(response['Items'][0]['Budget']['N']) < min_tolerance):
                error_obj = {
                    'Client ID': budget['Client ID'],
                    'Campaign ID': budget['Campaign ID'],
                    'Error Type': 'Budget_Decrease',
                    'Error Message': 'Budget decrease is less than 10% of the last budget.',
                    'Error Object': repr(budget)
                }
                return_data['data']['errors'].append(error_obj)
            # Add budget to update array
            return_data['data']['update'].append(budget)

        return return_data
    
    except Exception as e:
        return_data = {
            'status': 'error',
            'data': 'Error reading DynamoDB budget transaction data.'
        }

        print(e)

        return return_data

def logTransactions(updated_list):
    # Initialize return object
    return_data = {
        'status': 'success',
        'data': {
            'errors': []
        }
    }
    try:
        # Init DynamoDB client
        dynamodb_client = boto3.client('dynamodb', region_name="us-west-1")
        # Log each transaction to DynamoDB Table
        for success in updated_list:
            # Add transaction to DynamoDB
            try:
                dynamodb_client.put_item(
                    TableName="SEM_Automation_Transactions",
                    Item={
                        'ClientID': {'S': str(success['Client ID'])},
                        'CampaignID': {'S': str(success['Campaign ID'])},
                        'TransactionID': {'S': str(datetime.datetime.now()).replace(' ', '')},
                        'ResourceName': {'S': str(success['Resource Name'] or success['Budget Resource Name'])},
                        'Budget': {'N': str(success['Campaign Budget'].replace('$', ''))}
                    }
                )
            except Exception as e:
                print(e)
                err_obj = {
                    'Client ID': success['Client ID'],
                    'Campaign ID': success['Campaign ID'],
                    'Error Type': 'Budget_Transaction_Error',
                    'Error Message': 'Budget transaction failed to log to table.',
                    'Error Object': repr(success)
                }
                return_data['data']['errors'].append(err_obj)
                continue
        return return_data
    
    except Exception as e:
        print(e)
        return_data = {
            'status': 'error',
            'data': 'Error logging transactions to DynamoDB.'
        }
        return return_data

def logErrors(error_list):
    # Initialize return object
    return_data = {
        'status': 'success',
        'data': {
            'errors': []
        }
    }
    try:
        # Init DynamoDB client
        dynamodb_client = boto3.client('dynamodb', region_name="us-west-1")
        # Log each transaction to DynamoDB Table
        for error in error_list:
            # Add transaction to DynamoDB
            try:
                dynamodb_client.put_item(
                    TableName="SEM_Automation_Errors",
                    Item={
                        'ClientID': {'S': str(error['Client ID'])},
                        'CampaignID': {'S': str(error['Campaign ID'])},
                        'ErrorID': {'S': str(datetime.datetime.now()).replace(' ', '')},
                        'ErrorType': {'S': error['Error Type']},
                        'ErrorMessage': {'S': error['Error Message']},
                        'ErrorObject': {'S': error['Error Object']}
                    }
                )
            except ClientError as e:
                print(e.response['Error']['Message'])
            except Exception as e:
                err_obj = {
                    'ClientID': {'S': str(error['Client ID'])},
                    'CampaignID': {'S': str(error['Campaign ID'])},
                    'ErrorID': {'S': str(datetime.datetime.now()).replace(' ', '')},
                    'ErrorType': {'S': error['Error Type']},
                    'ErrorMessage': {'S': error['Error Message']},
                    'ErrorObject': {'S': error['Error Object']}
                }
                return_data['data']['errors'].append(err_obj)
                continue
        return return_data
    
    except Exception as e:
        return_data = {
            'status': 'error',
            'data': 'Error logging errors to DynamoDB.'
        }
        return return_data

def getErrorContacts():
    try:
        # Initialize DynamoDB client
        dynamodb_client = boto3.client('dynamodb', region_name="us-west-1")
        # Initialize return object
        return_data = {
            'status': 'success',
            'data': {
                'contacts': []
            }
        }
        # Get all contacts from DynamoDB
        response = dynamodb_client.scan(TableName="SEM_Automation_Contacts")
        return_data['data']['contacts'].extend(response['Items'])
        # Scan for more contacts
        while 'LastEvaluatedKey' in response:
            response = dynamodb_client.scan(TableName="SEM_Automation_Contacts", ExclusiveStartKey=response['LastEvaluatedKey'])
            return_data['data']['contacts'].extend(response['Items'])

        return return_data

    except Exception as e:
        return_data = {
            'status': 'error',
            'data': 'Error reading DynamoDB error contact data.'
        }

        print(e)

        return return_data

def sendErrorEmails(contact_data, error_list):
    if (len(error_list) > 0):
        try:
            ses_client = boto3.client("ses", region_name="us-west-1")
            # Initialize return object
            return_data = {
                'status': 'success'
            }
            # Construct contact list with list comprehension
            contact_list = [contact['Email']['S'] for contact in contact_data]
            # Construct error strings list with list comprehension
            error_strings = [('For Client ID: ' + str(error['Client ID']) + ', Campaign ID: ' + str(error['Campaign ID']) + ', Error Type: ' + error['Error Type'] + ', Error Message: ' + error['Error Message']) for error in error_list]
            # Constuct email body
            ses_client.send_email(
                Destination={
                    "ToAddresses": contact_list
                },
                Message={
                    "Body": {
                        "Text": {
                            "Charset": "UTF-8",
                            "Data": ' '.join(error_strings),
                        }
                    },
                    "Subject": {
                        "Charset": "UTF-8",
                        "Data": 'SEM Automation Error Report',
                    },
                },
                Source="lrouter@mxssolutions.com"
            )
            return return_data
        except Exception as e:
            print(e)
            return {
                'status': 'error',
                'data': 'Error sending erorr emails.'
            }
    else:
        return {
            'status': 'success'
        }
