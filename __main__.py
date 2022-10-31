from gspread_operations.gspread_operations import *
from aws_operations.aws_operations import *

def main():
    # Read worksheet data
    readSheetDataResponse = readSheetData()
    # If response is error, raise as critical error.
    if (readSheetDataResponse['status'] == 'error'):
        raise Exception(readSheetDataResponse['data'])

    # Read DynamoDB data
    readDynamoDBResponse = readDynamoDB(readSheetDataResponse['data'])
    # If response is error, raise as critical error.
    if (readDynamoDBResponse['status'] == 'error'):
        raise Exception(readDynamoDBResponse['data'])
    
    # Handle budgets
    print(readDynamoDBResponse['data'])



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Error: ' + str(e))