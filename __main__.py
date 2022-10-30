from gspread_operations.gspread_operations import *

def handleProgramErrors(errors):
    print(errors)

def main():
    # Intialize Arrays
    update_budgets = []
    new_budgets = []
    budget_errors = []

    # Read worksheet data
    readSheetDataResponse = readSheetData()
    # If response is error, raise as critical error.
    if (readSheetDataResponse['status'] == 'error'):
        raise Exception(readSheetDataResponse['data'])
    # Add to update_budgets array.
    update_budgets.extend(readSheetDataResponse['data'])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Error: ' + str(e))