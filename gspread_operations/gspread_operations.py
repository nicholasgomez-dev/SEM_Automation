import gspread

def readSheetData():
    try:
        #Initialize client
        googlesheets_client = gspread.service_account(filename='./gspread_operations/gspread.json')
        # Select Google Sheet File
        sem_automation_sheet = googlesheets_client.open("SEM Automation Test Sheet")
        # Get all data from Send worksheet.
        worksheet_data = sem_automation_sheet.worksheet("Send").get_all_records()
        # Return worksheet data
        return_data = {
            'status': 'success',
            'data': worksheet_data
        }
        return return_data

    except Exception:
        error_data = {
            'status': 'error',
            'data': 'Error reading Google Sheet data.'
        }
        return error_data