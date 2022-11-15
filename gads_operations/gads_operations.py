import uuid
from google.api_core import protobuf_helpers
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def updateBudgets(budget_list):
    try:
        # Initialize client object, Service, & Operation
        client = GoogleAdsClient.load_from_storage('./gads_operations/gads.yaml')
        campaign_budget_service = client.get_service("CampaignBudgetService")
        campaign_budget_operation = client.get_type("CampaignBudgetOperation")
        # Initialize return object
        return_data = {
            'status': 'success',
            'data': {
                'successful': [],
                'failed': []
            }
        }
        # Loop through budget list & update
        for budget in budget_list:
            # Convert budget amount to micros
            budget_to_micros = int(float(budget['Campaign Budget'].replace('$', ''))) * 1000000
            # Mutate campaign budget
            campaign_budget = campaign_budget_operation.update
            campaign_budget.resource_name = budget['Resource Name']
            campaign_budget.amount_micros = budget_to_micros
            # Create field mask
            field_mask = protobuf_helpers.field_mask(None, campaign_budget._pb)
            try:
                # Copy field mask to operation
                client.copy_from(campaign_budget_operation.update_mask, field_mask)
                # Process update
                campaign_budget_service.mutate_campaign_budgets(customer_id=budget['Client ID'], operations=[campaign_budget_operation])
                return_data['data']['successful'].append(budget)
            except Exception:
                err_obj = {
                    'Client ID': budget['Client ID'],
                    'Campaign ID': budget['Campaign ID'],
                    'Resource Name': budget['Resource Name'],
                    'Budget': budget['Campaign Budget'],
                    'Error Type': 'Budget_Update_Error',
                    'Error Message': 'Budget update failed.',
                    'Error Object': repr(budget)
                }
                return_data['data']['failed'].append(err_obj)
                continue
        return return_data

    except Exception as e:
        print(e)
        return_data = {
            'status': 'error',
            'data': 'Error updating budgets.'
        }
        return return_data

def createNewBudgets(budget_list):
    try:
        # Initialize client object, Service, & Operation
        client = GoogleAdsClient.load_from_storage('./gads_operations/gads.yaml')
        campaign_budget_service = client.get_service("CampaignBudgetService")
        campaign_budget_operation = client.get_type("CampaignBudgetOperation")
        # Initialize return object
        return_data = {
            'status': 'success',
            'data': {
                'successful': [],
                'failed': []
            }
        }
        # Loop through new budget list & create
        for new_budget in budget_list:
            # Convert budget amount to micros
            budget_to_micros = int(float(new_budget['Campaign Budget'].replace('$', ''))) * 1000000
            # Create campaign budget
            campaign_budget = campaign_budget_operation.create
            campaign_budget.name = "Automated_Budget_" + str(new_budget['Client ID']) + '_' + str(new_budget['Campaign ID']) + '_' + str(uuid.uuid4())
            campaign_budget.delivery_method = (client.enums.BudgetDeliveryMethodEnum.STANDARD)
            campaign_budget.amount_micros = budget_to_micros
            campaign_budget.explicitly_shared = True
            try:
                campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(customer_id=str(new_budget['Client ID']), operations=[campaign_budget_operation])
                new_budget['Resource Name'] = campaign_budget_response.results[0].resource_name
                return_data['data']['successful'].append(new_budget)
            except GoogleAdsException as ex:
                for error in ex.failure.errors:
                    err_obj = {
                        'Client ID': str(new_budget['Client ID']),
                        'Campaign ID': str(new_budget['Campaign ID']),
                        'Budget': new_budget['Campaign Budget'],
                        'Error Type': 'Budget_Create_Error',
                        'Error Message': 'Budget creation failed: ' + error.message,
                        'Error Object': repr(new_budget)
                    }
                    return_data['data']['failed'].append(err_obj)
                    continue
            except Exception as e:
                err_obj = {
                    'Client ID': str(new_budget['Client ID']),
                    'Campaign ID': str(new_budget['Campaign ID']),
                    'Budget': new_budget['Campaign Budget'],
                    'Error Type': 'Budget_Create_Error',
                    'Error Message': 'Budget creation failed.',
                    'Error Object': repr(new_budget)
                }
                return_data['data']['failed'].append(err_obj)
                continue
        return return_data
    
    except Exception as e:
        print(e)
        return_data = {
            'status': 'error',
            'data': 'Error creating new budgets.'
        }
        return return_data

def assignBudgets(budget_list):
    try:
        # Initialize client object, Service, & Operation
        client = GoogleAdsClient.load_from_storage('./gads_operations/gads.yaml')
        campaign_service = client.get_service("CampaignService")
        campaign_operation = client.get_type("CampaignOperation")
        # Initialize return object
        return_data = {
            'status': 'success',
            'data': {
                'successful': [],
                'failed': []
            }
        }
        # Loop through new budget list & assign to campaigns
        for budget in budget_list:
            # Assign budget to campaign
            campaign = campaign_operation.update
            campaign.resource_name = campaign_service.campaign_path(str(budget['Client ID']), str(budget['Campaign ID']))
            campaign.campaign_budget = budget['Resource Name']
            try:
                # Copy field mask to operation
                client.copy_from(campaign_operation.update_mask, protobuf_helpers.field_mask(None, campaign._pb))
                # Process update
                campaign_service.mutate_campaigns(customer_id=str(budget['Client ID']), operations=[campaign_operation])
                return_data['data']['successful'].append(budget)
            except Exception as e:
                err_obj = {
                    'Client ID': str(budget['Client ID']),
                    'Campaign ID': str(budget['Campaign ID']),
                    'Budget': budget['Campaign Budget'],
                    'Error Type': 'Budget_Assignment_Error',
                    'Error Message': 'Budget assignment failed.',
                    'Error Object': repr(budget)
                }
                return_data['data']['failed'].append(err_obj)
                continue
        return return_data
    except Exception as e:
        print(e)
        return_data = {
            'status': 'error',
            'data': 'Error assigning budgets.'
        }
        return return_data