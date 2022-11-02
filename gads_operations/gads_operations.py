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
            # Copy field mask to operation
            client.copy_from(campaign_budget_operation.update_mask, field_mask)
            try:
                campaign_budget_service.mutate_campaign_budgets(customer_id=budget['Client ID'], operations=[campaign_budget_operation])
                return_data['data']['successful'].append(budget)
            except Exception:
                return_data['data']['failed'].append(budget)
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
        client = GoogleAdsClient.load_from_storage('./gads_operations/gads.yaml')
        # Create budget
        # Return successful and failed lists
    
    except Exception as e:
        print(e)
        return_data = {
            'status': 'error',
            'data': 'Error creating new budgets.'
        }
        return return_data

def assignBudgets(budget_list):
    print(budget_list)