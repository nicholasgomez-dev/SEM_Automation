from google.api_core import protobuf_helpers
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

def updateBudgets(budget_list):
    try:
        client = GoogleAdsClient.load_from_storage('./gads_operations/gads.yaml')
        campaign_budget_service = client.get_service("CampaignBudgetService")
        campaign_budget_operation = client.get_type("CampaignBudgetOperation")

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

            # Mutate budget request
            campaign_budget_response = campaign_budget_service.mutate_campaign_budgets(
                customer_id="4714359917", operations=[campaign_budget_operation]
            )

            print(f"Updated campaign {campaign_budget_response.results[0].resource_name}.")
        
        return budget_list

    except Exception as e:
        return_data = {
            'status': 'error',
            'data': 'Error updating budgets.'
        }

        print(e)

        return return_data