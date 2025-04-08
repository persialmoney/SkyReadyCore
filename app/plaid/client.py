import os
from typing import List, Optional
import plaid
from plaid.api import plaid_api
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from datetime import datetime, timedelta

class PlaidClient:
    def __init__(self):
        configuration = plaid.Configuration(
            host=plaid.Environment.Sandbox,  # Change to Development or Production as needed
            api_key={
                'clientId': os.getenv('PLAID_CLIENT_ID'),
                'secret': os.getenv('PLAID_SECRET'),
            }
        )
        api_client = plaid.ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)

    async def get_transactions(self, access_token: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[dict]:
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()

        try:
            request = TransactionsGetRequest(
                access_token=access_token,
                start_date=start_date.date(),
                end_date=end_date.date(),
                options=TransactionsGetRequestOptions(
                    include_personal_finance_category=True
                )
            )
            response = self.client.transactions_get(request)
            return [
                {
                    'id': transaction.transaction_id,
                    'amount': transaction.amount,
                    'date': transaction.date,
                    'name': transaction.name,
                    'category': transaction.personal_finance_category.primary if transaction.personal_finance_category else None,
                    'merchant_name': transaction.merchant_name,
                }
                for transaction in response.transactions
            ]
        except plaid.ApiException as e:
            print(f"Error fetching transactions: {e}")
            raise 