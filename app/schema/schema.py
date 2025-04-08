from typing import List, Optional
import strawberry
from datetime import datetime
from ..plaid.client import PlaidClient

@strawberry.type
class Transaction:
    id: str
    amount: float
    date: str
    name: str
    category: Optional[str]
    merchant_name: Optional[str]

@strawberry.type
class Query:
    @strawberry.field
    def test(self) -> str:
        return "Hello World from backend"

    @strawberry.field
    async def transactions(
        self,
        access_token: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Transaction]:
        plaid_client = PlaidClient()
        
        # Convert string dates to datetime if provided
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
        
        transactions = await plaid_client.get_transactions(
            access_token=access_token,
            start_date=start_date_dt,
            end_date=end_date_dt
        )
        
        return [
            Transaction(
                id=t['id'],
                amount=t['amount'],
                date=t['date'],
                name=t['name'],
                category=t['category'],
                merchant_name=t['merchant_name']
            )
            for t in transactions
        ]

schema = strawberry.Schema(query=Query) 