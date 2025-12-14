import strawberry

@strawberry.type
class Query:
    @strawberry.field
    def test(self) -> str:
        return "Hello World from backend"

schema = strawberry.Schema(query=Query) 