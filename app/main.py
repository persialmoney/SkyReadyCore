from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from .schema.schema import schema
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(title="SkyReady Core API")

# Create GraphQL router
graphql_app = GraphQLRouter(schema)

# Add GraphQL endpoint
app.include_router(graphql_app, prefix="/graphql")

@app.get("/health")
async def health_check():
    return {"status": "healthy"} 