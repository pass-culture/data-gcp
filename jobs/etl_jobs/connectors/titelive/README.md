# Titelive API Connector

Connector for the Titelive/Epagine product catalog API.

## API Characteristics

- **Base URL**: `https://catsearch.epagine.fr/v1`
- **Authentication**: Bearer token (username/password → JWT token)
- **Token Lifetime**: 5 minutes (auto-refreshed at 4.5 minutes)
- **Rate Limits** (from load testing):
  - Burst capacity: 70 req/s for ~2000 requests
  - Sustained capacity: 30 req/s
  - Recovery period: 15 seconds after burst

## Endpoints

### GET /ean

Fetch product data by EAN codes.

- **Max EANs per request**: 250
- **Parameters**: `in=ean={ean1}|{ean2}|...`, optional `base={category}`

### GET /search

Search products by modification date.

- **Parameters**: `base`, `dateminm`, `datemaxm`, `page`, `nombre`
- **Max results**: 20,000 per query

## Configuration

Credentials are fetched from GCP Secret Manager:

- `titelive_epagine_api_username`
- `titelive_epagine_api_password`

## Usage

```python
from factories.titelive import TiteliveFactory

# Create connector via factory
connector = TiteliveFactory.create_connector(project_id="your-project-id")

# Fetch products by EAN
response = connector.get_by_eans(["9782070612758", "9782070584628"])

# Search by date
response = connector.search_by_date(
    base="paper",
    min_date="01/01/2024",
    max_date="31/01/2024",
    page=1,
)
```

## Migration from Legacy Client

### Before (external/titelive/src/api/client.py):

```python
from src.api.auth import TokenManager
from src.api.client import TiteliveClient

token_manager = TokenManager(project_id)
client = TiteliveClient(token_manager)
response = client.get_by_eans(ean_list)
```

### After (connectors/titelive/):

```python
from factories.titelive import TiteliveFactory

connector = TiteliveFactory.create_connector(project_id=project_id)
response = connector.get_by_eans(ean_list)
```
