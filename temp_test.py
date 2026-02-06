
import sys
from unittest.mock import MagicMock

# Mock asana and its components
sys.modules[\"asana\"] = MagicMock()
import asana
from asana.pagination.page_iterator import PageIterator

mock_client = MagicMock()
mock_client.call_api.return_value = {\"data\": [{\"gid\": \"1\"}, {\"gid\": \"2\"}], \"next_page\": None}

it = PageIterator(mock_client, {\"resource_path\": \"/x\", \"method\": \"GET\", \"query_params\": {}})
print(f\"List: {list(it)}\")

