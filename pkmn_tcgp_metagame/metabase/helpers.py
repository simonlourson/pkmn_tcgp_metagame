import aiohttp
import dagster

headers_content_json = {"content-type": "application/json"}
archived_payload = {"archived": True}
refresh_card_payload = {
  "ignore_cache": False,
  "collection_preview": False,
  "parameters": [],
}


async def get_databases(log: dagster.DagsterLogManager, session: aiohttp.ClientSession):
  resp = await session.get("/api/database")
  resp.raise_for_status()
  return await resp.json()


async def get_database(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession, database_name: str
):
  databases = await get_databases(log, session)
  for db in databases["data"]:
    if db["name"] == database_name:
      return db

  return None


async def get_collections(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession
):
  resp = await session.get("/api/collection")
  resp.raise_for_status()
  return await resp.json()


async def get_collection(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession, collection_name: str
):
  collections = await get_collections(log, session)
  for collection in collections:
    if collection["name"] == collection_name:
      return collection

  return None


async def get_cards(log: dagster.DagsterLogManager, session: aiohttp.ClientSession):
  resp = await session.get("/api/card")
  resp.raise_for_status()
  return await resp.json()


async def get_card(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession, card_name: str
):
  cards = await get_cards(log, session)
  for card in cards:
    if card["name"] == card_name and not card["archived"]:
      return card

  return None


async def get_dahsboards(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession
):
  resp = await session.get("/api/dashboard")
  resp.raise_for_status()
  return await resp.json()


async def get_dashboard(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession, dashboard_name: str
):
  dashboards = await get_dahsboards(log, session)
  for dashboard in dashboards:
    if dashboard["name"] == dashboard_name and not dashboard["archived"]:
      return dashboard

  return None


def create_card_payload(
  card_name: str,
  database_id: str,
  query: str,
  display: str = "table",
  visualization_settings: dict = {},
  parameters: list = [],
  template_tags: dict = {},
):
  return {
    "name": card_name,
    "type": "question",
    "dataset_query": {
      "database": database_id,
      "type": "native",
      "native": {"template-tags": template_tags, "query": query},
    },
    "display": display,
    "description": None,
    "visualization_settings": visualization_settings,
    "parameters": parameters,
    "collection_position": None,
    "result_metadata": None,
    "collection_id": None,
  }


async def archive_card_if_exists(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession, card_name: str
):
  card = await get_card(log, session, card_name)
  if card is not None:
    resp = await session.put(
      f"/api/card/{card['id']}", headers=headers_content_json, json=archived_payload
    )
    resp.raise_for_status()


async def archive_dashboard_if_exists(
  log: dagster.DagsterLogManager, session: aiohttp.ClientSession, dashboard_name: str
):
  dashboard = await get_dashboard(log, session, dashboard_name)
  if dashboard is not None:
    resp = await session.put(
      f"/api/dashboard/{dashboard['id']}",
      headers=headers_content_json,
      json=archived_payload,
    )
    resp.raise_for_status()


async def create_card(
  log: dagster.DagsterLogManager,
  session: aiohttp.ClientSession,
  card_name: str,
  card_payload: dict,
  refresh_data=False,
):
  await archive_card_if_exists(log, session, card_name)

  resp = await session.post(
    "/api/card", headers=headers_content_json, json=card_payload
  )
  resp.raise_for_status()

  json = await resp.json()

  if refresh_data:
    refresh = await session.post(
      f"/api/card/{json['id']}/query",
      headers=headers_content_json,
      json=refresh_card_payload,
    )
    refresh.raise_for_status()

  return json


async def create_dashboard(
  log: dagster.DagsterLogManager,
  session: aiohttp.ClientSession,
  dashboard_name: str,
  dashboard_payload: dict,
):
  await archive_dashboard_if_exists(log, session, dashboard_name)

  dashboard_creation_payload = {
    "collection_id": None,
    "description": None,
    "name": dashboard_name,
  }
  resp = await session.post(
    "/api/dashboard", headers=headers_content_json, json=dashboard_creation_payload
  )
  resp.raise_for_status()
  return_payload = await resp.json()

  dashboard_id = return_payload["id"]
  del return_payload["id"]

  return_payload["parameters"] = dashboard_payload["parameters"]
  return_payload["dashcards"] = dashboard_payload["dashcards"]

  resp = await session.put(
    f"/api/dashboard/{dashboard_id}", headers=headers_content_json, json=return_payload
  )
  resp.raise_for_status()

  return await resp.json()
