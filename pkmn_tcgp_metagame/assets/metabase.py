import aiohttp
import dagster

from pkmn_tcgp_metagame.assets.metabase_payloads.cards.average_card_count_one_deck_one_season import (
  average_card_count_one_deck_one_season,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.cards.details_one_deck_one_season import (
  details_one_deck_one_season,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.cards.usage_rate_one_deck_all_weeks import (
  usage_rate_one_deck_all_weeks,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.cards.usage_rate_one_deck_days_since_season_start import (
  usage_rate_one_deck_days_since_season_start,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.cards.usage_rate_one_deck_one_season_all_cards import (
  usage_rate_one_deck_one_season_all_cards,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.cards.usage_rate_win_rate_one_season_top_decks import (
  usage_rate_win_rate_one_season_top_decks,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.cards.win_rate_against_deck_one_season import (
  win_rate_against_deck_one_season,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.dashboards.deck_deep_dive import (
  get_payload,
)
from pkmn_tcgp_metagame.assets.metabase_payloads.dashboards.season_overview import (
  get_payload_season_overview,
)
from pkmn_tcgp_metagame.metabase.helpers import (
  create_card,
  create_card_payload,
  create_dashboard,
  get_collection,
  get_database,
)
from pkmn_tcgp_metagame.metabase.metabase_resource import MetabaseResource

headers_accept_json = {"accept": "application/json"}
headers_content_json = {"content-type": "application/json"}

default_deck = "Dracaufeu-ex (A1), Sulfura-ex (A1)"


@dagster.asset(
  group_name="metabase",
  kinds=["python", "metabase"],
)
async def user_admin(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource
) -> dagster.MaterializeResult:
  """Metabase instance with the admin user created"""
  async with aiohttp.ClientSession(base_url=metabase.host) as session:
    resp = await session.get("/api/session/properties", headers=headers_accept_json)
    json = await resp.json()
    setup_token = json["setup-token"]

    setup_data = {
      "token": setup_token,
      "user": {
        "first_name": dagster.EnvVar("METABASE_ADMIN_FIRST_NAME").get_value(),
        "last_name": dagster.EnvVar("METABASE_ADMIN_LAST_NAME").get_value(),
        "email": dagster.EnvVar("METABASE_ADMIN_USER").get_value(),
        "site_name": dagster.EnvVar("METABASE_ADMIN_SITE_NAME").get_value(),
        "password": dagster.EnvVar("METABASE_ADMIN_PASSWORD").get_value(),
        "password_confirm": dagster.EnvVar("METABASE_ADMIN_PASSWORD").get_value(),
      },
      "prefs": {
        "site_name": dagster.EnvVar("METABASE_ADMIN_SITE_NAME").get_value(),
        "site_locale": "en",
      },
    }

    resp = await session.post(
      "/api/setup", headers=headers_content_json, json=setup_data
    )

    try:
      resp.raise_for_status()
      context.log.info("admin user was created")
    except aiohttp.ClientResponseError:
      context.log.warning("ClientResponseError : admin creation can only be run once")


@dagster.asset(group_name="metabase", kinds=["python", "metabase"], deps=["user_admin"])
async def deleted_samples(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource
):
  """Metabase instance with the sample database and example collection deleted"""
  async with metabase.get_session() as session:
    sample_database_name = "Sample Database"
    database = await get_database(context.log, session, sample_database_name)
    if database is not None:
      await session.delete(f"api/database/{database['id']}")
    else:
      context.log.warning(f"database {sample_database_name} does not exist")

    example_collection_name = "Examples"
    collection_id = await get_collection(context.log, session, example_collection_name)
    if collection_id is not None:
      await session.put(
        f"api/collection/{collection_id['id']}",
        headers=headers_content_json,
        json={"archived": True},
      )
    else:
      context.log.warning(f"collection {example_collection_name} does not exist")


@dagster.asset(group_name="metabase", kinds=["python", "metabase"], deps=["user_admin"])
async def database_pokemon(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource
) -> dict:
  """Metabase database containing Pokemon TCG Pocket metagame analysis"""
  async with metabase.get_session() as session:
    database_name = "Pokemon TCG Pocket Metagame"
    database = await get_database(context.log, session, database_name)

    if database is not None:
      return database

    database_payload = {
      "is_on_demand": False,
      "is_full_sync": True,
      "is_sample": False,
      "cache_ttl": None,
      "refingerprint": False,
      "auto_run_queries": True,
      "schedules": {},
      "details": {
        "host": dagster.EnvVar("POSTGRES_HOST_DOCKER").get_value(),
        "port": dagster.EnvVar("POSTGRES_PORT").get_value(),
        "dbname": dagster.EnvVar("POSTGRES_DB").get_value(),
        "user": dagster.EnvVar("POSTGRES_USER").get_value(),
        "password": dagster.EnvVar("POSTGRES_PASSWORD").get_value(),
        "schema-filters-type": "all",
        "ssl": False,
        "tunnel-enabled": False,
        "advanced-options": False,
      },
      "name": database_name,
      "engine": "postgres",
    }

    resp = await session.post(
      "/api/database", headers=headers_content_json, json=database_payload
    )
    resp.raise_for_status()
    return await resp.json()


@dagster.asset(group_name="metabase", kinds=["python", "metabase"], deps=["sets"])
async def card_sets(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card with a table showing the list of sets"""
  async with metabase.get_session() as session:
    card_name = "sets"
    card_payload = create_card_payload(
      card_name, database_pokemon["id"], "select * from sets"
    )
    return await create_card(
      context.log, session, card_name, card_payload, refresh_data=True
    )


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def card_usage_rate_one_deck_all_weeks(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card with a line graph showing the evolution of one deck use rate by week"""
  async with metabase.get_session() as session:
    card_payload = usage_rate_one_deck_all_weeks(database_pokemon["id"])
    return await create_card(context.log, session, card_payload["name"], card_payload)


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def card_details_one_deck_one_season(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card with statistics for one deck and one season"""
  async with metabase.get_session() as session:
    card_payload = details_one_deck_one_season(database_pokemon["id"])
    return await create_card(context.log, session, card_payload["name"], card_payload)


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def card_win_rate_against_deck_one_season(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card with a bar podium showing the most effective startegies against a specific deck"""
  async with metabase.get_session() as session:
    card_payload = win_rate_against_deck_one_season(database_pokemon["id"])
    return await create_card(context.log, session, card_payload["name"], card_payload)


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def card_usage_rate_one_deck_one_season_all_cards(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card row bar chart showing the usage rate of each card in a deck"""
  async with metabase.get_session() as session:
    card_payload = usage_rate_one_deck_one_season_all_cards(database_pokemon["id"])
    return await create_card(context.log, session, card_payload["name"], card_payload)


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def card_average_card_count_one_deck_one_season(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card row bar chart showing the usage rate of each card in a deck"""
  async with metabase.get_session() as session:
    card_payload = average_card_count_one_deck_one_season(database_pokemon["id"])
    return await create_card(context.log, session, card_payload["name"], card_payload)


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def card_usage_rate_one_deck_days_since_season_start(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card row bar chart showing the usage rate of each card in a deck"""
  async with metabase.get_session() as session:
    card_payload = usage_rate_one_deck_days_since_season_start(database_pokemon["id"])
    return await create_card(context.log, session, card_payload["name"], card_payload)


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def card_usage_rate_win_rate_one_season_top_decks(
  context: dagster.AssetExecutionContext, metabase: MetabaseResource, database_pokemon
):
  """Metabase card scatter plot showing the top decks by usage for one season, their use rate and win rate"""
  async with metabase.get_session() as session:
    card_payload = usage_rate_win_rate_one_season_top_decks(database_pokemon["id"])
    return await create_card(context.log, session, card_payload["name"], card_payload)


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def dashboard_deck_deep_dive(
  context: dagster.AssetExecutionContext,
  metabase: MetabaseResource,
  card_details_one_deck_one_season,
  card_win_rate_against_deck_one_season,
  card_usage_rate_one_deck_one_season_all_cards,
  card_usage_rate_one_deck_all_weeks,
  card_usage_rate_one_deck_days_since_season_start,
  card_average_card_count_one_deck_one_season,
):
  """Metabase dashboard showing detailed cards for one deck and one season"""
  async with metabase.get_session() as session:
    payload = get_payload(
      card_details_one_deck_one_season["id"],
      card_usage_rate_one_deck_one_season_all_cards["id"],
      card_usage_rate_one_deck_all_weeks["id"],
      card_win_rate_against_deck_one_season["id"],
      card_usage_rate_one_deck_days_since_season_start["id"],
      card_average_card_count_one_deck_one_season["id"],
    )

    json = await create_dashboard(context.log, session, "deck deep dive", payload)

    resp = await session.post(
      "/api/collection/root/move-dashboard-question-candidates",
      headers=headers_content_json,
      json={
        "card_ids": [
          card_details_one_deck_one_season["id"],
          card_usage_rate_one_deck_one_season_all_cards["id"],
          card_usage_rate_one_deck_all_weeks["id"],
          card_win_rate_against_deck_one_season["id"],
          card_usage_rate_one_deck_days_since_season_start["id"],
          card_average_card_count_one_deck_one_season["id"],
        ]
      },
    )
    resp.raise_for_status()

    return json


@dagster.asset(group_name="metabase", kinds=["python", "metabase"])
async def dashboard_season_overview(
  context: dagster.AssetExecutionContext,
  metabase: MetabaseResource,
  card_usage_rate_win_rate_one_season_top_decks,
  card_sets,
  dashboard_deck_deep_dive,
):
  """Metabase dashboard showing the top decks by usage for one season, their use rate and win rate"""
  async with metabase.get_session() as session:
    payload = get_payload_season_overview(
      card_usage_rate_win_rate_one_season_top_decks["id"],
      card_sets["id"],
      dashboard_deck_deep_dive,
    )

    json = await create_dashboard(context.log, session, "season_overview", payload)

    resp = await session.post(
      "/api/collection/root/move-dashboard-question-candidates",
      headers=headers_content_json,
      json={"card_ids": [card_usage_rate_win_rate_one_season_top_decks["id"]]},
    )
    resp.raise_for_status()

    return json
