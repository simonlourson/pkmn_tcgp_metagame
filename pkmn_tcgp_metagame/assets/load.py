import csv
import json
import os
from datetime import datetime

import dagster

from pkmn_tcgp_metagame.assets import constants
from pkmn_tcgp_metagame.postgres.helpers import execute_many, execute_sql_script
from pkmn_tcgp_metagame.postgres.postgres_resource import PostgresResource


@dagster.multi_asset(
  can_subset=True,
  specs=[
    dagster.AssetSpec(
      key="raw_cards",
      deps=["set_files"],
      kinds=["python", "postgres"],
      group_name="load",
      description="Table raw.cards created and loaded with data",
    ),
    dagster.AssetSpec(
      key="raw_sets",
      deps=["set_files"],
      kinds=["python", "postgres"],
      group_name="load",
      description="Table raw.sets created and loaded with data",
    ),
    dagster.AssetSpec(
      key="raw_evolutions",
      deps=["set_files"],
      kinds=["python", "postgres"],
      group_name="load",
      description="Table raw.evolutions created and loaded with data",
    ),
  ],
)
def load_set_files(context: dagster.AssetExecutionContext, database: PostgresResource):
  if "raw_cards" in context.selected_output_names:
    query_create_raw_cards = """
      drop table if exists raw.cards;
      create table raw.cards (
        card_url varchar null,
        set_code varchar null,
        card_number int null,
        card_name varchar null,
        card_type varchar null,
        card_subtype varchar null,
        card_stage varchar null,
        is_promo boolean null
      );
    """
    execute_sql_script(context.log, database, query_create_raw_cards)

  if "raw_sets" in context.selected_output_names:
    query_create_raw_sets = """
      drop table if exists raw.sets;
      create table raw.sets (
        set_code varchar null,
        set_name varchar null,
        set_release_date timestamp null
      );
    """
    execute_sql_script(context.log, database, query_create_raw_sets)

  if "raw_evolutions" in context.selected_output_names:
    query_create_raw_evolutions = """
      drop table if exists raw.evolutions;
      create table raw.evolutions (
        previous_stage_url varchar null,
        next_stage_url varchar null
      );
    """
    execute_sql_script(context.log, database, query_create_raw_evolutions)

  set_data = []
  card_data = []
  evolution_data = []

  for file in [
    f"{constants.SETS_OUTPUT_DIR}/{file}"
    for file in os.listdir(constants.SETS_OUTPUT_DIR)
  ]:
    with open(file) as f:
      set = json.load(f)
      set_code = set["code"]

      set_data.append(
        (
          set_code,
          set["name"],
          datetime.strptime(set["release_date"], "%d %b %y")
          if set["release_date"]
          else None,
        )
      )

      for card in set["cards"]:
        card_data.append(
          (
            card["url"],
            set_code,
            card["number"],
            card["name"],
            card["type"],
            card["subtype"],
            card["stage"],
            card["is_promo"],
          )
        )

        previous_stages = card["evolves_from"]
        if previous_stages is not None:
          for previous_stage_url in previous_stages:
            evolution_data.append((previous_stage_url, card["url"]))

  if "raw_cards" in context.selected_output_names:
    execute_many(context.log, database, "INSERT INTO raw.cards values ()", card_data)
    yield dagster.MaterializeResult(
      asset_key="raw_cards",
      metadata={"Number of lines": dagster.MetadataValue.int(len(card_data))},
    )

  if "raw_sets" in context.selected_output_names:
    execute_many(context.log, database, "INSERT INTO raw.sets values ()", set_data)
    yield dagster.MaterializeResult(
      asset_key="raw_sets",
      metadata={"Number of lines": dagster.MetadataValue.int(len(set_data))},
    )

  if "raw_evolutions" in context.selected_output_names:
    execute_many(
      context.log, database, "INSERT INTO raw.evolutions values ()", evolution_data
    )
    yield dagster.MaterializeResult(
      asset_key="raw_evolutions",
      metadata={"Number of lines": dagster.MetadataValue.int(len(evolution_data))},
    )


@dagster.multi_asset(
  can_subset=True,
  specs=[
    dagster.AssetSpec(
      key="raw_tournaments",
      deps=["tournament_files"],
      kinds=["python", "postgres"],
      group_name="load",
      description="Table raw.tournaments created and loaded with data",
    ),
    dagster.AssetSpec(
      key="raw_decklists",
      deps=["tournament_files"],
      kinds=["python", "postgres"],
      group_name="load",
      description="Table raw.decklists created and loaded with data",
    ),
    dagster.AssetSpec(
      key="raw_matches",
      deps=["tournament_files"],
      kinds=["python", "postgres"],
      group_name="load",
      description="Table raw.matches created and loaded with data",
    ),
  ],
)
def load_tournaments_files(
  context: dagster.AssetExecutionContext, database: PostgresResource
):
  if "raw_tournaments" in context.selected_output_names:
    query_create_raw_tournaments = """
      drop table if exists raw.tournaments;
      create table raw.tournaments (
        tournament_id varchar null,
        tournament_name varchar null,
        tournament_organizer varchar null,
        tournament_date timestamp NULL
      );
    """
    execute_sql_script(context.log, database, query_create_raw_tournaments)

  if "raw_decklists" in context.selected_output_names:
    query_create_raw_tournaments = """
      drop table if exists raw.decklists;
      create table raw.decklists (
        tournament_id varchar null,
        player_id varchar null,
        card_url varchar null,
        decklist_count int null
      );
    """
    execute_sql_script(context.log, database, query_create_raw_tournaments)

  if "raw_matches" in context.selected_output_names:
    query_create_raw_matches = """
      drop table if exists raw.matches;
      create table raw.matches (
        tournament_id varchar null,
        winner_player_id varchar null,
        loser_player_id varchar null
      );
    """
    execute_sql_script(context.log, database, query_create_raw_matches)

  decklist_data = []
  tournament_data = []
  match_data = []

  for file in [
    f"{constants.TOURNAMENTS_OUTPUT_DIR}/{file}"
    for file in os.listdir(constants.TOURNAMENTS_OUTPUT_DIR)
  ]:
    with open(file) as f:
      tournament = json.load(f)
      tournament_data.append(
        (
          tournament["id"],
          tournament["name"],
          tournament["organizer"],
          datetime.strptime(tournament["date"], "%Y-%m-%dT%H:%M:%S.000Z"),
        )
      )

      if "raw_decklists" in context.selected_output_names:
        for player in tournament["players"]:
          player_id = player["id"]
          for card in player["decklist"]:
            decklist_data.append(
              (tournament["id"], player_id, card["url"], int(card["count"]))
            )

      if "raw_matches" in context.selected_output_names:
        for match in tournament["matches"]:
          match_results = match["match_results"]

          # Only insert the match if it's not a draw
          if match_results[0]["score"] == match_results[1]["score"]:
            continue

          sorted_match_results = sorted(match_results, key=lambda x: x["score"])
          match_data.append(
            (
              tournament["id"],
              sorted_match_results[1]["player_id"],
              sorted_match_results[0]["player_id"],
            )
          )

  if "raw_tournaments" in context.selected_output_names:
    execute_many(
      context.log, database, "insert into raw.tournaments values ()", tournament_data
    )
    yield dagster.MaterializeResult(
      asset_key="raw_tournaments",
      metadata={"Number of lines": dagster.MetadataValue.int(len(tournament_data))},
    )

  if "raw_decklists" in context.selected_output_names:
    execute_many(
      context.log, database, "insert into raw.decklists values ()", decklist_data
    )
    yield dagster.MaterializeResult(
      asset_key="raw_decklists",
      metadata={"Number of lines": dagster.MetadataValue.int(len(decklist_data))},
    )

  if "raw_matches" in context.selected_output_names:
    execute_many(context.log, database, "insert into raw.matches values ()", match_data)
    yield dagster.MaterializeResult(
      asset_key="raw_matches",
      metadata={"Number of lines": dagster.MetadataValue.int(len(match_data))},
    )


@dagster.asset(
  group_name="load",
  deps=["translation_files"],
  kinds=["python", "postgres"],
)
async def raw_translations(
  context: dagster.AssetExecutionContext, database: PostgresResource
) -> dagster.MaterializeResult:
  """Table raw.translations created and loaded with data"""
  translation_input_file = f"{constants.JSON_OUTPUT}/translations/fr.csv"

  translation_data = []

  with open(translation_input_file) as f:
    reader = csv.reader(f)
    for row in reader:
      translation_data.append(tuple(row))

  query_create_raw_translations = """
    drop table if exists raw.translations;
    create table raw.translations (
      set_code varchar null,
      card_number int null,
      card_name varchar null
    );
  """
  execute_sql_script(context.log, database, query_create_raw_translations)

  execute_many(
    context.log, database, "insert into raw.translations values ()", translation_data
  )
  return dagster.MaterializeResult(
    metadata={
      "Number of lines": dagster.MetadataValue.int(len(translation_data)),
    }
  )
