import uuid


def usage_rate_one_deck_all_weeks(database_pokemon_id):
  template_tag_id = str(uuid.uuid4())
  query = """
    with all_weeks as (
      select distinct set_code, week_in_season, 'seaon ' || set_code || ' week ' || week_in_season as week
      from tournaments
      order by set_code, week_in_season
    ), denominator as (
      select set_code, week_in_season, nb_decklists
      from nb_decklists_per_season_per_week
    ), usage_rate as (
      select t.set_code, t.week_in_season, dk.deck_name, count(distinct da.decklist_id)::float / d.nb_decklists as usage_rate
      from decklists_aggregated da
      inner join decks dk on da.deck_id = dk.deck_id
      inner join tournaments t on da.tournament_id = t.tournament_id
      inner join denominator d on t.set_code = d.set_code and t.week_in_season = d.week_in_season
      where dk.deck_name = {{ deck_name }}
      group by t.set_code, t.week_in_season, dk.deck_name, d.nb_decklists
    )
    select aw.week, aw.set_code, ur.deck_name, coalesce(ur.usage_rate, 0) as usage_rate
    from all_weeks aw
    left outer join usage_rate ur on aw.set_code = ur.set_code and aw.week_in_season = ur.week_in_season
    order by aw.set_code, aw.week_in_season
  """

  return {
    "name": "usage rate, one deck, all weeks",
    "cache_ttl": None,
    "type": "question",
    "dataset_query": {
      "database": database_pokemon_id,
      "type": "native",
      "native": {
        "template-tags": {
          "deck_name": {
            "type": "text",
            "name": "deck_name",
            "id": template_tag_id,
            "display-name": "Deck Name",
          }
        },
        "query": query,
      },
    },
    "display": "line",
    "description": None,
    "visualization_settings": {
      "graph.dimensions": ["week", "set_code"],
      "graph.series_order_dimension": None,
      "graph.series_order": None,
      "graph.y_axis.title_text": "usage rate",
      "graph.x_axis.scale": "ordinal",
      "column_settings": {'["name","usage_rate"]': {"number_style": "percent"}},
      "graph.metrics": ["usage_rate"],
    },
    "parameters": [
      {
        "id": template_tag_id,
        "type": "category",
        "target": ["variable", ["template-tag", "deck_name"]],
        "name": "Deck Name",
        "slug": "deck_name",
        "values_query_type": "none",
      }
    ],
    "parameter_mappings": [],
    "archived": False,
    "enable_embedding": False,
    "embedding_params": None,
    "collection_id": None,
    "dashboard_id": None,
    "collection_position": None,
    "collection_preview": True,
  }
