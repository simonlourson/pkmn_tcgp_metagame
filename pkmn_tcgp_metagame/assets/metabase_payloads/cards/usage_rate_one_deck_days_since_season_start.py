import uuid


def usage_rate_one_deck_days_since_season_start(database_pokemon_id):
  template_tag_id_deck = str(uuid.uuid4())
  template_tag_id_set = str(uuid.uuid4())
  query = """
    with denominator as (
      select set_code, day_in_season, nb_decklists
      from nb_decklists_per_season_per_day
    ), usage_rate as (
      select t.set_code, t.day_in_season, dk.deck_name, count(distinct da.decklist_id)::float / d.nb_decklists as usage_rate
      from decklists_aggregated da
      inner join decks dk on da.deck_id = dk.deck_id
      inner join tournaments t on da.tournament_id = t.tournament_id
      inner join denominator d on t.set_code = d.set_code and t.day_in_season = d.day_in_season
      where dk.deck_name = {{ deck_name }}
      and t.set_code = {{ set_code }}
      group by t.set_code, t.day_in_season, dk.deck_name, d.nb_decklists
    )
    select ur.set_code, ur.day_in_season, ur.deck_name, coalesce(ur.usage_rate, 0) as usage_rate
    from usage_rate ur
    order by ur.set_code, ur.day_in_season
  """

  return {
    "name": "usage rate, one deck, days since season start",
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
            "id": template_tag_id_deck,
            "display-name": "Deck Name",
          },
          "set_code": {
            "type": "text",
            "name": "set_code",
            "id": template_tag_id_set,
            "display-name": "Set Code",
          },
        },
        "query": query,
      },
    },
    "display": "area",
    "description": None,
    "visualization_settings": {
      "graph.dimensions": ["day_in_season"],
      "graph.series_order_dimension": None,
      "graph.series_order": None,
      "graph.y_axis.title_text": "usage rate",
      "graph.x_axis.scale": "linear",
      "column_settings": {'["name","usage_rate"]': {"number_style": "percent"}},
      "graph.metrics": ["usage_rate"],
      "graph.x_axis.title_text": "days since season start",
    },
    "parameters": [
      {
        "id": template_tag_id_deck,
        "type": "category",
        "target": ["variable", ["template-tag", "deck_name"]],
        "name": "Deck Name",
        "slug": "deck_name",
        "values_query_type": "none",
      },
      {
        "id": template_tag_id_set,
        "type": "category",
        "target": ["variable", ["template-tag", "set_code"]],
        "name": "Set Code",
        "slug": "set_code",
      },
    ],
    "parameter_mappings": [],
    "archived": False,
    "enable_embedding": False,
    "embedding_params": None,
    "collection_position": None,
    "collection_preview": True,
    "collection_id": None,
  }
