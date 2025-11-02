import uuid


def average_card_count_one_deck_one_season(database_pokemon_id):
  template_tag_id_deck = str(uuid.uuid4())
  template_tag_id_set = str(uuid.uuid4())
  query = """
    select
      c.card_type,
      c.card_subtype,
      sum(cidu.average_count) as average_count
    from card_in_deck_usage cidu
    inner join decks d on cidu.deck_id = d.deck_id
    inner join cards c on cidu.card_url = c.card_url
    where d.deck_name = {{ deck_name }}
    and cidu.set_code = {{ set_code }}
    group by c.card_type, c.card_subtype
  """

  return {
    "name": "average card count, one deck, one season",
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
    "display": "pie",
    "description": None,
    "visualization_settings": {
      "graph.dimensions": ["set_code", "card_name_with_set"],
      "graph.series_order_dimension": None,
      "graph.series_order": None,
      "graph.x_axis.title_text": "Season",
      "graph.x_axis.scale": "ordinal",
      "column_settings": {'["name","usage_rate"]': {"number_style": "percent"}},
      "graph.metrics": ["usage_rate"],
      "pie.dimension": ["card_type", "card_subtype"],
      "pie.percent_visibility": "legend",
      "pie.show_legend": False,
      "pie.show_total": True,
      "pie.show_labels": True,
    },
    "parameters": [
      {
        "id": template_tag_id_deck,
        "type": "category",
        "target": ["variable", ["template-tag", "deck_name"]],
        "name": "Deck Name",
        "slug": "deck_name",
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
