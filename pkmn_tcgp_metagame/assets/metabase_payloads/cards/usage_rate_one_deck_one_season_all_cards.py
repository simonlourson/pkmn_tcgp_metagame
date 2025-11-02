import uuid


def usage_rate_one_deck_one_season_all_cards(database_pokemon_id):
  template_tag_id_deck = str(uuid.uuid4())
  template_tag_id_set = str(uuid.uuid4())

  query = """
    select
      cidu.set_code,
      d.deck_name,
      c.card_name_with_set,
      'https://pocket.limitlesstcg.com' || c.card_url as card_url,
      cidu.usage_rate
    from card_in_deck_usage cidu
    inner join decks d on cidu.deck_id = d.deck_id
    inner join cards c on cidu.card_url = c.card_url
    where d.deck_name = {{ deck_name }}
    and cidu.set_code = {{ set_code }}
    and cidu.usage_rate > 0.1
    order by cidu.set_code, cidu.usage_rate desc 
  """

  return {
    "name": "usage rate, one deck, one season, all cards",
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
    "display": "row",
    "description": None,
    "visualization_settings": {
      "graph.dimensions": ["set_code", "card_name_with_set"],
      "graph.series_order_dimension": None,
      "graph.series_order": None,
      "graph.x_axis.title_text": "Season",
      "graph.x_axis.scale": "ordinal",
      "column_settings": {'["name","usage_rate"]': {"number_style": "percent"}},
      "graph.metrics": ["usage_rate"],
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
    "result_metadata": None,
    "collection_id": None,
  }
