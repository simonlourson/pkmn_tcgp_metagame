import uuid


def details_one_deck_one_season(database_pokemon_id):
  template_tag_id_deck = str(uuid.uuid4())
  template_tag_id_set = str(uuid.uuid4())
  query = """
    select 
      d.deck_name, 
      s.set_code || ' : ' || s.set_name as season,
      dw.win_rate,
      du.usage_rate
    from deck_win_rate dw
    inner join deck_usage du on dw.deck_id = du.deck_id and dw.set_code = du.set_code
    inner join decks d on dw.deck_id = d.deck_id
    inner join sets s on dw.set_code = s.set_code
    where d.deck_name = {{ deck_name }}
    and dw.set_code = {{ set_code }} 
  """

  return {
    "name": "details, one deck, one season",
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
    "display": "object",
    "description": None,
    "visualization_settings": {
      "column_settings": {
        '["name","deck_name"]': {"column_title": "Deck name"},
        '["name","season"]': {"column_title": "Season"},
        '["name","win_rate"]': {"column_title": "Win rate", "number_style": "percent"},
        '["name","usage_rate"]': {
          "column_title": "Usage rate",
          "number_style": "percent",
        },
      }
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
    "collection_id": None,
    "dashboard_id": None,
    "collection_position": None,
    "collection_preview": True,
    "result_metadata": None,
  }
