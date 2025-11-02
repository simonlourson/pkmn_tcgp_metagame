import uuid


def win_rate_against_deck_one_season(database_pokemon_id):
  template_tag_id_deck = str(uuid.uuid4())
  template_tag_id_set = str(uuid.uuid4())

  query = """
  with ranked_deck_against as (
    select
      dadwr.set_code,
      d.deck_name,
      dadwr.win_rate,
      rank() over (partition by dadwr.set_code order by dadwr.win_rate desc) as rank_win_rate
    from deck_against_deck_win_rate dadwr
    inner join decks d on dadwr.deck_id = d.deck_id
    inner join decks ad on dadwr.against_deck_id = ad.deck_id
    inner join deck_usage du on du.set_code = dadwr.set_code and dadwr.deck_id = du.deck_id
    where dadwr.deck_id <> dadwr.against_deck_id
    and dadwr.nb_wins + dadwr.nb_losses > 20
    and ad.deck_name = {{ deck_name }}
    and dadwr.set_code = {{ set_code }}
  )
  select 'Podium' as podium, * from ranked_deck_against
  where rank_win_rate <= 3 
  """

  return {
    "name": "win rate, against deck, one season",
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
            "default": None,
            "required": False,
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
    "display": "bar",
    "description": None,
    "visualization_settings": {
      "graph.show_goal": True,
      "graph.y_axis.title_text": "win rate",
      "graph.x_axis.labels_enabled": False,
      "graph.goal_label": "50% win rate",
      "graph.series_order_dimension": None,
      "graph.goal_value": 0.5,
      "graph.metrics": ["win_rate"],
      "graph.label_value_formatting": "auto",
      "graph.series_order": None,
      "column_settings": {'["name","win_rate"]': {"number_style": "percent"}},
      "graph.x_axis.scale": "ordinal",
      "graph.dimensions": ["podium", "deck_name"],
      "stackable.stack_type": None,
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
