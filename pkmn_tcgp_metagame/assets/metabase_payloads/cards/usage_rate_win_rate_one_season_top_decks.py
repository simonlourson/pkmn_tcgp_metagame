import uuid


def usage_rate_win_rate_one_season_top_decks(database_pokemon_id):
  template_tag_id_set = str(uuid.uuid4())

  query = """
    select
      du.set_code,
      d.deck_name,
      dw.nb_wins,
      dw.nb_losses,
      dw.win_rate,
      du.nb_decklists,
      du.usage_rate,
      du.usage_rank
    from deck_usage du
    inner join deck_win_rate dw on du.set_code = dw.set_code and du.deck_id = dw.deck_id
    inner join decks d on du.deck_id = d.deck_id
    where du.usage_rank <= 16
    and du.set_code = {{ set_code }}
    order by du.set_code
  """

  return {
    "name": "usage rate, win rate, one season, top decks",
    "cache_ttl": None,
    "type": "question",
    "dataset_query": {
      "database": database_pokemon_id,
      "type": "native",
      "native": {
        "template-tags": {
          "set_code": {
            "type": "text",
            "name": "set_code",
            "id": template_tag_id_set,
            "display-name": "Set Code",
          }
        },
        "query": query,
      },
    },
    "display": "scatter",
    "description": None,
    "visualization_settings": {
      "graph.show_goal": True,
      "graph.y_axis.title_text": "win rate",
      "graph.goal_label": "50% win rate",
      "graph.series_order_dimension": None,
      "graph.x_axis.title_text": "usage rate",
      "graph.goal_value": 0.5,
      "graph.metrics": ["win_rate"],
      "graph.series_order": None,
      "column_settings": {
        '["name","win_rate"]': {"number_style": "percent"},
        '["name","usage_rate"]': {"number_style": "percent"},
      },
      "graph.x_axis.scale": "linear",
      "graph.dimensions": ["usage_rate", "deck_name"],
    },
    "parameters": [
      {
        "id": template_tag_id_set,
        "type": "category",
        "target": ["variable", ["template-tag", "set_code"]],
        "name": "Set Code",
        "slug": "set_code",
      }
    ],
    "parameter_mappings": [],
    "archived": False,
    "enable_embedding": False,
    "embedding_params": None,
    "collection_position": None,
    "collection_preview": True,
    "collection_id": None,
  }
