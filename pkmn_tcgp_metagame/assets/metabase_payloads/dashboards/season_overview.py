import uuid


def get_payload_season_overview(
  card_win_rate_usage_rate_id, card_sets_id, dashboard_deck_deep_dive
):
  parameter_id_set_code = str(uuid.uuid4()).split("-")[0]

  parameters = [
    {
      "slug": "season",
      "values_query_type": "list",
      "default": "A1",
      "name": "Season",
      "type": "string/=",
      "sectionId": "string",
      "values_source_type": "card",
      "id": parameter_id_set_code,
      "values_source_config": {
        "card_id": card_sets_id,
        "value_field": ["field", "set_code", {"base-type": "type/Text"}],
      },
      "required": True,
    }
  ]

  parameter_id_deep_dive_deck_name = -1
  parameter_id_deep_dive_set_code = -1

  for parameter in dashboard_deck_deep_dive["parameters"]:
    if parameter["slug"] == "deck_name":
      parameter_id_deep_dive_deck_name = parameter["id"]
    if parameter["slug"] == "season":
      parameter_id_deep_dive_set_code = parameter["id"]

  dashcards = [
    {
      "id": -1,
      "card_id": card_win_rate_usage_rate_id,
      "dashboard_tab_id": None,
      "action_id": None,
      "row": 0,
      "col": 0,
      "size_x": 24,
      "size_y": 10,
      "series": [],
      "visualization_settings": {
        "click_behavior": {
          "type": "link",
          "linkType": "dashboard",
          "targetId": dashboard_deck_deep_dive["id"],
          "parameterMapping": {
            parameter_id_deep_dive_deck_name: {
              "source": {"type": "column", "id": "deck_name", "name": "deck_name"},
              "target": {"type": "parameter", "id": parameter_id_deep_dive_deck_name},
              "id": parameter_id_deep_dive_deck_name,
            },
            parameter_id_deep_dive_set_code: {
              "source": {"type": "column", "id": "set_code", "name": "set_code"},
              "target": {"type": "parameter", "id": parameter_id_deep_dive_set_code},
              "id": parameter_id_deep_dive_set_code,
            },
          },
        },
        "card.title": "usage rate and win rate for the top 16 most used decks",
      },
      "parameter_mappings": [
        {
          "parameter_id": parameter_id_set_code,
          "card_id": card_win_rate_usage_rate_id,
          "target": ["variable", ["template-tag", "set_code"]],
        }
      ],
    }
  ]

  return {"parameters": parameters, "dashcards": dashcards}
