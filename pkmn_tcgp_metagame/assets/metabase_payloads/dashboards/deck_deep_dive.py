import uuid


def get_payload(
  card_stats_id,
  card_deck_composition_id,
  card_usage_rate_id,
  card_best_decks_against_id,
  card_usage_rate_days_id,
  card_deck_composition_by_type_id,
):
  parameter_id_deck_name = str(uuid.uuid4()).split("-")[0]
  parameter_id_set_code = str(uuid.uuid4()).split("-")[0]

  parameters = [
    {
      "name": "Deck Name",
      "slug": "deck_name",
      "id": parameter_id_deck_name,
      "type": "string/=",
      "sectionId": "string",
      "values_query_type": "none",
      "isMultiSelect": False,
    },
    {
      "name": "Season",
      "slug": "season",
      "id": parameter_id_set_code,
      "type": "string/=",
      "sectionId": "string",
      "values_query_type": "none",
      "isMultiSelect": False,
    },
  ]

  dashcards = [
    {
      "id": -1,
      "card_id": card_stats_id,
      "dashboard_tab_id": None,
      "action_id": None,
      "row": 0,
      "col": 0,
      "size_x": 16,
      "size_y": 7,
      "series": [],
      "visualization_settings": {"card.title": "statistics for this deck"},
      "parameter_mappings": [
        {
          "parameter_id": parameter_id_deck_name,
          "card_id": card_stats_id,
          "target": ["variable", ["template-tag", "deck_name"]],
        },
        {
          "parameter_id": parameter_id_set_code,
          "card_id": card_stats_id,
          "target": ["variable", ["template-tag", "set_code"]],
        },
      ],
    },
    {
      "id": -2,
      "card_id": card_deck_composition_id,
      "dashboard_tab_id": None,
      "action_id": None,
      "row": 9,
      "col": 16,
      "size_x": 8,
      "size_y": 7,
      "series": [],
      "visualization_settings": {
        "click_behavior": {
          "type": "link",
          "linkType": "url",
          "linkTemplate": "{{card_url}}",
        },
        "card.title": "card makeup for this deck ",
      },
      "parameter_mappings": [
        {
          "parameter_id": parameter_id_deck_name,
          "card_id": card_deck_composition_id,
          "target": ["variable", ["template-tag", "deck_name"]],
        },
        {
          "parameter_id": parameter_id_set_code,
          "card_id": card_deck_composition_id,
          "target": ["variable", ["template-tag", "set_code"]],
        },
      ],
    },
    {
      "id": -3,
      "card_id": card_usage_rate_id,
      "dashboard_tab_id": None,
      "action_id": None,
      "row": 7,
      "col": 0,
      "size_x": 16,
      "size_y": 7,
      "series": [],
      "visualization_settings": {
        "click_behavior": {
          "type": "crossfilter",
          "parameterMapping": {
            parameter_id_set_code: {
              "source": {"type": "column", "id": "set_code", "name": "set_code"},
              "target": {"type": "parameter", "id": parameter_id_set_code},
              "id": parameter_id_set_code,
            }
          },
        },
        "card.title": "usage rate for this deck across all weeks",
      },
      "parameter_mappings": [
        {
          "parameter_id": parameter_id_deck_name,
          "card_id": card_usage_rate_id,
          "target": ["variable", ["template-tag", "deck_name"]],
        }
      ],
    },
    {
      "id": -4,
      "card_id": card_best_decks_against_id,
      "dashboard_tab_id": None,
      "action_id": None,
      "row": 0,
      "col": 16,
      "size_x": 8,
      "size_y": 7,
      "series": [],
      "visualization_settings": {
        "click_behavior": {
          "type": "crossfilter",
          "parameterMapping": {
            parameter_id_deck_name: {
              "source": {"type": "column", "id": "deck_name", "name": "deck_name"},
              "target": {"type": "parameter", "id": parameter_id_deck_name},
              "id": parameter_id_deck_name,
            }
          },
        },
        "card.title": "best decks against this deck",
      },
      "parameter_mappings": [
        {
          "parameter_id": parameter_id_deck_name,
          "card_id": card_best_decks_against_id,
          "target": ["variable", ["template-tag", "deck_name"]],
        },
        {
          "parameter_id": parameter_id_set_code,
          "card_id": card_best_decks_against_id,
          "target": ["variable", ["template-tag", "set_code"]],
        },
      ],
    },
    {
      "id": -5,
      "card_id": card_usage_rate_days_id,
      "dashboard_tab_id": None,
      "action_id": None,
      "row": 14,
      "col": 0,
      "size_x": 16,
      "size_y": 7,
      "series": [],
      "visualization_settings": {
        "card.title": "usage rate by days since season start",
      },
      "parameter_mappings": [
        {
          "parameter_id": parameter_id_deck_name,
          "card_id": card_usage_rate_days_id,
          "target": ["variable", ["template-tag", "deck_name"]],
        },
        {
          "parameter_id": parameter_id_set_code,
          "card_id": card_usage_rate_days_id,
          "target": ["variable", ["template-tag", "set_code"]],
        },
      ],
    },
    {
      "id": -6,
      "card_id": card_deck_composition_by_type_id,
      "dashboard_tab_id": None,
      "action_id": None,
      "row": 14,
      "col": 16,
      "size_x": 8,
      "size_y": 7,
      "series": [],
      "visualization_settings": {
        "card.title": "deck composition by card type",
      },
      "parameter_mappings": [
        {
          "parameter_id": parameter_id_set_code,
          "card_id": card_deck_composition_by_type_id,
          "target": ["variable", ["template-tag", "set_code"]],
        },
        {
          "parameter_id": parameter_id_deck_name,
          "card_id": card_deck_composition_by_type_id,
          "target": ["variable", ["template-tag", "deck_name"]],
        },
      ],
    },
  ]

  return {"parameters": parameters, "dashcards": dashcards}
