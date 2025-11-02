from pathlib import Path

from dagster_dbt import DbtProject

dbt_project = DbtProject(
  project_dir=Path(__file__).joinpath("../..", "pkmn_tcgp_metagame_sql").resolve()
)
