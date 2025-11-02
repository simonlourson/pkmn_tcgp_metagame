import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets

from pkmn_tcgp_metagame.project import dbt_project


@dbt_assets(
  manifest=dbt_project.manifest_path,
)
def dbt_build(context: dg.AssetExecutionContext, dbt: DbtCliResource):
  yield from dbt.cli(["build"], context=context).stream()
