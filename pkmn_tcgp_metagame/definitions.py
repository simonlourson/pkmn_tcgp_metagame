from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource

from pkmn_tcgp_metagame.assets import extract, load, metabase, transform
from pkmn_tcgp_metagame.metabase.metabase_resource import MetabaseResource
from pkmn_tcgp_metagame.postgres.postgres_io_manager import PostgresIOManager
from pkmn_tcgp_metagame.postgres.postgres_resource import PostgresResource
from pkmn_tcgp_metagame.project import dbt_project

defs = Definitions(
  assets=load_assets_from_modules([extract, load, transform, metabase]),
  resources={
    "dbt": DbtCliResource(
      project_dir=dbt_project,
    ),
    "database": PostgresResource(
      host=EnvVar("POSTGRES_HOST").get_value(),
      port=EnvVar("POSTGRES_PORT").get_value(),
      database=EnvVar("POSTGRES_DB").get_value(),
      user=EnvVar("POSTGRES_USER").get_value(),
      password=EnvVar("POSTGRES_PASSWORD").get_value(),
    ),
    "metabase": MetabaseResource(
      host=f"http://{EnvVar('METABASE_HOST').get_value()}:{EnvVar('METABASE_PORT').get_value()}",
      user=EnvVar("METABASE_ADMIN_USER").get_value(),
      password=EnvVar("METABASE_ADMIN_PASSWORD").get_value(),
    ),
    "io_manager": PostgresIOManager(
      database=EnvVar("DAGSTER_POSTGRES_DB").get_value(),
      host=EnvVar("POSTGRES_HOST").get_value(),
      port=EnvVar("POSTGRES_PORT").get_value(),
      user=EnvVar("DAGSTER_POSTGRES_USER").get_value(),
      password=EnvVar("DAGSTER_POSTGRES_PASSWORD").get_value(),
    ),
  },
)
