import pickle
from contextlib import contextmanager

import psycopg
from dagster import ConfigurableIOManagerFactory, InputContext, IOManager, OutputContext
from pydantic import Field

from pkmn_tcgp_metagame.postgres.helpers import execute_many, execute_sql_script


class InternalPostgresIOManager(IOManager):
  @contextmanager
  def get_connection(self):
    conn = psycopg.connect(
      f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    )
    conn.autocommit = True

    yield conn

    conn.close()

  def __init__(self, context, database, host, port, user, password):
    self.database = database
    self.host = host
    self.port = port
    self.user = user
    self.password = password

    try:
      execute_sql_script(
        context.log,
        self,
        """
        create table if not exists io_manager (
          path varchar primary key,
          data bytea not null
        )
      """,
      )
    except psycopg.errors.UniqueViolation:
      context.log.info("io_manager already exists")

  def handle_output(self, context: OutputContext, obj):
    execute_many(
      context.log,
      self,
      "insert into io_manager values () on conflict (path) do update set data=excluded.data",
      [(context.asset_key.path, pickle.dumps(obj))],
    )

  def load_input(self, context: InputContext):
    with self.get_connection() as conn:
      with conn.cursor() as cur:
        cur.execute(
          "select data from io_manager where path = %s", (context.asset_key.path,)
        )
        return pickle.loads(bytes(cur.fetchone()[0]))


class PostgresIOManager(ConfigurableIOManagerFactory):
  database: str = Field(description=("Name of the postgres database"))
  host: str = Field(description=("Host of the postgres server"))
  port: int = Field(description=("Port of the postgres server"))
  user: str = Field(description=("username to the postgres server"))
  password: str = Field(description=("password to the postgres server"))

  def create_io_manager(self, context) -> InternalPostgresIOManager:
    return InternalPostgresIOManager(
      context, self.database, self.host, self.port, self.user, self.password
    )
