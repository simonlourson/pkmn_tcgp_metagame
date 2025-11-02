from contextlib import contextmanager

import psycopg
from dagster import ConfigurableResource
from pydantic import Field


class PostgresResource(ConfigurableResource):
  """Resource for interacting with a Postgres database."""

  database: str = Field(description=("Name of the postgres database"))
  host: str = Field(description=("Host of the postgres server"))
  port: int = Field(description=("Port of the postgres server"))
  user: str = Field(description=("username to the postgres server"))
  password: str = Field(description=("password to the postgres server"))

  @classmethod
  def _is_dagster_maintained(cls) -> bool:
    return True

  @contextmanager
  def get_connection(self):
    conn = psycopg.connect(
      f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    )
    conn.autocommit = True

    yield conn

    conn.close()
