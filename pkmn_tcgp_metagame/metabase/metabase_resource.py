from contextlib import asynccontextmanager

import aiohttp
from dagster import ConfigurableResource
from pydantic import Field


class MetabaseResource(ConfigurableResource):
  """Resource for interacting with a Metabase instance."""

  host: str = Field(description=("Host of the metabase instance"))
  user: str = Field(description=("username to the metabase instance"))
  password: str = Field(description=("password to the metabase instance"))

  @classmethod
  def _is_dagster_maintained(cls) -> bool:
    return True

  @asynccontextmanager
  async def get_session(self):
    session = aiohttp.ClientSession(
      base_url=self.host, headers={"accept": "application/json"}
    )
    session_data = {"username": self.user, "password": self.password, "remember": False}

    resp = await session.post(
      "/api/session", headers={"content-type": "application/json"}, json=session_data
    )
    resp.raise_for_status()

    json = await resp.json()
    session.headers["cookie"] = f"metabase.SESSION={json['id']}"

    yield session

    session.close()
