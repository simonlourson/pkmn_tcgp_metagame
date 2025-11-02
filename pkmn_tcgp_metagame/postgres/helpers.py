from dagster import DagsterLogManager


def execute_sql_script(log: DagsterLogManager, database, query: str, params=None):
  log.info(query)
  with database.get_connection() as conn:
    with conn.cursor() as cur:
      if params is None:
        cur.execute(query)
      else:
        cur.execute(query, params)


def execute_many(log: DagsterLogManager, database, sql: str, data):
  log.info(sql)
  parameters = "(" + ",".join(["%s" for d in data[0]]) + ")"
  with database.get_connection() as conn:
    with conn.cursor() as cur:
      cur.executemany(sql.replace("()", parameters), data)
