# pkmn_tcgp_metagame

Metagame anamysis for Pokemon TCG Pocket, [Medium article here](https://medium.com/@louislourson/using-a-modern-data-stack-to-analyze-the-pokemon-tcg-pocket-metagame-6ed69c01c9d5).



## Setup

```bash
docker compose up -d
uv sync
dagster dev
```
## Interesting things about this repo
* [Metabase resource](https://github.com/simonlourson/pkmn_tcgp_metagame/blob/main/pkmn_tcgp_metagame/metabase/metabase_resource.py) to handle API connections
* [Custom Postgres IO manager](https://github.com/simonlourson/pkmn_tcgp_metagame/blob/main/pkmn_tcgp_metagame/postgres/postgres_io_manager.py) to store asset results
* [Infrastructure diagram](https://github.com/simonlourson/pkmn_tcgp_metagame/blob/main/infrastructure.md)