# Infrastructure Architecture

This diagram shows the infrastructure setup for the Pokemon TCG Pocket Metagame project based on the `compose.yaml` configuration.

```mermaid
graph TB
    subgraph External["External Access"]
        User[("User<br/>Browser")]
        DBClient[("Database<br/>Client")]
    end

    subgraph LocalServices["Local Services"]
        Dagster[Dagster Dev<br/>Port: 3000]
    end

    subgraph DockerNetwork["Docker Network: pokenet (bridge)"]
        subgraph Metabase["metabase container"]
            MB[Metabase<br/>v0.55.2.1<br/>Port: 3001]
        end

        subgraph Postgres["postgres-pokemon container"]
            PG[(PostgreSQL<br/>latest<br/>Port: 5432)]
            subgraph Databases["Databases"]
                MainDB[("Main DB<br/>POSTGRES_DB")]
                MetabaseDB[("Metabase DB<br/>METABASE_POSTGRES_DB")]
                DagsterDB[("Dagster DB<br/>DAGSTER_POSTGRES_DB")]
            end
        end
    end

    User -->|"Port 3000"| Dagster
    User -->|"Port 3001"| MB
    DBClient -->|"5432:5432"| PG
    Dagster -->|"Database Connection"| PG
    MB -->|"MB_DB Connection<br/>Port 5432"| PG
    PG --> MainDB
    PG --> MetabaseDB
    PG --> DagsterDB

    style Dagster fill:#ffd54f
    style LocalServices fill:#fff9c4
    style Metabase fill:#e8f5e9
    style Postgres fill:#e3f2fd
    style Databases fill:#f3e5f5
    style DockerNetwork fill:#fff3e0
    style External fill:#fce4ec
```

## Components

### Dagster Dev
- **Port**: `3000`
- **Purpose**: Data orchestration platform for managing data pipelines and workflows
- **Deployment**: Runs locally via `dagster dev` command
- **Database**: Stores run history and metadata in PostgreSQL (`DAGSTER_POSTGRES_DB`)
- **Access**: Dagster UI available at `http://localhost:3000`

### Metabase
- **Image**: `metabase/metabase:v0.55.2.1`
- **Container**: `metabase`
- **Port**: `3001`
- **Purpose**: Analytics and business intelligence platform
- **Database**: Stores metadata in PostgreSQL (`METABASE_POSTGRES_DB`)
- **Access**: Metabase UI available at `http://localhost:3001`

### PostgreSQL
- **Image**: `postgres:latest`
- **Container**: `postgres-pokemon`
- **Port**: `5432:5432`
- **Purpose**: Primary database server hosting multiple databases
- **Initialization**: Scripts in `./initdb` directory run on first startup
- **Databases**:
  - Main application database (`POSTGRES_DB`)
  - Metabase metadata database (`METABASE_POSTGRES_DB`)
  - Dagster storage database (`DAGSTER_POSTGRES_DB`)

### Network
- **Name**: `pokenet`
- **Driver**: `bridge`
- **Purpose**: Internal communication between containers

## Notes
- All environment variables are configured via `.env` file
- PostgreSQL initialization scripts can be added to `./initdb` directory
- Dagster runs locally via `dagster dev` and connects to the PostgreSQL container
- Both Dagster (port 3000) and Metabase (port 3001) provide web interfaces for users
