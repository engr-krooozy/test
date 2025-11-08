# Data Pipeline Project

This project is a simple data pipeline that moves data from MongoDB and Postgres into a Snowflake data warehouse. It demonstrates how to handle change data capture (CDC), build a basic transformation layer with dbt, and set up a small CI/CD workflow.

The pipeline is designed to be idempotent, meaning it can be run multiple times without creating duplicate data. It uses a high-watermark strategy to fetch only new and updated records from the source databases, and then merges them into Snowflake.

## Project Structure

```
.
├── .github/workflows/ci.yml   # CI/CD workflow for dbt tests
├── data_pipeline/
│   ├── dbt/                      # dbt project for data transformation
│   │   ├── models/
│   │   │   ├── staging/          # Staging models
│   │   │   └── marts/            # Dimension and fact tables
│   │   ├── dbt_project.yml       # dbt project configuration
│   │   └── profiles.yml          # dbt connection profiles
│   └── src/                      # Python scripts for data extraction
│       ├── mongo_extractor.py
│       ├── postgres_extractor.py
│       └── main.py
├── .env.example                # Example environment file for credentials
├── docker-compose.yml          # Docker Compose file for local services
├── Dockerfile                  # Dockerfile for the pipeline container
└── requirements.txt            # Python dependencies
```

## Prerequisites

*   Docker and Docker Compose
*   A Snowflake account with a user and warehouse set up
*   Python 3.9

## Setup Instructions

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2.  **Create a `.env` file:**
    Copy the `.env.example` file to a new file named `.env`:
    ```bash
    cp .env.example .env
    ```
    Fill in your Snowflake, MongoDB, and Postgres credentials in the `.env` file.

3.  **Build and start the services:**
    ```bash
    docker-compose up --build -d
    ```
    This will start the MongoDB, Postgres, and pipeline containers.

4.  **Seed the databases:**
    Run the following commands to populate the databases with sample data.
    ```bash
    docker-compose exec pipeline python scripts/seed_mongo.py
    docker-compose exec pipeline python scripts/seed_postgres.py
    ```

## Running the Pipeline

To run the entire data extraction and loading pipeline, execute the `main.py` script inside the `pipeline` container:

```bash
docker-compose exec pipeline python data_pipeline/src/main.py
```

## dbt Project

The dbt project is located in the `data_pipeline/dbt` directory. To run the dbt models, you can use the following commands:

```bash
# Navigate to the dbt project directory
cd data_pipeline/dbt

# Install dbt dependencies
dbt deps

# Run the dbt models
dbt run

# Run dbt tests
dbt test
```

## CI/CD Workflow

This project includes a simple CI/CD workflow using GitHub Actions. The workflow is defined in `.github/workflows/ci.yml` and automatically runs `dbt test` on every push to the repository. To make this work, you'll need to add your Snowflake credentials as secrets to your GitHub repository. The required secrets are:

*   `SNOWFLAKE_ACCOUNT`
*   `SNOWFLAKE_USER`
*   `SNOWFLAKE_PASSWORD`
*   `SNOWFLAKE_ROLE`
*   `SNOWFLAKE_DATABASE`
*   `SNOWFLAKE_WAREHOUSE`
*   `SNOWFLAKE_SCHEMA`
