from pyspark import pipelines as dp
from pyspark.sql import SparkSession

# Bronze layer: Raw ingestion from PostgreSQL dvdrental database
# Reads the film table with minimal transformations

@dp.table(
    comment="Bronze layer - Raw film data from PostgreSQL dvdrental database",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "film_id"
    }
)
def dvdrental_film():
    """
    Ingest film table from PostgreSQL database.
    
    Uses the default schema from pipeline config (bronze).
    
    Connection details should be configured via:
    - Databricks secrets for credentials
    - Pipeline configuration for host/database
    """
    
    # PostgreSQL connection properties from Databricks secrets
    # Create secrets using: databricks secrets create-scope --scope postgres-secrets
    # Add values using: databricks secrets put --scope postgres-secrets --key <key_name>
    
    host = dbutils.secrets.get(scope="postgres-secrets", key="postgres-host")
    port = dbutils.secrets.get(scope="postgres-secrets", key="postgres-port")
    database = dbutils.secrets.get(scope="postgres-secrets", key="postgres-database")
    username = dbutils.secrets.get(scope="postgres-secrets", key="postgres-user")
    password = dbutils.secrets.get(scope="postgres-secrets", key="postgres-password")
    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    
    # Read from PostgreSQL film table
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dvdrental.film")
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    
    return df
