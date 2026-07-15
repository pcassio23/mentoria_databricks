# Bronze Layer - Ingestão de Cotações BCB com Auto Loader

import dlt
from pyspark.sql import functions as F

@dlt.table(name=f"{spark.conf.get('catalog', 'dev_catalog')}.bronze.cotacao_moeda_bcb")
def cotacao_moeda_bcb():
    catalog = spark.conf.get("catalog", "dev_catalog")
    volume_path = f"/Volumes/{catalog}/default/landing_zone/cotacoes/"
    schema_location = f"/Volumes/{catalog}/default/landing_zone/_schemas/cotacao_moeda_bcb/"
    
    return (
        spark.readStream
        .format("cloudFiles")  # ← Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(volume_path)
        .withColumn("_bronze_ingest_timestamp", F.current_timestamp())
        .withColumn("_bronze_source_file", F.input_file_name())
    )