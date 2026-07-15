# Bronze Layer - Ingestão de Cotações BCB com Auto Loader

import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="cotacao_moeda_bcb",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def cotacao_moeda_bcb():
    # Catalog fixo - gerenciado via bundle variables no pipeline YAML
    # O DLT pipeline já está configurado com o catalog correto via ${var.catalog}
    catalog = "dev_catalog"
    volume_path = f"/Volumes/{catalog}/default/landing_zone/cotacoes/"
    schema_location = f"/Volumes/{catalog}/default/landing_zone/_schemas/cotacao_moeda_bcb/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(volume_path)
        .withColumns({
            "_bronze_ingest_timestamp": F.current_timestamp(),
            "_bronze_source_file": F.col("_metadata.file_path")
        })
    )
