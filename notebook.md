### Read from the dataset


```python
df = spark.read.format('csv').options(header='true').load('dbfs:/databricks-datasets/COVID/coronavirusdataset/SeoulFloating.csv')
```

### Create the database and collection using the Catalog API


```python
cosmosEndpoint = "https://REPLACEME.documents.azure.com:443/"
cosmosMasterKey = "REPLACEME"
cosmosDatabaseName = "sampleDB"
cosmosContainerName = "sampleContainer"

spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)

spark.sql("CREATE DATABASE IF NOT EXISTS cosmosCatalog.{};".format(cosmosDatabaseName))
spark.sql("CREATE TABLE IF NOT EXISTS cosmosCatalog.{}.{} using cosmos.items TBLPROPERTIES(partitionKeyPath = '/id', manualThroughput = '1100')".format(cosmosDatabaseName, cosmosContainerName))


```

### Ingesting the data


```python
#Set the write configuration
writeCfg = {
  "spark.cosmos.accountEndpoint": cosmosEndpoint,
  "spark.cosmos.accountKey": cosmosMasterKey,
  "spark.cosmos.database": cosmosDatabaseName,
  "spark.cosmos.container": cosmosContainerName,
  "spark.cosmos.write.strategy": "ItemOverwrite",
}

#ingest the data
df\
   .toDF("date","hour","birth_year","sex","province","city","id")\
   .write\
   .format("cosmos.items")\
   .options(**writeCfg)\
   .mode("APPEND")\
   .save()

```

### Reading the data


```python
#Set the read configuration
readCfg = {
  "spark.cosmos.accountEndpoint": cosmosEndpoint,
  "spark.cosmos.accountKey": cosmosMasterKey,
  "spark.cosmos.database": cosmosDatabaseName,
  "spark.cosmos.container": cosmosContainerName,
  "spark.cosmos.partitioning.strategy": "Restrictive",
  "spark.cosmos.read.inferSchemaEnabled" : "false"
}

#Read the data into a Spark dataframe and print the count
query_df = spark.read.format("cosmos.items").options(**readCfg).load()
print(query_df.count())

```
