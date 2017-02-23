gradle run -Pargs="--runner=BlockingDataflowPipelineRunner --project=umg-dev --stagingLocation=gs://umg-dev/temp/dataflow --bigtableProjectId=umg-dev --bigtableInstanceId=consumption-swift --bigtableTableId=artist_daily_by_territory --inputAvroSchemaFileName=avro-schema/artist_daily_by_territory.avsc --bigTableMappingFile=bigtable-mapping/artist_daily_by_territory_mapping.json --autoscalingAlgorithm=NONE --numWorkers=30 --inputBigTable=gs://umg-dev/data/consumption/artist_daily_by_territory/*"
#test_artists_daily_by_territory

#gs://umg-dev/data/consumption/artist_daily_by_territory/*