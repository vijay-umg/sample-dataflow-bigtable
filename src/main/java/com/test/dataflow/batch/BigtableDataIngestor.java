package com.test.dataflow.batch;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.test.dataflow.options.ConsumptionsDataFlowOptions;
import com.test.dataflow.parser.AvroToMutuationParser;
import com.test.dataflow.util.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by arumugv on 2/21/17.
 */
public class BigtableDataIngestor {

    public static final Logger LOGGER = LoggerFactory.getLogger(BigtableDataIngestor.class);

    public static void main(String[] args) throws Exception{

        // CloudBigtableOptions is one way to retrieve the options.  It's not required.
        ConsumptionsDataFlowOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ConsumptionsDataFlowOptions.class);

        // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
        CloudBigtableTableConfiguration config =
                CloudBigtableTableConfiguration.fromCBTOptions(options);

        Pipeline pipeline = Pipeline.create(options);


        Schema schema = AvroSchemaUtil.getSchemaFromClasspath(options.getInputAvroSchemaFileName());
       // Schema schema = (new Schema.Parser()).parse(BigtableDataIngestor.class.getResourceAsStream(options.getInputAvroSchemaFileName()));

        //"gs://umg-dev/data/consumption/samples/artist_daily_by_territory.avro"
        PCollection inputCollection = (PCollection)pipeline.apply(AvroIO.Read.named("read avro").from(options.getInputBigTable()).withSchema(schema));



        // This sets up serialization for Puts and Deletes so that Dataflow can potentially move them
        // through the network
        CloudBigtableIO.initializeForWrite(pipeline);

        CloudBigtableScanConfiguration bconfig = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();


        PCollection inputCollectionMutated = (PCollection)inputCollection
                .apply(ParDo.of(new AvroToMutuationParser(schema, options.getBigTableMappingFile())));


        inputCollectionMutated.apply(CloudBigtableIO.writeToTable(config));

        pipeline.run();
    }
}
