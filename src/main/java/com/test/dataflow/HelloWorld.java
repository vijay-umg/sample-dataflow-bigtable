package com.test.dataflow;

import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

/**
 * Created by arumugv on 2/18/17.
 */
public class HelloWorld {
    private static final byte[] FAMILY = Bytes.toBytes("cf");
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");

    // This is a random value so that there will be some changes in the table
    // each time the job runs.
    private static final byte[] VALUE = Bytes.toBytes("value_" + (60 * Math.random()));

    static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
            c.output(new Put(c.element().getBytes()).addColumn(FAMILY, QUALIFIER, VALUE));
        }
    };

    /**
     * <p>Creates a dataflow pipeline that creates the following chain:</p>
     * <ol>
     *   <li> Puts an array of "Hello", "World" into the Pipeline
     *   <li> Creates Puts from each of the words in the array
     *   <li> Performs a Bigtable Put on the items in the
     * </ol>
     *
     * @param args Arguments to use to configure the Dataflow Pipeline.  The first three are required
     *   when running via managed resource in Google Cloud Platform.  Those options should be omitted
     *   for LOCAL runs.  The last four arguments are to configure the Bigtable connection.
     *        --runner=BlockingDataflowPipelineRunner
     *        --project=[dataflow project] \\
     *        --stagingLocation=gs://[your google storage bucket] \\
     *        --bigtableProject=[bigtable project] \\
     *        --bigtableInstanceId=[bigtable instance id] \\
     *        --bigtableTableId=[bigtable tableName]
     */

    public static void main(String[] args) {
        // CloudBigtableOptions is one way to retrieve the options.  It's not required.
        CloudBigtableOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CloudBigtableOptions.class);

        // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
        CloudBigtableTableConfiguration config =
                CloudBigtableTableConfiguration.fromCBTOptions(options);

        Pipeline p = Pipeline.create(options);
        // This sets up serialization for Puts and Deletes so that Dataflow can potentially move them
        // through the network
        CloudBigtableIO.initializeForWrite(p);

        CloudBigtableScanConfiguration bconfig = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();

        p.apply(Create.of("Hello", "World"))
                .apply(ParDo.of(MUTATION_TRANSFORM))
                .apply(CloudBigtableIO.writeToTable(config));

        p.run();
    }
}
