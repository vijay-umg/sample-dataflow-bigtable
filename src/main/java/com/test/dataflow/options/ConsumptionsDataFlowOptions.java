package com.test.dataflow.options;

import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;


/**
 * Created by arumugv on 2/21/17.
 */
public interface ConsumptionsDataFlowOptions extends CloudBigtableOptions{

    @Description("Path to the directory of input for big table")
    String getInputBigTable();

    void setInputBigTable(String value);


    @Description("Input Avro schema file")
    @Validation.Required
    String getInputAvroSchemaFileName();
    void setInputAvroSchemaFileName(String value);

    @Description("Input Big table mapping file")
    String getBigTableMappingFile();
    void setBigTableMappingFile(String value);

}
