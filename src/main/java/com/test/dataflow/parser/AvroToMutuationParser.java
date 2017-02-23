package com.test.dataflow.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

import com.test.dataflow.util.SSTFileUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Created by arumugv on 2/21/17.
 */
public class AvroToMutuationParser extends DoFn<GenericRecord, Mutation> {

    public static final Logger LOGGER = LoggerFactory.getLogger(AvroToMutuationParser.class);

    //private Set<String> avroFields;

    private String jsonMapping;

    private static final ObjectMapper mapper = new ObjectMapper();



    public AvroToMutuationParser(Schema schema, String mappingFile) throws IOException{
       // schema.getF
        jsonMapping = SSTFileUtils.loadFilefromClasspath(mappingFile);
        LOGGER.info("json mapping is {}",jsonMapping);

    }

    @Override
    public void processElement(ProcessContext c) throws Exception {

        GenericRecord record = c.element();

        JsonNode mapppingNode = mapper.readValue(jsonMapping, JsonNode.class);

        JsonNode keyArrNode = mapppingNode.findPath("keys");

        //construct key
        StringBuilder key = new StringBuilder();
        if(keyArrNode.isArray()){
            for(int i=0; i < keyArrNode.size(); i++){
                JsonNode node = keyArrNode.get(i);
                if(i == keyArrNode.size() - 1){
                    key.append(record.get(node.asText()));
                }else{
                    key.append(record.get(node.asText()));
                    key.append( "#");
                }
            }

        }
        LOGGER.info("key is {}",key.toString());
       // System.out.println(key.toString());
        JsonNode columnArrNode = mapppingNode.findPath("columns");

        //construct columns
        if(columnArrNode.isArray()){
            for (JsonNode node : columnArrNode) {
                StringBuilder columnQualifier = new StringBuilder();
                if(StringUtils.isNotEmpty(node.path("column-qualifier-prefix").asText())){
                    columnQualifier.append(node.path("column-qualifier-prefix").asText());
                    columnQualifier.append(":");
                }

               // System.out.println(columnQualifier.toString());
                columnQualifier.append(record.get(node.path("column-qualifier").asText()).toString());
                LOGGER.info("columnQualifier is {}",columnQualifier.toString());
                c.output(new Put(key.toString().getBytes()).addColumn(node.path("column-family").asText().getBytes(),
                        columnQualifier.toString().getBytes(),
                        record.get(node.path("avro-field-name").asText()).toString().getBytes()));

            }
        }


    }


}
