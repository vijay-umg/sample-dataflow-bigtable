/*
package com.test.dataflow.parser;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

*/
/**
 * Created by arumugv on 2/21/17.
 *//*

public class ArtistDailyByTerrAvroToMutuation extends DoFn<GenericRecord, Mutation> {

    public ArtistDailyByTerrAvroToMutuation(){

    }

    @Override
    public void processElement(DoFn<GenericRecord, Mutation>.ProcessContext c) throws Exception {

        GenericRecord gr = c.element();
        //construct key
        StringBuilder key = new StringBuilder();
        key.append(gr.get("master_artist_id") + "#");
        key.append(gr.get("day") + "#");
        key.append(gr.get("country_subdivision_code") + "#");

        c.output(new Put(key.toString().getBytes()).addColumn("key".getBytes(),
                "units".getBytes(),
                gr.get("units").toString().getBytes()));

        c.output(new Put(key.toString().getBytes()).addColumn("key".getBytes(),
                "physical_album_units".getBytes(),
                gr.get("physical_album_units").toString().getBytes()));

        c.output(new Put(key.toString().getBytes()).addColumn("key".getBytes(),
                "digital_album_units".getBytes(),
                gr.get("digital_album_units").toString().getBytes()));

        c.output(new Put(key.toString().getBytes()).addColumn("key".getBytes(),
                "digital_track_units".getBytes(),
                gr.get("digital_track_units").toString().getBytes()));

    }


}
*/
