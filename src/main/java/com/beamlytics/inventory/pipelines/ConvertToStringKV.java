package com.beamlytics.inventory.pipelines;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

public class ConvertToStringKV extends DoFn<KV<Row,Long>, KV<String,String>> {

        @ProcessElement
        public void processElement(@Element KV<Row,Long> inputElement, OutputReceiver<KV<String,String>> out)
        {
            Row inputRow = inputElement.getKey();
            Long count = inputElement.getValue();

            assert inputRow != null;
            Integer key_store =  inputRow.getValue("store_id");
            Integer key_product =  inputRow.getValue("product_id");
            Long time_stamp=inputRow.getValue("timestamp");

            String key_store_s;

            if (null==key_store)
            {key_store_s="";}
            else {
                key_store_s=Integer.toString(key_store);
            }

            assert count != null;
            assert time_stamp != null;

            String key_redis_fscount = key_store_s+":"+Integer.toString(key_product);
            String value_redis_fscount = Long.toString(time_stamp)+":"+Long.toString(count);

            out.output(KV.of(key_redis_fscount,value_redis_fscount));
        }


}
