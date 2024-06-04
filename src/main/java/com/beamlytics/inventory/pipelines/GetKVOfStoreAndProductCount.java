package com.beamlytics.inventory.pipelines;

import com.beamlytics.inventory.dataobjects.StockAggregation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

public class GetKVOfStoreAndProductCount extends DoFn<StockAggregation, KV<Row, Long>> {

       Schema schema;
       Boolean negativeCount=false;
        public GetKVOfStoreAndProductCount(Schema schema, Boolean negativeCount){
            this.schema=schema;
            this.negativeCount=negativeCount;
        }
        @ProcessElement
        public void processElement(@Element StockAggregation input, OutputReceiver<KV<Row, Long>> out, @Timestamp Instant instant) {

            //int storeId = null==input.getStoreId() ? null: input.getStoreId();
            int productId = input.getProductId();




            Row outputRow = Row.withSchema(schema).addValue(productId).addValue(null==input.getStoreId()?null:input.getStoreId()).addValue(instant.getMillis()).build();

            Long count = input.getCount();

            if (null==count)  count=0L;
            if (this.negativeCount && count>0) count=count*(-1);


            out.outputWithTimestamp(KV.of(outputRow, count), instant);


        }

    }


