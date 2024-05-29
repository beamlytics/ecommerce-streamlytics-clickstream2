/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.beamlytics.inventory.businesslogic.core.transforms.stock;

import com.beamlytics.inventory.businesslogic.core.transforms.CreateStockAggregatorMetadata;
import com.beamlytics.inventory.businesslogic.core.utils.Print;
import com.beamlytics.inventory.dataobjects.Stock.StockEvent;
import com.beamlytics.inventory.dataobjects.StockAggregation;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.joda.time.Instant;

import javax.annotation.Nullable;

// @Experimental
public class CountIncomingStockPerProductLocation
    extends PTransform<PCollection<StockEvent>, PCollection<StockAggregation>> {

  private Duration duration;

  public CountIncomingStockPerProductLocation(Duration duration) {
    this.duration = duration;
  }

  public CountIncomingStockPerProductLocation(@Nullable String name, Duration duration) {
    super(name);
    this.duration = duration;
  }


  @Override
  public PCollection<StockAggregation> expand(PCollection<StockEvent> input) {



    //TODO: #20 remove hard coded attribute names if we allow integration with various systems
     PCollection<Row> step1 = input
        .apply("SelectProductIdStoreId", Select.<StockEvent>fieldNames("product_id", "store_id", "count")
        );


     step1.apply(ParDo.of(new Print<>("SelectProductIdStoreId-step1: ")));

 //TODO : sum of longs is not correct. rather we should just choose latest value of count

//             PCollection<Row> step2 = step1.apply(
//            Group.<Row>byFieldNames("product_id", "store_id","timestamp")
//                .aggregateField("count", Sum.ofLongs(), "count"));


 // always pick the latest count, as it is final available count
 // apache beam does not support applying Latest.combineFn() while Group.byFieldNames() transformation
 // Following approach is used , it is cumbersome
 // first convert Row to KV (Row, Long) storing count in value for each row.

 //  then group byKey and get latest count -- this can be achieved by a simple transformation using Latest.perKye()
 // then convert KV back into Row and put the count as an attribute of row
 // continue step4 transformation to convert Row into StockAggregation object

      final Schema outputSchema =
              Schema.of(
                      Schema.Field.of("product_id", FieldType.INT32),
                      Schema.Field.of("store_id", FieldType.INT32));

      PCollection<KV<Row,Long>> step2 = step1.apply(ParDo.of(new DoFn<Row, KV<Row, Long>>() {

          @ProcessElement
          public void processElement(@Element Row inputRow, OutputReceiver <KV<Row, Long>> out, @Timestamp Instant instant)
          {

              Row outputRow = Row.withSchema(outputSchema)
                      .addValue(inputRow.getValue("product_id"))
                      .addValue(inputRow.getValue("store_id")).build();

              Long count = inputRow.getValue("count");
              out.outputWithTimestamp(KV.of(outputRow,count),instant);

          }


      })).setCoder(KvCoder.of(RowCoder.of(outputSchema), BigEndianLongCoder.of()));

             step2.apply(ParDo.of(new Print<>("SelectProductIdStoreId-step2: ")));



    PCollection<KV<Row,Long>> step3 = step2.apply(
            "SelectLatestProductIdStoreIdCount",
            Latest.perKey());

    step3.apply(ParDo.of(new Print<>("SelectLatestProductIdStoreIdCount-step3: ")));

      final Schema outputSchema_step4 =
              Schema.of(
                      Schema.Field.of("product_id", FieldType.INT32),
                      Schema.Field.of("store_id", FieldType.INT32),
                      Schema.Field.of("count", FieldType.INT64));

    PCollection<Row> step4 = step3.apply( ParDo.of(new DoFn<KV<Row, Long>, Row>() {

        @ProcessElement
        public void processElement( @Element KV<Row, Long> inputKV, OutputReceiver<Row> outputReceiver, @Timestamp Instant instant)
        {
            Row row = inputKV.getKey();
            Long count = inputKV.getValue();

            Row.Builder builder = Row.withSchema(outputSchema_step4);
            assert row != null;
            builder.addValue(row.getValue("product_id"));
            builder.addValue(row.getValue("store_id"));
            builder.addValue(count);
            Row outputRow = builder.build();
            outputReceiver.outputWithTimestamp(outputRow,instant);
        }
            })

    ).setRowSchema(outputSchema_step4);

      step4.apply(ParDo.of(new Print<>("SelectLatestProductIdStoreIdCount-step3: ")));

   return step4
        .apply(
            AddFields.<Row>create()
                // Need this field until we have @Nullable Schema check
                .field("durationMS", FieldType.INT64)
                .field("startTime", FieldType.INT64))
        .apply(Convert.to(StockAggregation.class))
        .apply(new CreateStockAggregatorMetadata(duration.getMillis()));
  }


}
