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

import com.beamlytics.inventory.businesslogic.core.options.RetailPipelineOptions;
import com.beamlytics.inventory.businesslogic.core.utils.JSONUtils;
import com.beamlytics.inventory.businesslogic.core.utils.Print;
import com.beamlytics.inventory.businesslogic.core.utils.WriteRawJSONMessagesToBigQuery;
import com.beamlytics.inventory.dataobjects.Stock.StockEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

// @Experimental
public class StockProcessing extends PTransform<PCollection<String>, PCollection<StockEvent>> {

  @Override
  public PCollection<StockEvent> expand(PCollection<String> input) {
    Pipeline p = input.getPipeline();

    RetailPipelineOptions options = p.getOptions().as(RetailPipelineOptions.class);

    /**
     * **********************************************************************************************
     * Write Raw Inventory delivery
     * **********************************************************************************************
     */

    //@Nishana : since all raw input is stored in raw table, and clean , parsed messged are stored in clean table
    // we can always do outer join to get parse failed messages.
    // In this case, we may not need a separte bq table to store parse failed messages.

    input.apply(new WriteRawJSONMessagesToBigQuery(options.getInventoryBigQueryRawTable()));
    /**
     * **********************************************************************************************
     * Validate Inventory delivery
     * **********************************************************************************************
     */
    //this is equivalent to schema validation
    PCollection<StockEvent> inventory =
        input.apply(JSONUtils.ConvertJSONtoPOJO.create(StockEvent.class));

    /**
     * **********************************************************************************************
     * Write Store Corrected Inventory Data To DW
     * **********************************************************************************************
     */
    if (options.getTestModeEnabled()) {
      input.apply(ParDo.of(new Print<>("StoreCorrectedInventoryDataToDW")));

    } else {
      inventory.apply(
          "StoreCorrectedInventoryDataToDW",
          BigQueryIO.<StockEvent>write()
              .useBeamSchema()
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              // TODO #8 : enable time partitioning on this table 
              //     .withTimePartitioning(new TimePartitioning().setField("timestamp"))
              .to(
                  String.format(
                      "%s:%s",
                      options.getDataWarehouseOutputProject(),
                      options.getInventoryBigQueryCleanTable())));
    }

    //TODO #9 : remove hardcoded fixed duration of 5 sec, also this duration needs to be lower for ecommerce env. We need to test with different values and figure out minimal time system can support with varying number of worker nodes and capacities
    //return inventory.apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));
    return inventory;
  }
}
