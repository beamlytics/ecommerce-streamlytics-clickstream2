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
package com.beamlytics.inventory.pipelines;

//TODO: remove all google guava dependencies project wise
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.*;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
//TODO #26 : remove all Experimental annotation once verified that its working fine and no-longer experiement
import org.apache.http.annotation.Experimental;
import org.joda.time.Duration;

import com.beamlytics.inventory.businesslogic.core.options.RetailPipelineOptions;
import com.beamlytics.inventory.businesslogic.core.transforms.clickstream.ClickstreamProcessing;
import com.beamlytics.inventory.businesslogic.core.transforms.clickstream.WriteAggregationToBigQuery;
import com.beamlytics.inventory.businesslogic.core.transforms.stock.CountGlobalStockUpdatePerProduct;
import com.beamlytics.inventory.businesslogic.core.transforms.stock.CountIncomingStockPerProductLocation;
import com.beamlytics.inventory.businesslogic.core.transforms.stock.StockProcessing;
import com.beamlytics.inventory.businesslogic.core.transforms.transaction.CountGlobalStockFromTransaction;
import com.beamlytics.inventory.businesslogic.core.transforms.transaction.TransactionPerProductAndLocation;
import com.beamlytics.inventory.businesslogic.core.transforms.transaction.TransactionProcessing;
import com.beamlytics.inventory.businesslogic.core.utils.Print;
import com.beamlytics.inventory.businesslogic.core.utils.ReadPubSubMsgPayLoadAsString;
import com.beamlytics.inventory.dataobjects.ClickStream.ClickStreamEvent;
import com.beamlytics.inventory.dataobjects.Stock.StockEvent;
import com.beamlytics.inventory.dataobjects.StockAggregation;
import com.beamlytics.inventory.dataobjects.Transaction.TransactionEvent;

/**
 * Primary pipeline using {@link ClickstreamProcessing}
 */
@Experimental
public class RetailDataProcessingPipeline {

  @VisibleForTesting public PCollection<ClickStreamEvent> testClickstreamEvents = null;

  @VisibleForTesting public PCollection<String> testTransactionEvents = null;

  @VisibleForTesting public PCollection<String> testStockEvents = null;

  public void startRetailPipeline(Pipeline p) throws Exception {

    RetailPipelineOptions options = p.getOptions().as(RetailPipelineOptions.class);

    boolean prodMode = !options.getTestModeEnabled();

    /**
     * **********************************************************************************************
     * Process Clickstream
     * **********************************************************************************************
     */
    PCollection<String> clickStreamJSONMessages = null;

    if (prodMode) {
      clickStreamJSONMessages =
          p.apply(
              "ReadClickStream",
              PubsubIO.readStrings()
                  .fromSubscription(options.getClickStreamPubSubSubscription())
                  .withTimestampAttribute("TIMESTAMP"));
    } else {
      checkNotNull(testClickstreamEvents, "In TestMode you must set testClickstreamEvents");
      clickStreamJSONMessages = testClickstreamEvents.apply(ToJson.of());
    }
    clickStreamJSONMessages.apply(new ClickstreamProcessing());

    /**
     * **********************************************************************************************
     * Process Transactions
     * **********************************************************************************************
     */


   PCollection<String> transactionsJSON = null;
   if (prodMode) {
     transactionsJSON =
         p.apply(
             "ReadTransactionStream",
             new ReadPubSubMsgPayLoadAsString(options.getTransactionsPubSubSubscription()));
   } else {
     checkNotNull(testTransactionEvents, "In TestMode you must set testClickstreamEvents");
     transactionsJSON = testTransactionEvents;
   }

   PCollection<TransactionEvent> transactionWithStoreLoc =
       transactionsJSON.apply(new TransactionProcessing());

    
    
    /**
     * **********************************************************************************************
     * Aggregate sales per item per location
     * **********************************************************************************************
     */

//TODO #25 : Modify transaction schema to add type of transaction from event hub: 
// EOMM-SALE, TAKE-SALE, ORDER-CONFIRM-IN-OMS, ORDER-CANCEL-IN-OMS, ORDER-SCHEDULE-IN-OMS, ORDER-CONFIRMED-IN-WMS, ORDER-SHIPPED-FROM-WMS, ORDER-RETURNED-STORE, ORDER-RETURNED-WMS      

//TODO #24 : Filter transactions to calculate availability, rest should be written to bigquery only for analytical purposes
// e.g. TAKE-SALE and ORDER-SHIPPED-FROM-WMS will reduce from supply and demand both
// but ECCOMM-SALE will create an open demand till we ship the order, but we need to reduce it from supply to update availability, so it needs to be tracked under a demand bucket, which would further be subdivided into demand type buckets to allow the demand to move as order is processed in OMS and WMS till it shipped out. 
// Need to maintain transactional-atomic-consistency when moving demand from one bucket to another

   PCollection<StockAggregation> transactionPerProductAndLocation =
       transactionWithStoreLoc.apply(new TransactionPerProductAndLocation());

  //TODO: #12 remove hardcoded seconds     

   PCollection<StockAggregation> inventoryTransactionPerProduct =
       transactionPerProductAndLocation.apply(
           new CountGlobalStockFromTransaction(Duration.standardSeconds(5)));

    /**
     * **********************************************************************************************
     * Process Stock stream
     * **********************************************************************************************
     */
   PCollection<String> inventoryJSON = null;
   if (prodMode) {
     inventoryJSON =
         p.apply(
             "ReadStockStream",
             new ReadPubSubMsgPayLoadAsString(options.getInventoryPubSubSubscriptions()));
   } else {
     checkNotNull(testStockEvents, "In TestMode you must set testClickstreamEvents");
     inventoryJSON = testStockEvents;
   }

   PCollection<StockEvent> inventory = inventoryJSON.apply(new StockProcessing());

    /**
     * **********************************************************************************************
     * Aggregate Inventory delivery per item per location
     * **********************************************************************************************
     */
   //TODO: #13 remove hardcoded seconds in counting inventory
   
     PCollection<StockAggregation> incomingStockPerProductLocation =
       inventory.apply(new CountIncomingStockPerProductLocation(Duration.standardSeconds(5)));

  //TODO: #14 remove hardcoded seconds
  
       PCollection<StockAggregation> incomingStockPerProduct =
       incomingStockPerProductLocation.apply(
           new CountGlobalStockUpdatePerProduct(Duration.standardSeconds(5)));

    /**
     * **********************************************************************************************
     * Write Stock Aggregates - Combine Transaction / Inventory
     * **********************************************************************************************
     */
   PCollection<StockAggregation> inventoryLocationUpdates =
       PCollectionList.of(transactionPerProductAndLocation)
           .and(inventoryTransactionPerProduct)
           .apply(Flatten.pCollections());

   PCollection<StockAggregation> inventoryGlobalUpdates =
       PCollectionList.of(inventoryTransactionPerProduct)
           .and(incomingStockPerProduct)
           .apply(Flatten.pCollections());



// We are writing supply and demand of each product in a row to biquery, aggregated for past 5 min.

           //TODO #11 : remove the hardcoding of 10 seconds and paramterize it



           inventoryLocationUpdates.apply(
       WriteAggregationToBigQuery.create("StoreStockEvent", Duration.standardSeconds(10)));

   inventoryGlobalUpdates.apply(
       WriteAggregationToBigQuery.create("GlobalStockEvent", Duration.standardSeconds(10)));

    /**
     * **********************************************************************************************
     * Send Inventory updates to PubSub
     * **********************************************************************************************
     */
// TODO: #23 Add an attribute by calculating availability as "total supply - total demand"    

// TODO: #22 Add an attribute by projecting future availability by date

//TODO: add looker visulation for streaming data for total demand, total supply, total on hand availability-and drilled down to store level

   PCollection<String> stockUpdates =
       inventoryGlobalUpdates.apply(
           "ConvertToPubSub", MapElements.into(TypeDescriptors.strings()).via(Object::toString));

   if (options.getTestModeEnabled()) {
     stockUpdates.apply(ParDo.of(new Print<>("Inventory PubSub Message is: ")));
   } else {
     stockUpdates.apply(PubsubIO.writeStrings().to(options.getAggregateStockPubSubOutputTopic()));
   }

    p.run();
  }

  public static void main(String[] args) throws Exception {

    RetailPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(RetailPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    new RetailDataProcessingPipeline().startRetailPipeline(p);
  }
}
