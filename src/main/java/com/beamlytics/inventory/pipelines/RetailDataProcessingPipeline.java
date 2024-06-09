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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.http.annotation.Experimental;
import org.joda.time.Duration;

import java.util.Map;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary pipeline using {@link ClickstreamProcessing}
 */
@Experimental
public class RetailDataProcessingPipeline {

    @VisibleForTesting
    public PCollection<ClickStreamEvent> testClickstreamEvents = null;

    @VisibleForTesting
    public PCollection<String> testTransactionEvents = null;

    @VisibleForTesting
    public PCollection<String> testStockEvents = null;

    public static void main(String[] args) throws Exception {

        RetailPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(RetailPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        new RetailDataProcessingPipeline().startRetailPipeline(p);
    }

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
            clickStreamJSONMessages = p.apply("ReadClickStream", PubsubIO.readStrings().fromSubscription(options.getClickStreamPubSubSubscription()).withTimestampAttribute("TIMESTAMP"));
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
            transactionsJSON = p.apply("ReadTransactionStream", new ReadPubSubMsgPayLoadAsString(options.getTransactionsPubSubSubscription()));
        } else {
            checkNotNull(testTransactionEvents, "In TestMode you must set testClickstreamEvents");
            transactionsJSON = testTransactionEvents;
        }

        PCollection<TransactionEvent> transactionWithStoreLoc = transactionsJSON.apply(new TransactionProcessing());
        transactionWithStoreLoc.apply(ParDo.of(new Print<>("transactionWithStoreLoc - 5 sec fixed windowed transactions")));

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

        PCollection<StockAggregation> transactionPerProductAndLocation = transactionWithStoreLoc.apply(new TransactionPerProductAndLocation());
        transactionPerProductAndLocation.apply(ParDo.of(new Print<>("transactionPerProductAndLocation : Sum of windowed transactions")));

        //TODO: #12 remove hardcoded seconds

        PCollection<StockAggregation> inventoryTransactionPerProduct = transactionPerProductAndLocation.apply(new CountGlobalStockFromTransaction(Duration.standardSeconds(5)));
        inventoryTransactionPerProduct.apply(ParDo.of(new Print<>("inventoryTransactionPerProduct: Sum of global Transaction Per Product, summed over 5 seconds window")));
        /**
         * **********************************************************************************************
         * Process Stock stream
         * **********************************************************************************************
         */
        PCollection<String> inventoryJSON = null;
        if (prodMode) {
            inventoryJSON = p.apply("ReadStockStream", new ReadPubSubMsgPayLoadAsString(options.getInventoryPubSubSubscriptions()));
        } else {
            checkNotNull(testStockEvents, "In TestMode you must set testClickstreamEvents");
            inventoryJSON = testStockEvents;
        }

        PCollection<StockEvent> stock_full_and_delta_updates = inventoryJSON.apply(new StockProcessing());

        PCollection<StockEvent> stock_full_sync_updates = stock_full_and_delta_updates.apply(Filter.by(stockevent -> {
            assert stockevent != null;
            return (stockevent.getEventType() == null || stockevent.getEventType().isEmpty() || stockevent.getEventType().equalsIgnoreCase("Full"));
        }));


        //Full stock update due to full sync or due to cycle count of a single SKU across warehouse.
        //Can not accept partial cycle count from a single location in warehpuse.
        //The window is 24hours but emitted within 1 minute of arrival in the window , and allows till 2 hours after watermark. It ignores the fired pane.
        //If a full sync comes late , it can be safely ignored if an earlier full sync over-wrote the supply picture.
        PCollection<StockEvent> stock_full_sync_updates_windowed;
        stock_full_sync_updates_windowed = stock_full_sync_updates
                .apply(
                        Window.<StockEvent>into(FixedWindows.of(Duration.standardHours(24)))
                                .triggering(Repeatedly.forever(AfterFirst.of(
                                        AfterPane.elementCountAtLeast(1),
                                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))).discardingFiredPanes().withAllowedLateness(Duration.standardHours(2))
                );

        stock_full_sync_updates_windowed.apply(ParDo.of(new Print<>("stock_full_sync_updates_windowed")));

        /**
         *
         * **********************************************************************************************
         * Aggregate Inventory delivery per item per location
         * **********************************************************************************************
         */
        //TODO: #13 remove hardcoded seconds in counting inventory

        PCollection<StockAggregation> incomingStockPerProductLocation = stock_full_sync_updates_windowed.apply(new CountIncomingStockPerProductLocation(Duration.standardSeconds(5)));
        incomingStockPerProductLocation.apply(ParDo.of(new Print<>("incomingStockPerProductLocation -- Latest full stock per product per location in last 5 second window")));

        //TODO: #14 remove hardcoded seconds

        PCollection<StockAggregation> incomingStockPerProduct = incomingStockPerProductLocation.apply(new CountGlobalStockUpdatePerProduct(Duration.standardSeconds(5)));
        incomingStockPerProduct.apply(ParDo.of(new Print<>("incomingStockPerProduct -- Latest full stock globally in last 5 second window")));

        /**
         * **********************************************************************************************
         * Write Stock Aggregates - Combine Transaction / Inventory
         * **********************************************************************************************
         */


        final Schema product_id_store_id_key_schema = Schema.of(
                Schema.Field.of("product_id", Schema.FieldType.INT32),
                Schema.Field.of("store_id", Schema.FieldType.INT32).withNullable(true),
                Schema.Field.of("timestamp", Schema.FieldType.INT64)

        );

        PCollection<KV<Row, Long>> count_of_product_at_each_store_stock =
                incomingStockPerProductLocation.apply(ParDo.of(new GetKVOfStoreAndProductCount(product_id_store_id_key_schema,false)))
                        .setCoder(KvCoder.of(RowCoder.of(product_id_store_id_key_schema),
                BigEndianLongCoder.of()));

        //TODO store count_of_product_at_each_store_stock in memorystore with time stamp



        //count_of_product_at_each_store_stock.apply(ParDo.of(new Print<>("count_of_product_at_each_store_stock")));

        PCollection<KV<String,String>> redis_key_value_withts_fs =
                count_of_product_at_each_store_stock.apply(ParDo.of(new ConvertToStringKV()));


        redis_key_value_withts_fs.apply(ParDo.of(new Print<>("redis_key_value_withts_fs -- Latest full stock per product per location in last 5 second window writing to memorystore")));



        redis_key_value_withts_fs.apply("Writing Full Stock Postion Per Product Per Store to MemoryStore",
                        RedisIO.write().withMethod(RedisIO.Write.Method.SET)
                                .withEndpoint(options.getRedisHost(), options.getRedisPort()).withAuth(options.getRedisAuth()));

        PCollection<KV<Row, Long>> count_of_product_at_each_store_transaction =
                transactionPerProductAndLocation.apply(ParDo.of(new GetKVOfStoreAndProductCount(product_id_store_id_key_schema,true)))
                        .setCoder(KvCoder.of(RowCoder.of(product_id_store_id_key_schema),
                                BigEndianLongCoder.of()));;

        //count_of_product_at_each_store_transaction.apply(ParDo.of(new Print<>("count_of_product_at_each_store_transaction")));

        PCollection<KV<Row, Long>> count_of_product_at_global_stock =
                incomingStockPerProduct.apply(ParDo.of(new GetKVOfStoreAndProductCount(product_id_store_id_key_schema,false)))
                        .setCoder(KvCoder.of(RowCoder.of(product_id_store_id_key_schema),
                                BigEndianLongCoder.of()));

        //count_of_product_at_global_stock.apply(ParDo.of(new Print<>("count_of_product_at_global_stock")));

        PCollection<KV<String,String>> redis_key_value_withts_fs_global =
                count_of_product_at_global_stock.apply(ParDo.of(new ConvertToStringKV()));




        redis_key_value_withts_fs_global.apply(ParDo.of(new Print<>("redis_key_value_withts_fs_global -- Latest full stock globally in last 5 second window writing to memorystore")));

        redis_key_value_withts_fs_global.apply("Writing Full Stock Postion global to MemoryStore",
                RedisIO.write().withMethod(RedisIO.Write.Method.SET)
                        .withEndpoint(options.getRedisHost(), options.getRedisPort()).withAuth(options.getRedisAuth()));



        PCollection<KV<Row, Long>> count_of_product_at_global_transaction =
                inventoryTransactionPerProduct.apply(ParDo.of(new GetKVOfStoreAndProductCount(product_id_store_id_key_schema,true)))
                        .setCoder(KvCoder.of(RowCoder.of(product_id_store_id_key_schema),
                                BigEndianLongCoder.of()));

        count_of_product_at_global_transaction.apply(ParDo.of(new Print<>("count_of_product_at_global_transaction")));






        RedisConnectionConfiguration config = RedisConnectionConfiguration.create().withHost(options.getRedisHost()).withPort(options.getRedisPort()).withAuth(options.getRedisAuth());
        PCollection<String> redis_key_value_withts_fs_result_step1= redis_key_value_withts_fs
                .apply("get keys", Keys.create());
        redis_key_value_withts_fs_result_step1.apply(ParDo.of(new Print<>("redis_key_value_withts_fs_result_step1 - keys")));
        PCollection<KV<String, String>> redis_key_value_withts_fs_result = redis_key_value_withts_fs_result_step1
                .apply(RedisIO.readKeyPatterns().withConnectionConfiguration(config));
        redis_key_value_withts_fs_result.apply(ParDo.of(new Print<>("redis_key_value_withts_fs_result -- Passing this as side input")));


        PCollectionView<Map<String, String>> redis_key_value_withts_fs_result_map = redis_key_value_withts_fs_result.apply(View.asMap());

        PCollection<KV<Row, Long>> inventoryLocationUpdates_Row_Total = count_of_product_at_each_store_transaction
                .apply(ParDo.of(
                new SumOfStocks(redis_key_value_withts_fs_result_map)).withSideInput("side_input",redis_key_value_withts_fs_result_map)
                )
                .setCoder(
                        KvCoder.of(
                                RowCoder.of(
                                        product_id_store_id_key_schema),
                                        BigEndianLongCoder.of()));

        inventoryLocationUpdates_Row_Total.apply(ParDo.of(new Print<>("final count per product per location is: ")));


        PCollection<KV<String,String>> redis_key_value_withts_fs_global_result= redis_key_value_withts_fs_global
                .apply("get keys", Keys.create())
                .apply(RedisIO.readKeyPatterns().withConnectionConfiguration(config));

        redis_key_value_withts_fs_global_result.apply(ParDo.of(new Print<>("redis_key_value_withts_fs_global_result -- Passing this as side input for global inventory")));


        PCollectionView<Map<String, String>> redis_key_value_withts_fs_global_result_map = redis_key_value_withts_fs_global_result.apply(View.asMap());

        PCollection<KV<Row, Long>> inventoryGlobalUpdates_Row_Total = count_of_product_at_global_transaction
                .apply(ParDo.of(
                        new SumOfStocks(redis_key_value_withts_fs_global_result_map)).withSideInput("side_input",redis_key_value_withts_fs_global_result_map)
                )
                .setCoder(
                        KvCoder.of(
                                RowCoder.of(
                                        product_id_store_id_key_schema),
                                BigEndianLongCoder.of()));

        inventoryGlobalUpdates_Row_Total.apply(ParDo.of(new Print<>("final count per product globally is: ")));




        //inventoryLocationGlobalUpdates_Row_Total.apply(ParDo.of(new Print<>("final count per product per location is: ")));

        final Schema totals_row_schema =  Schema.of(
                Schema.Field.of("product_id", Schema.FieldType.INT32),
                Schema.Field.of("store_id", Schema.FieldType.INT32).withNullable(true),
                Schema.Field.of("count", Schema.FieldType.INT64),
                Schema.Field.of("timestamp", Schema.FieldType.DATETIME)
        );

        PCollection<Row> inventoryLocationUpdates_Row_Total_Row = inventoryLocationUpdates_Row_Total.apply
                (ParDo.of(new ConvertToRow(totals_row_schema)))
                .setCoder(
                RowCoder.of(totals_row_schema));

        PCollection<Row> inventoryLocationGlobalUpdates_Row_Total_Row =  inventoryGlobalUpdates_Row_Total.apply(ParDo.of(new ConvertToRow(totals_row_schema))).setCoder(
                RowCoder.of(
                        totals_row_schema
                ));
//        inventoryLocationUpdates_Row_Total_Row.apply(ParDo.of(new Print<>("final count per product per location is: ")));
       // inventoryLocationGlobalUpdates_Row_Total_Row.apply(ParDo.of(new Print<>("final count per product per location is: ")));



        inventoryLocationUpdates_Row_Total_Row.apply(WriteAggregationToBigQuery.create("StoreStockEvent", Duration.standardSeconds(10)));

        inventoryLocationGlobalUpdates_Row_Total_Row.apply(WriteAggregationToBigQuery.create("GlobalStockEvent", Duration.standardSeconds(10)));








// We are writing supply and demand of each product in a row to biquery, aggregated for past 10 sec.

        //TODO #11 : remove the hardcoding of 10 seconds and paramterize it


//        inventoryLocationUpdates.apply(WriteAggregationToBigQuery.create("StoreStockEvent", Duration.standardSeconds(10)));
//
//        inventoryGlobalUpdates.apply(WriteAggregationToBigQuery.create("GlobalStockEvent", Duration.standardSeconds(10)));

        /**
         * **********************************************************************************************
         * Send Inventory updates to PubSub
         * **********************************************************************************************
         */
// TODO: #23 Add an attribute by calculating availability as "total supply - total demand"

// TODO: #22 Add an attribute by projecting future availability by date

// TODO: add looker visualization for streaming data for total demand, total supply, total on hand availability-and drilled down to store level

//        PCollection<String> stockUpdates = inventoryLocationGlobalUpdates_Row_Total_Row.apply("ConvertToPubSub", MapElements.into(TypeDescriptors.strings()).via(Object::toString));
//
//        if (options.getTestModeEnabled()) {
//            stockUpdates.apply(ParDo.of(new Print<>("Inventory PubSub Message is: ")));
//        } else {
//            stockUpdates.apply(PubsubIO.writeStrings().to(options.getAggregateStockPubSubOutputTopic()));
//        }

        p.run();
    }
}
