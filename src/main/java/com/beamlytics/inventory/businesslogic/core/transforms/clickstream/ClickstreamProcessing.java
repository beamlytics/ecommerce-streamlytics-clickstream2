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
package com.beamlytics.inventory.businesslogic.core.transforms.clickstream;

import com.beamlytics.inventory.businesslogic.core.DeploymentAnnotations.PartialResultsExpectedOnDrain;
import com.beamlytics.inventory.businesslogic.core.options.RetailPipelineOptions;
import com.beamlytics.inventory.businesslogic.core.transforms.DeadLetterSink.SinkType;
import com.beamlytics.inventory.businesslogic.core.utils.JSONUtils;
import com.beamlytics.inventory.businesslogic.core.utils.Print;
import com.beamlytics.inventory.businesslogic.core.utils.WriteRawJSONMessagesToBigQuery;
import com.beamlytics.inventory.dataobjects.ClickStream.ClickStreamEvent;
import com.beamlytics.inventory.dataobjects.ClickStream.PageViewAggregator;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.http.annotation.Experimental;
import org.joda.time.Duration;

/**
 * Process clickstream from online stores.
 *
 * <p>Read Click Stream Topic
 *
 * <p>Parse Messages to Beam SCHEMAS
 *
 * <p>Branch 1:
 *
 * <p>Write RAW JSON String Clickstream for storage
 *
 * <p>Branch 2:
 *
 * <p>Clean the data
 *
 * <p>Write Cleaned Data to BigQuery
 *
 * <p>Branch 2.1:
 *
 * <p>Filter out events of type ERROR
 *
 * <p>Count Page Views per product in 5 sec windows
 *
 * <p>Export page view aggregates to BigTable
 *
 * <p>Export page view aggregates to BigQuery
 */
@PartialResultsExpectedOnDrain
@Experimental
public class ClickstreamProcessing extends PTransform<PCollection<String>, PCollection<Row>> {

    @SuppressWarnings("unchecked")
    @Override
    public PCollection<Row> expand(PCollection<String> input) {

        RetailPipelineOptions options =
                input.getPipeline().getOptions().as(RetailPipelineOptions.class);

        Schema csEvtSchema = null;

        try {
            csEvtSchema = input.getPipeline().getSchemaRegistry().getSchema(ClickStreamEvent.class);
        } catch (NoSuchSchemaException e) {
            throw new IllegalArgumentException("Unable to get Schema for ClickStreamEvent class.");
        }

     
        
        /**
         * **********************************************************************************************
         * Parse Messages to SCHEMAS
         * **********************************************************************************************
         */
        PCollection<Row> csEvtRows =
                input.apply(JSONUtils.JSONtoRowWithDeadLetterSink.withSchema(csEvtSchema));

        /**
         * **********************************************************************************************
         * Write RAW JSON String Clickstream for storage
         * **********************************************************************************************
         */
        input.apply(
                "StoreRawData",
                new WriteRawJSONMessagesToBigQuery(
                        options.getDataWarehouseOutputProject(), options.getClickStreamBigQueryRawTable()));

        /**
         * *********************************************************************************************
         * Clean the data
         *
         * <p>*********************************************************************************************
         */
        PCollection<Row> cleanCSRow =
                csEvtRows.apply(
                        new ValidateAndCorrectCSEvt(
                                ((options.getTestModeEnabled()) ? SinkType.LOG : SinkType.BIGQUERY)));

        /**
         * *********************************************************************************************
         * Store Cleaned Data To DW
         *
         * <p>*********************************************************************************************
         */
        if (options.getTestModeEnabled()) {
            cleanCSRow.apply(ParDo.of(new Print<>("StoreCleanedDataToDW: ")));
        } else {
            cleanCSRow.apply(
                    "StoreCleanedDataToDW",
                    BigQueryIO.<Row>write()
                            .useBeamSchema()
                            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            //TODO #2 : enable timestamp as partition, currently the schema defines it as long but it is not encoded as logical type of timestamp, rather string.
                            
                            //.withTimePartitioning(new TimePartitioning().setField("timestamp"))
                            .to(
                                    String.format(
                                            "%s:%s",
                                            options.getDataWarehouseOutputProject(),
                                            options.getClickStreamBigQueryCleanTable())));
        }
        /**
         * *********************************************************************************************
         * Sessionize the data using sessionid
         *
         * <p>*********************************************************************************************
         */
        PCollection<Row> sessionizedCS =
                cleanCSRow.apply(ClickStreamSessions.create(Duration.standardSeconds(30)));

        /**
         * *********************************************************************************************
         * Write sessionized clickstream to BigQuery
         *
         * <p>*********************************************************************************************
         */
        if (options.getTestModeEnabled()) {
            sessionizedCS.apply(ParDo.of(new Print<>("Sessionized Data is: ")));
        } else {
            sessionizedCS.apply(
                    "sessionizedClickstream",
                    BigQueryIO.<Row>write()
                            .useBeamSchema()
                            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            //TODO: #3 Need to make sure that sessionlized data goes to time partitioned table in bigquery
                            //.withTimePartitioning(new TimePartitioning().setField("timestamp"))
                            .to(
                                    String.format(
                                            "%s:%s",
                                            options.getDataWarehouseOutputProject(),
                                            options.getClickStreamSessionizedTable())));
        }
        /**
         * *********************************************************************************************
         * Filter out events of type ERROR
         *
         * <p>*********************************************************************************************
         */
        PCollection<Row> cleanDataWithOutErrorEvents =
                cleanCSRow.apply(Filter.<Row>create().whereFieldName("event", c -> !c.equals("ERROR")));

        /**
         * *********************************************************************************************
         * Count Page Views per product in 5 sec windows
         *
         * <p>*********************************************************************************************
         */
        PCollection<PageViewAggregator> pageViewAggregator =
                cleanDataWithOutErrorEvents.apply(new CountViewsPerProduct(Duration.standardSeconds(5)));

        /**
         * *********************************************************************************************
         * Export page view aggregates to BigTable & BigQuery
         *
         * <p>*********************************************************************************************
         */

        //TODO: Commented #1 bigtable sink to save on costs during poc
        //pageViewAggregator.apply(
        //        WriteAggregatesToBigTable.writeToBigTable(Duration.standardSeconds(5)));

        pageViewAggregator.apply(
                WriteAggregationToBigQuery.writeAggregationToBigQuery(
                        "PageView", Duration.standardSeconds(5)));

        return cleanDataWithOutErrorEvents;
    }
}
