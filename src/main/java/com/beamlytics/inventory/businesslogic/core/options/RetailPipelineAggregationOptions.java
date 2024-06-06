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
package com.beamlytics.inventory.businesslogic.core.options;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

// @Experimental
public interface RetailPipelineAggregationOptions extends BigQueryOptions {

  @Default.String("aggregate-tables")
  String getAggregateBigTableInstance();

  void setAggregateBigTableInstance(String aggregateBigTableInstance);

  @Default.String("Retail_Store_Aggregations.StoreStockEvent_10S")
  String getStoreStockAggregateBigQueryTable();

  void setStoreStockAggregateBigQueryTable(String aggregateBigQueryTable);

  @Default.String("Retail_Store_Aggregations.GlobalStockEvent_10S")
  String getGlobalStockAggregateBigQueryTable();

  void setGlobalStockAggregateBigQueryTable(String aggregateBigQueryTable);

  @Default.String("Retail_Store_Aggregations")
  String getAggregateBigQueryTable();

  void setAggregateBigQueryTable(String aggregateBigQueryTable);

  @Description("The fixed window period which aggregations are computed over")
  @Default.Integer(5)
  Integer getAggregationDefaultSec();

  void setAggregationDefaultSec(Integer aggregationDefaultSec);
}
