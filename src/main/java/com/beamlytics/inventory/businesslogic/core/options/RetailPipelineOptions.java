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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
//import org.apache.beam.sdk.options.Default;

// @Experimental
public interface RetailPipelineOptions
    extends 
        DataflowPipelineOptions,
        RetailPipelineAggregationOptions,
        RetailPipelineClickStreamOptions,
        RetailPipelineInventoryOptions,
        RetailPipelineTransactionsOptions,
        RetailPipelineStoresOptions,
        RetailPipelineReportingOptions {

  @Default.Boolean(false)
  Boolean getDebugMode();

  void setDebugMode(Boolean debugMode);

  @Default.Boolean(false)
  Boolean getTestModeEnabled();

  void setTestModeEnabled(Boolean testModeEnabled);

  /**
   * Memorystore/Redis instance host. Update with a running memorystore instance in the command-line to execute the pipeline
   */
  @Description("Redis host")
  @Default.String("localhost")
  String getRedisHost();
  void setRedisHost(String value);

  @Description("Redis auth")
  @Default.String("f2b04bcd-8974-41e1-aedf-85fb286bd9b0")
  String getRedisAuth();
  void setRedisAuth(String value);

  /**
   * Memorystore/Redis instance port. The default port for Redis is 6379
   */
  @Description("Redis port")
  @Default.Integer(6379)
  Integer getRedisPort();
  void setRedisPort(Integer value);




}
