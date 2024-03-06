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
package com.beamlytics.inventory.businesslogic.core.transforms.transaction;

import javax.annotation.Nullable;

import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

import com.beamlytics.inventory.businesslogic.core.transforms.CreateStockAggregatorMetadata;
import com.beamlytics.inventory.dataobjects.StockAggregation;

// @Experimental
public class CountGlobalStockFromTransaction
    extends PTransform<PCollection<StockAggregation>, PCollection<StockAggregation>> {

  private Duration durationMS;

  public CountGlobalStockFromTransaction(Duration durationMS) {
    this.durationMS = durationMS;
  }

  public CountGlobalStockFromTransaction(@Nullable String name, Duration durationMS) {
    super(name);
    this.durationMS = durationMS;
  }

  @Override
  public PCollection<StockAggregation> expand(PCollection<StockAggregation> input) {
    return input
        .apply("SelectProductId", Select.<StockAggregation>fieldNames("product_id"))
        .apply(
            Group.<Row>byFieldNames("product_id")
                .aggregateField("product_id", Count.combineFn(), "count"))
        .apply("SelectProductCount", Select.fieldNames("key.product_id", "value.count"))
        .apply(
            AddFields.<Row>create()
                .field("store_id", FieldType.INT32)
                .field("durationMS", FieldType.INT64)
                .field("startTime", FieldType.INT64))
        .apply(Convert.to(StockAggregation.class))
        .apply(new CreateStockAggregatorMetadata(durationMS.getMillis()));
  }
}
