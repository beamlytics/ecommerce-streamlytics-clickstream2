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
package com.beamlytics.inventory.businesslogic.core.transforms;

import com.beamlytics.inventory.dataobjects.StockAggregation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import javax.annotation.Nullable;

/**
 * Adds metadata to the stock aggregation, for use in downstream systems. The start time of the
 * aggregation, derived from the Window is added as well as the duration, which is passed in as a
 * parameter. This allows for downstream reporting systems to query aggregate values using time
 * boundaries.
 */
// @Experimental
public class CreateStockAggregatorMetadata
    extends PTransform<PCollection<StockAggregation>, PCollection<StockAggregation>> {

  private Long durationMS;

  public static CreateStockAggregatorMetadata create(Long durationMS) {
    return new CreateStockAggregatorMetadata(durationMS);
  }

  public CreateStockAggregatorMetadata(Long durationMS) {
    this.durationMS = durationMS;
  }

  public CreateStockAggregatorMetadata(@Nullable String name, Long durationMS) {
    super(name);
    this.durationMS = durationMS;
  }

  @Override
  public PCollection<StockAggregation> expand(PCollection<StockAggregation> input) {

    return input.apply(
        ParDo.of(
            new DoFn<StockAggregation, StockAggregation>() {
              @ProcessElement
              public void process(
                  @Element StockAggregation input,
                  @Timestamp Instant time,
                  OutputReceiver<StockAggregation> o) {
                o.output(
                    input.toBuilder()
                        .setDurationMS(durationMS)
                        .setStartTime(time.getMillis() - durationMS + 1)
                        .build());
              }
            }));
  }
}
