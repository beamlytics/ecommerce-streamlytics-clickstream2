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
package com.beamlytics.inventory.core.transform.clickstream;

import com.beamlytics.inventory.businesslogic.core.options.RetailPipelineOptions;
import com.beamlytics.inventory.dataobjects.ClickStream.ClickStreamBigTableSchema;
import com.beamlytics.inventory.dataobjects.ClickStream.PageViewAggregator;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.common.primitives.Longs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.http.annotation.Experimental;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Optional;

@Experimental
public class WriteAggregatesToBigTable {

  public static WriteToBigTable writeToBigTable(Duration windowSize) {
    return new WriteToBigTable(windowSize);
  }

  public static class WriteToBigTable extends PTransform<PCollection<PageViewAggregator>, PDone> {

    Duration windowSize;

    public WriteToBigTable(Duration windowSize) {
      this.windowSize = windowSize;
    }

    public WriteToBigTable(@Nullable String name, Duration windowSize) {
      super(name);
      this.windowSize = windowSize;
    }

    @Override
    public PDone expand(PCollection<PageViewAggregator> input) {

      RetailPipelineOptions options =
          input.getPipeline().getOptions().as(RetailPipelineOptions.class);

      CloudBigtableTableConfiguration tableConfiguration =
          new CloudBigtableTableConfiguration.Builder()
              .withProjectId(options.getProject())
              .withInstanceId(options.getAggregateBigTableInstance())
              .withTableId("PageView5MinAggregates")
              .build();

      PCollection<Mutation> mutations =
          input.apply(
              "Convert Aggregation to BigTable Put",
              ParDo.of(new CreateBigTableRowFromAggregation()));

      if (options.getTestModeEnabled()) {
        mutations.apply(ParDo.of(new PrintMutation()));
        return PDone.in(input.getPipeline());
      }

      return mutations.apply(CloudBigtableIO.writeToTable(tableConfiguration));
    }
  }

  public static class CreateBigTableRowFromAggregation extends DoFn<PageViewAggregator, Mutation> {
    @ProcessElement
    public void process(@Element PageViewAggregator input, OutputReceiver<Mutation> o)
        throws UnsupportedEncodingException {

      Put put =
          new Put(String.format("%s-%s", input.getPage(), input.getStartTime()).getBytes("UTF-8"));

      String charset = "UTF-8";

      // TODO This should never be Null eliminate bug.
      String pageRef = Optional.ofNullable(input.getPage()).orElse("");
      Long count = Optional.ofNullable(input.getCount()).orElse(0L);

      put.addColumn(
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_FAMILY.getBytes(charset),
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_PAGE_VIEW_REF.getBytes(charset),
          pageRef.getBytes(charset));

      put.addColumn(
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_FAMILY.getBytes(charset),
          ClickStreamBigTableSchema.PAGE_VIEW_AGGREGATION_COL_PAGE_VIEW_COUNT.getBytes(charset),
          Longs.toByteArray(count));

      o.output(put);
    }
  }

  public static class PrintMutation extends DoFn<Mutation, Mutation> {

    private static final Logger LOG = LoggerFactory.getLogger(PrintMutation.class);

    @ProcessElement
    public void process(@Element Mutation mutation) throws IOException {

      LOG.info("Mutation to BigTable is " + mutation.toJSON());
    }
  }
}
