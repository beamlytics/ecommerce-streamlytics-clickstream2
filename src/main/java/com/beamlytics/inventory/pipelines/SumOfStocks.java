package com.beamlytics.inventory.pipelines;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

public class SumOfStocks extends DoFn<KV<Row, Iterable<Long>>, KV<Row, Long>> {


        @ProcessElement
        public void processElement(@Element KV<Row, Iterable<Long>> inputElement, OutputReceiver<KV<Row, Long>> out, @Timestamp Instant instant) {
            Iterable<Long> values = inputElement.getValue();
            Long sum = 0L;
            assert values != null;
            for (Long value : values) {
                sum += value;
            }
            out.outputWithTimestamp(KV.of(inputElement.getKey(), sum), instant);
        }
    }

