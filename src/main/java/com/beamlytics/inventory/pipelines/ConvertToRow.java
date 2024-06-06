package com.beamlytics.inventory.pipelines;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;

public class ConvertToRow extends DoFn<KV<Row, Long>,Row> {

    Schema totalsRowSchema;
    public ConvertToRow(Schema totalsRowSchema) {
        this.totalsRowSchema=totalsRowSchema;
    }

    @ProcessElement
    public void processElement(@Element KV<Row,Long> inputElement,
                               OutputReceiver<Row> out,
                               @Timestamp Instant instant)
    {
        Row inputRow = inputElement.getKey();
        Long count = inputElement.getValue();
        Integer storeId;
        Integer productId;
        Row.Builder outputRowBuilder = Row.withSchema(this.totalsRowSchema);
        Row outputRow;
        assert inputRow != null;
        storeId=inputRow.getValue("store_id");
        productId=inputRow.getValue("product_id");
        DateTime dateTimeForRecord = instant.toDateTime();
        outputRow = outputRowBuilder.addValue(productId).addValue(storeId).addValue(count).addValue(dateTimeForRecord).build();
        out.outputWithTimestamp(outputRow,instant);
    }
}
