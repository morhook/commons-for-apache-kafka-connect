/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.output.parquet;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldType;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SinkRecordConverter {

    private final Logger logger = LoggerFactory.getLogger(SinkRecordConverter.class);

    private final Collection<OutputField> fields;

    private final AvroData avroData;

    private final boolean envelopeEnabled;

    SinkRecordConverter(final Collection<OutputField> fields, final AvroData avroData, final boolean envelopeEnabled) {
        this.fields = fields;
        this.avroData = avroData;
        this.envelopeEnabled = envelopeEnabled;
    }

    SinkRecordConverter(final Collection<OutputField> fields, final AvroData avroData) {
        this.fields = fields;
        this.avroData = avroData;
        this.envelopeEnabled = true;
    }

    public GenericRecord convert(final SinkRecord record, final Schema schema) {
        logger.debug("Convert record {} for schema {}", record, schema);
        return createRecord(schema, record);
    }

    private GenericRecord createRecord(final Schema schema, final SinkRecord record) {
        if (envelopeEnabled) {
            final GenericData.Record avroRecord = new GenericData.Record(schema);
            for (final OutputField f : fields) {
                final Object fieldValue = getRecordValueFor(f.getFieldType(), record);
                avroRecord.put(f.getFieldType().name, fieldValue);
            }
            return avroRecord;
        } else {
            return tryUnwrapEnvelope(schema, record);
        }
    }

    private GenericData.Record tryUnwrapEnvelope(final Schema schema, final SinkRecord record) {
        // envelope can be disabled only in case of single field
        final OutputField field = fields.iterator().next();
        final Object fieldValue = getRecordValueFor(field.getFieldType(), record);
        final Schema.Type originalValueSchemaType = avroData.fromConnectSchema(record.valueSchema()).getType();
        if (originalValueSchemaType == Schema.Type.MAP) {
            @SuppressWarnings("unchecked") final Set<Map.Entry<String, Object>> entries =
                ((Map<String, Object>) fieldValue).entrySet();
            final GenericData.Record avroRecord = new GenericData.Record(schema);
            for (final Map.Entry<String, Object> entry : entries) {
                avroRecord.put(entry.getKey(), entry.getValue());
            }
            return avroRecord;
        } else if (originalValueSchemaType == Schema.Type.RECORD) {
            return (GenericData.Record) fieldValue;
        } else {
            final GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put(field.getFieldType().name, fieldValue);
            return avroRecord;
        }
    }


    private Object getRecordValueFor(final OutputFieldType fieldType, final SinkRecord record) {
        switch (fieldType) {
            case KEY:
                return fromConnectData(record.keySchema(), record.key());
            case VALUE:
                return fromConnectData(record.valueSchema(), record.value());
            case OFFSET:
                return record.kafkaOffset();
            case TIMESTAMP:
                return record.timestamp();
            case HEADERS:
                final Map<String, Object> headers = new HashMap<String, Object>();
                for (final Header h : record.headers()) {
                    final String k = h.key();
                    final Object v = fromConnectData(h.schema(), h.value());
                    headers.put(k, v);
                }
                return headers;
            default:
                throw new ConnectException("Unsupported output field: " + fieldType);
        }
    }

    private Object fromConnectData(final org.apache.kafka.connect.data.Schema schema, final Object value) {
        final Object avroDataValue = avroData.fromConnectData(schema, value);
        if (avroDataValue instanceof NonRecordContainer) {
            return ((NonRecordContainer) avroDataValue).getValue();
        } else {
            return avroDataValue;
        }
    }


}
