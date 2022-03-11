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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetOutputWriterTest {

    @Test
    void testWriteAllFields(@TempDir final Path tmpDir) throws IOException {
        final Path parquetFile = tmpDir.resolve("parquet.file");
        final List<String> values = Arrays.asList("a", "b", "c", "d");
        writeRecords(
                parquetFile,
                Arrays.asList(
                        new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)
                ),
                SchemaBuilder.STRING_SCHEMA,
                values,
                true,
                true
        );
        int counter = 0;
        final int timestamp = 1000;
        for (final String r : readRecords(parquetFile)) {
            final String expectedString =
                    "{\"key\": \"some-key-" + counter + "\", "
                            + "\"offset\": 100, "
                            + "\"timestamp\": "
                            + (timestamp + counter) + ", "
                            + "\"headers\": "
                            + "{\"a\": {\"bytes\": \"b\"}, \"c\": {\"bytes\": \"d\"}}, "
                            + "\"value\": \"" + values.get(counter) + "\"}";
            assertThat(r).isEqualTo(expectedString);
            counter++;
        }
    }

    @Test
    void testWritePartialFields(@TempDir final Path tmpDir) throws IOException {
        final Path parquetFile = tmpDir.resolve("parquet.file");
        final List<String> values = Arrays.asList("a", "b", "c", "d");
        writeRecords(
                parquetFile,
                Arrays.asList(
                        new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                        new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)
                ),
                SchemaBuilder.STRING_SCHEMA,
                values,
                false,
                true
        );
        int counter = 0;
        for (final String r : readRecords(parquetFile)) {
            final String expectedString =
                    "{\"key\": \"some-key-" + counter + "\", "
                            + "\"value\": \"" + values.get(counter) + "\"}";
            assertThat(r).isEqualTo(expectedString);
            counter++;
        }
    }

    @Test
    void testWriteValueStruct(@TempDir final Path tmpDir) throws IOException {
        final Path parquetFile = tmpDir.resolve("parquet.file");
        final Schema recordSchema =
                SchemaBuilder.struct()
                    .field("name", Schema.STRING_SCHEMA)
                    .field("age", Schema.INT32_SCHEMA)
                .build();

        final List<Struct> values =
                Arrays.asList(
                        new Struct(recordSchema)
                                .put("name", "name-0").put("age", 0),
                        new Struct(recordSchema)
                                .put("name", "name-1").put("age", 1),
                        new Struct(recordSchema)
                                .put("name", "name-2").put("age", 2),
                        new Struct(recordSchema)
                                .put("name", "name-3").put("age", 3)
                );
        writeRecords(
                parquetFile,
                Arrays.asList(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)),
                recordSchema,
                values,
                false,
                true
        );
        int counter = 0;
        for (final String r : readRecords(parquetFile)) {
            final String expectedString = "{\"value\": {\"name\": \"name-" + counter + "\", \"age\": " + counter + "}}";
            assertThat(r).isEqualTo(expectedString);
            counter++;
        }
    }

    @Test
    void testWriteValueStructWithoutEnvelope(@TempDir final Path tmpDir) throws IOException {
        final Path parquetFile = tmpDir.resolve("parquet.file");
        final Schema recordSchema =
                SchemaBuilder.struct()
                        .field("name", Schema.STRING_SCHEMA)
                        .field("age", Schema.INT32_SCHEMA)
                        .build();

        final List<Struct> values =
                Arrays.asList(
                        new Struct(recordSchema)
                                .put("name", "name-0").put("age", 0),
                        new Struct(recordSchema)
                                .put("name", "name-1").put("age", 1),
                        new Struct(recordSchema)
                                .put("name", "name-2").put("age", 2),
                        new Struct(recordSchema)
                                .put("name", "name-3").put("age", 3)
                );
        writeRecords(
                parquetFile,
                Arrays.asList(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)),
                recordSchema,
                values,
                false,
                false
        );

        final List<String> actualRecords = readRecords(parquetFile);
        assertThat(actualRecords).hasSameSizeAs(values)
            .containsExactlyElementsOf(
                    values.stream()
                            .map(struct -> "{\"name\": \"" + struct.get("name") + "\","
                                    + " \"age\": " + struct.get("age") + "}")
                            .collect(Collectors.toList()));
    }

    @Test
    void testWriteValueArray(@TempDir final Path tmpDir) throws IOException {
        final Path parquetFile = tmpDir.resolve("parquet.file");
        final Schema recordSchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();

        final List<List<Integer>> values =
                Arrays.asList(
                        Arrays.asList(1, 2, 3, 4),
                        Arrays.asList(5, 6, 7, 8),
                        Arrays.asList(9, 10)
                );
        writeRecords(
                parquetFile,
                Arrays.asList(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)),
                recordSchema,
                values,
                false,
                true
        );
        int counter = 0;
        for (final String r : readRecords(parquetFile)) {
            final String expectedString = "{\"value\": " + values.get(counter) + "}";
            assertThat(r).isEqualTo(expectedString);
            counter++;
        }
    }

    @Test
    void testWriteValueMap(@TempDir final Path tmpDir) throws IOException {
        final Path parquetFile = tmpDir.resolve("parquet.file");
        final Schema recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

        writeRecords(
                parquetFile,
                Arrays.asList(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)),
                recordSchema,
                Arrays.asList(new HashMap<String, Integer>() {{
                        put("a", 1);
                        put("b", 2);
                    }}
                ),
                false,
                true
        );
        for (final String r : readRecords(parquetFile)) {
            final String mapValue =  "{\"a\": 1, \"b\": 2}";
            final String expectedString = "{\"value\": " + mapValue + "}";
            assertThat(r).isEqualTo(expectedString);
        }
    }

    @Test
    void testWriteValueMapWithoutEnvelope(@TempDir final Path tmpDir) throws IOException {
        final Path parquetFile = tmpDir.resolve("parquet.file");
        final Schema recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

        writeRecords(
                parquetFile,
                Arrays.asList(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE)),
                recordSchema,
                Arrays.asList(new HashMap<String, Integer>() {{
                        put("a", 1);
                        put("b", 2);
                    }}
                ),
                false,
                false
        );

        final String expectedString = "{\"a\": 1, \"b\": 2}";
        assertThat(readRecords(parquetFile)).containsExactly(expectedString);
    }

    private <T> void writeRecords(final Path parquetFile,
                                  final Collection<OutputField> fields,
                                  final Schema recordSchema,
                                  final List<T> records,
                                  final boolean withHeaders,
                                  final boolean withEnvelope) throws IOException {
        final OutputStream out = new FileOutputStream(parquetFile.toFile());
        final Headers headers = new ConnectHeaders();
        headers.add("a", "b".getBytes(StandardCharsets.UTF_8), Schema.BYTES_SCHEMA);
        headers.add("c", "d".getBytes(StandardCharsets.UTF_8), Schema.BYTES_SCHEMA);
        try (final OutputStream o = out;
             final ParquetOutputWriter parquetWriter = 
                 new ParquetOutputWriter(fields, o, Collections.emptyMap(), withEnvelope)) {
            int counter = 0;
            final List<SinkRecord> sinkRecords = new ArrayList<SinkRecord>();
            for (final T r : records) {
                final SinkRecord sinkRecord =
                        new SinkRecord(
                                "some-topic", 1,
                                Schema.STRING_SCHEMA, "some-key-" + counter,
                                recordSchema, r,
                                100L, 1000L + counter,
                                TimestampType.CREATE_TIME,
                                withHeaders ? headers : null);
                sinkRecords.add(sinkRecord);
                counter++;
            }
            parquetWriter.writeRecords(sinkRecords);
        }

    }

    private List<String> readRecords(final Path parquetFile) throws IOException {
        final ParquetInputFile inputFile = new ParquetInputFile(parquetFile);
        final List<String> records = new ArrayList<String>();
        try (final ParquetReader<Object> reader =
                     AvroParquetReader.builder(inputFile)
                             .withCompatibility(false)
                             .build()) {
            Object r = reader.read();
            while (r != null) {
                records.add(r.toString());
                r = reader.read();
            }
        }
        return records;
    }

    static class ParquetInputFile implements InputFile {

        final SeekableByteChannel seekableByteChannel;

        ParquetInputFile(final Path tmpFilePath) throws IOException {
            this.seekableByteChannel = Files.newByteChannel(tmpFilePath, StandardOpenOption.READ);
        }

        @Override
        public long getLength() throws IOException {
            return seekableByteChannel.size();
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {
                @Override
                public long getPos() throws IOException {
                    return seekableByteChannel.position();
                }

                @Override
                public void seek(final long newPosition) throws IOException {
                    seekableByteChannel.position(newPosition);
                }
            };
        }
    }

}
