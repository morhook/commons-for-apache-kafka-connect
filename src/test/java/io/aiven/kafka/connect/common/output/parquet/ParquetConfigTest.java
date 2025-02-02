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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.CompressionType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetConfigTest {

    @Test
    void testGenerateParquetConfig() {

        final Map<String, String> origins = new HashMap<String, String>() {{
                put("connect.parquet.aa", "aa");
                put("connect.parquet.bb", "bb");
                put("connect.parquet.cc", "cc");
                put("connect.parquet.avro.schema", "aa");
            }};

        final ParquetConfig parquetConfig = new ParquetConfig(origins);
        final Configuration config = parquetConfig.parquetConfiguration();

        assertThat(config.get("parquet.aa")).isEqualTo("aa");
        assertThat(config.get("parquet.bb")).isEqualTo("bb");
        assertThat(config.get("parquet.cc")).isEqualTo("cc");
        assertThat(config.get("parquet.avro.schema")).isNull();
    }

    @Test
    void testConvertCompressionTypeToParquetCompressorName() {
        assertThat(
                new ParquetConfig(
                    new HashMap<String, String>() {{
                        put(AivenCommonConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.NONE.name);
                    }}
                ).compressionCodecName())
            .isEqualTo(CompressionCodecName.UNCOMPRESSED);
        assertThat(new ParquetConfig(Collections.emptyMap()).compressionCodecName())
            .isEqualTo(CompressionCodecName.UNCOMPRESSED);
        assertThat(
                new ParquetConfig(
                    new HashMap<String, String>() {{
                        put(AivenCommonConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
                    }}
                ).compressionCodecName())
            .isEqualTo(CompressionCodecName.ZSTD);
    }
}
