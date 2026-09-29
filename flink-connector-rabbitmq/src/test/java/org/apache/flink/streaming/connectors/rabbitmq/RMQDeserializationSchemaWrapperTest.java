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

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests delegation to deserializers that emit zero, one or multiple records per delivery. */
class RMQDeserializationSchemaWrapperTest {

    @Test
    void testCollectorOnlyDeserializer() throws IOException {
        final RMQDeserializationSchemaWrapper<String> wrapper =
                new RMQDeserializationSchemaWrapper<>(new CollectorOnlySchema());
        final List<String> records = new ArrayList<>();
        wrapper.deserialize(null, null, new byte[] {1}, collector(records));
        assertThat(records).containsExactly("before", "after");
        assertThat(wrapper.getProducedType()).isEqualTo(Types.STRING);
    }

    @Test
    void testEmptyDeliveryProducesNoRecord() throws IOException {
        final RMQDeserializationSchemaWrapper<String> wrapper =
                new RMQDeserializationSchemaWrapper<>(new CollectorOnlySchema());
        final List<String> records = new ArrayList<>();
        wrapper.deserialize(null, null, new byte[0], collector(records));
        assertThat(records).isEmpty();
    }

    @Test
    void testDeserializationFailureIsPropagated() {
        final RMQDeserializationSchemaWrapper<String> wrapper =
                new RMQDeserializationSchemaWrapper<>(new CollectorOnlySchema());
        assertThatThrownBy(
                        () ->
                                wrapper.deserialize(
                                        null, null, new byte[] {-1}, collector(new ArrayList<>())))
                .isInstanceOf(IOException.class)
                .hasMessage("Synthetic malformed delivery");
    }

    private static RMQDeserializationSchema.RMQCollector<String> collector(List<String> records) {
        return new RMQDeserializationSchema.RMQCollector<String>() {
            @Override
            public boolean setMessageIdentifiers(String correlationId, long deliveryTag) {
                return false;
            }

            @Override
            public void collect(String record) {
                records.add(record);
            }

            @Override
            public void close() {}
        };
    }

    private static class CollectorOnlySchema implements DeserializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(byte[] message) {
            throw new UnsupportedOperationException("Use the collector overload");
        }

        @Override
        public void deserialize(byte[] message, Collector<String> out) throws IOException {
            if (Arrays.equals(message, new byte[] {-1})) {
                throw new IOException("Synthetic malformed delivery");
            }
            if (message.length > 0) {
                out.collect("before");
                out.collect("after");
            }
        }

        @Override
        public boolean isEndOfStream(String record) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
