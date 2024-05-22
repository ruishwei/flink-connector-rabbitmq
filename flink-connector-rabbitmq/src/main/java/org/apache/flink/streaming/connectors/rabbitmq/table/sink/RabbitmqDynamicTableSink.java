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

package org.apache.flink.streaming.connectors.rabbitmq.table.sink;

import org.apache.flink.streaming.connectors.rabbitmq.table.options.RabbitmqOptions;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

public class RabbitmqDynamicTableSink implements DynamicTableSink {
    private final EncodingFormat<SerializationSchema<RowData>> messageEncodingFormat;

    private final ResolvedSchema resolvedSchema;
    private final ReadableConfig config;

    private final RMQConnectionConfig rmqConnectionConfig;

    public RabbitmqDynamicTableSink(
            ResolvedSchema resolvedSchema,
            ReadableConfig config,
            RMQConnectionConfig rmqConnectionConfig,
            EncodingFormat<SerializationSchema<RowData>> messageEncodingFormat) {
        this.resolvedSchema = resolvedSchema;
        this.config = config;
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.messageEncodingFormat = messageEncodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return messageEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> messageSerializationSchema =
                messageEncodingFormat.createRuntimeEncoder(
                        context, resolvedSchema.toPhysicalRowDataType());
        return SinkFunctionProvider.of(new RMQSink<>(
                        rmqConnectionConfig,
                        config.get(RabbitmqOptions.QUEUE_NAME),
                        messageSerializationSchema)
                ,config.get(RabbitmqOptions.PARALLELISM));
    }

    @Override
    public DynamicTableSink copy() {
        return new RabbitmqDynamicTableSink(
                resolvedSchema, config, rmqConnectionConfig, messageEncodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "Rabbitmq table sink";
    }
}
