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

package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.streaming.connectors.rabbitmq.table.sink.RabbitmqDynamicTableSink;
import org.apache.flink.streaming.connectors.rabbitmq.table.source.RabbitmqDynamicTableSource;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.streaming.connectors.rabbitmq.table.options.RabbitmqOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

public class RabbitmqDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    private static final String IDENTIFIER = "rabbitmq";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig config = helper.getOptions();
        validateSourceOptions(config);
        DecodingFormat<DeserializationSchema<RowData>> messageDecodingFormat =
                getMessageDecodingFormat(helper);
        RMQConnectionConfig rmqConnectionConfig = createRabbitmqConfig(config);

        return new RabbitmqDynamicTableSource(
                context.getCatalogTable().getResolvedSchema(),
                config,
                rmqConnectionConfig,
                messageDecodingFormat);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOST);
        requiredOptions.add(PORT);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        requiredOptions.add(VIRTUAL_HOST);
        requiredOptions.add(QUEUE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(NETWORK_RECOVERY_INTERVAL);
        optionalOptions.add(AUTOMATIC_RECOVERY);
        optionalOptions.add(TOPOLOGY_RECOVERY);
        optionalOptions.add(CONNECTION_TIMEOUT);
        optionalOptions.add(REQUESTED_CHANNEL_MAX);
        optionalOptions.add(REQUESTED_FRAME_MAX);
        optionalOptions.add(REQUESTED_HEARTBEAT);
        optionalOptions.add(DELIVERY_TIMEOUT);
        optionalOptions.add(PREFETCH_COUNT);
        optionalOptions.add(FORMAT);
        optionalOptions.add(USE_CORRELATION_ID);
        return optionalOptions;
    }

    private RMQConnectionConfig createRabbitmqConfig(ReadableConfig config) {
        RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
        builder.setHost(config.get(HOST));
        builder.setPort(config.get(PORT));
        builder.setUserName(config.get(USERNAME));
        builder.setPassword(config.get(PASSWORD));
        builder.setVirtualHost(config.get(VIRTUAL_HOST));

        if (config.get(NETWORK_RECOVERY_INTERVAL) != null) {
            builder.setNetworkRecoveryInterval(config.get(NETWORK_RECOVERY_INTERVAL));
        }
        if (config.get(AUTOMATIC_RECOVERY) != null) {
            builder.setAutomaticRecovery(config.get(AUTOMATIC_RECOVERY));
        }
        if (config.get(TOPOLOGY_RECOVERY) != null) {
            builder.setTopologyRecoveryEnabled(config.get(TOPOLOGY_RECOVERY));
        }
        if (config.get(CONNECTION_TIMEOUT) != null) {
            builder.setConnectionTimeout(config.get(CONNECTION_TIMEOUT));
        }
        if (config.get(REQUESTED_CHANNEL_MAX) != null) {
            builder.setRequestedChannelMax(config.get(REQUESTED_CHANNEL_MAX));
        }
        if (config.get(REQUESTED_FRAME_MAX) != null) {
            builder.setRequestedFrameMax(config.get(REQUESTED_FRAME_MAX));
        }
        if (config.get(REQUESTED_HEARTBEAT) != null) {
            builder.setRequestedHeartbeat(config.get(REQUESTED_HEARTBEAT));
        }
        if (config.get(DELIVERY_TIMEOUT) != null) {
            builder.setDeliveryTimeout(config.get(DELIVERY_TIMEOUT));
        }
        if (config.get(PREFETCH_COUNT) != null) {
            builder.setPrefetchCount(config.get(PREFETCH_COUNT));
        }
        return builder.build();
    }

    private static DecodingFormat<DeserializationSchema<RowData>> getMessageDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);
    }

    private static EncodingFormat<SerializationSchema<RowData>> getMessageEncodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT);
    }

    private void validateSourceOptions(ReadableConfig config) {
        FactoryUtil.validateFactoryOptions(this.requiredOptions(), this.optionalOptions(), config);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig config = helper.getOptions();
        validateSourceOptions(config);
        EncodingFormat<SerializationSchema<RowData>> messageEncodingFormat =
                getMessageEncodingFormat(helper);
        RMQConnectionConfig rmqConnectionConfig = createRabbitmqConfig(config);

        return new RabbitmqDynamicTableSink(
                context.getCatalogTable().getResolvedSchema(),
                config,
                rmqConnectionConfig,
                messageEncodingFormat);
    }
}
