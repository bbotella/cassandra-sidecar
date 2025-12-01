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

package org.apache.cassandra.sidecar.cdc;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.codecs.CdcConfigMappingsCodec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.db.CdcConfigAccessor;
import org.apache.cassandra.sidecar.tasks.CdcConfigRefresherNotifierTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CONFIG_MAPPINGS_CHANGED;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CdcConfigImplTest
{
    private static final Vertx vertx = Vertx.vertx();
    private static final ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
    private static final ClusterLease clusterLease = new ClusterLease();
    private PeriodicTaskExecutor executor = new PeriodicTaskExecutor(executorPools, clusterLease);

    @BeforeEach
    void beforeEach()
    {
        executor = new PeriodicTaskExecutor(executorPools, clusterLease);
    }

    @AfterEach
    void afterEach()
    {
        clusterLease.setOwnershipTesting(ClusterLease.Ownership.INDETERMINATE);
        executor.close(Promise.promise());
    }

    @AfterAll
    static void teardown()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testIsConfigReadySchemaNotInitialized()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        when(cdcConfigAccessor.isAvailable()).thenReturn(false);

        CdcConfigImpl cdcConfig = new CdcConfigImpl(vertx, cdcConfigAccessor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testIsConfigReadyKafkaConfigsEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("k1", "v1"));
        CdcConfigImpl cdcConfig = new CdcConfigImpl(vertx, cdcConfigAccessor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testIsConfigReadyCdcConfigsEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("k1", "v1"));
        CdcConfigImpl cdcConfig = new CdcConfigImpl(vertx, cdcConfigAccessor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testReturnDefaultValuesWhenConfigsAreEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        CdcConfigImpl cdcConfig = new CdcConfigImpl(vertx, cdcConfigAccessor);
        assertThat(cdcConfig.datacenter()).isEqualTo(null);
        assertThat(cdcConfig.env()).isEqualTo("");
        assertThat(cdcConfig.kafkaTopic()).isNull();
        assertThat(cdcConfig.logOnly()).isFalse();
        assertThat(cdcConfig.watermarkWindow()).isEqualTo(new SecondBoundConfiguration(72, TimeUnit.HOURS));
        assertThat(cdcConfig.persistDelay()).isEqualTo(new MillisecondBoundConfiguration(1, TimeUnit.SECONDS));
    }

    @Test
    void testConfigsWhenConfigsAreNotEmpty() throws InterruptedException
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        CdcConfigRefresherNotifierTask.ConfigMappings configMappings = new CdcConfigRefresherNotifierTask.ConfigMappings();
        configMappings.setKafkaConfigMappings(Map.of("k1", "v1"));
        configMappings.setCdcConfigMappings(Map.of("datacenter", "DC1",
                                                   "env", "if",
                                                   "log_only", "false",
                                                   "topic", "topic1",
                                                   "watermark_seconds", "120",
                                                   "persist_delay_millis", "5000"));

        CdcConfigImpl cdcConfig = new CdcConfigImpl(vertx, cdcConfigAccessor);
        vertx.eventBus().registerDefaultCodec(CdcConfigRefresherNotifierTask.ConfigMappings.class, CdcConfigMappingsCodec.INSTANCE);
        vertx.eventBus().publish(ON_CDC_CONFIG_MAPPINGS_CHANGED.address(), configMappings);

        loopAssert(5, ()-> assertThat(cdcConfig.isConfigReady()).isTrue());
        assertThat(cdcConfig.datacenter()).isEqualTo("DC1");
        assertThat(cdcConfig.env()).isEqualTo("if");
        assertThat(cdcConfig.kafkaTopic()).isEqualTo("topic1");
        assertThat(cdcConfig.logOnly()).isFalse();
        assertThat(cdcConfig.watermarkWindow()).isEqualTo(new SecondBoundConfiguration(2, TimeUnit.MINUTES));
        assertThat(cdcConfig.persistDelay()).isEqualTo(new MillisecondBoundConfiguration(5, TimeUnit.SECONDS));
    }

    private CdcConfigAccessor mockCdcConfigAccessor()
    {
        CdcConfigAccessor cdcConfigAccessor = mock(CdcConfigAccessor.class, RETURNS_DEEP_STUBS);
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of());
        when(cdcConfigAccessor.isAvailable()).thenReturn(true);
        return cdcConfigAccessor;
    }
}
