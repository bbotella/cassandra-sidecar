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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import org.apache.cassandra.cdc.CdcLogMode;
import org.apache.cassandra.cdc.api.EventConsumer;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.cdc.kafka.KafkaPublisher;
import org.apache.cassandra.cdc.kafka.TopicSupplier;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.sidecar.CdcSidecarInstancesProvider;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.cdc.sidecar.SidecarCdc;
import org.apache.cassandra.cdc.sidecar.SidecarCdcClient;
import org.apache.cassandra.cdc.stats.ICdcStats;
import org.apache.cassandra.secrets.SecretsProvider;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.secrets.SslConfigSecretsProvider;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.coordination.RangeManager;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.db.VirtualTablesDatabaseAccessor;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CACHE_WARMED_UP;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CONFIGURATION_CHANGED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;

/**
 * Class that handles CDC life cycle
 */
@Singleton
public class CdcPublisher implements Handler<Message<Object>>, PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcPublisher.class);
    private static final long INITIALIZATION_LOOP_DELAY_MILLIS = 1000;

    // SSL Configuration Keys
    private static final String SSL_ENABLED_KEY = "enabled";
    private static final String SSL_PREFER_OPENSSL_KEY = "preferOpenSSL";
    private static final String SSL_CLIENT_AUTH_KEY = "clientAuth";
    private static final String SSL_CIPHER_SUITES_KEY = "cipherSuites";
    private static final String SSL_SECURE_TRANSPORT_PROTOCOLS_KEY = "secureTransportProtocols";
    private static final String SSL_HANDSHAKE_TIMEOUT_KEY = "handshakeTimeout";
    private static final String SSL_KEYSTORE_PATH_KEY = "keystorePath";
    private static final String SSL_KEYSTORE_PASSWORD_KEY = "keystorePassword";
    private static final String SSL_KEYSTORE_TYPE_KEY = "keystoreType";
    private static final String SSL_TRUSTSTORE_PATH_KEY = "truststorePath";
    private static final String SSL_TRUSTSTORE_PASSWORD_KEY = "truststorePassword";
    private static final String SSL_TRUSTSTORE_TYPE_KEY = "truststoreType";
    private final TaskExecutorPool executorPools;
    private final CdcConfig conf;
    private volatile boolean isRunning = false;
    private volatile boolean isInitialized = false;
    private volatile boolean cdcCacheWarmedUp = false;
    private final CdcDatabaseAccessor databaseAccessor;
    private final VirtualTablesDatabaseAccessor virtualTables;
    private final SidecarCdcStats sidecarCdcStats;
    private final SchemaSupplier schemaSupplier;
    private final CdcSidecarInstancesProvider sidecarInstancesProvider;
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final ClusterConfigProvider clusterConfigProvider;
    private final SidecarCdcClient.ClientConfig clientConfig;
    private final ICdcStats cdcStats;
    private final SidecarConfiguration sidecarConfiguration;
    private CdcManager cdcManager;
    private Serializer<CdcEvent> avroSerializer;
    private Provider<RangeManager> rangeManagerProvider;

    @Inject
    public CdcPublisher(Vertx vertx,
                        SidecarConfiguration sidecarConfiguration,
                        ExecutorPools executorPools,
                        ClusterConfigProvider clusterConfigProvider,
                        SchemaSupplier schemaSupplier,
                        CdcSidecarInstancesProvider sidecarInstancesProvider,
                        SidecarCdcClient.ClientConfig clientConfig,
                        InstanceMetadataFetcher instanceMetadataFetcher,
                        CdcConfig conf,
                        CdcDatabaseAccessor databaseAccessor,
                        ICdcStats cdcStats,
                        VirtualTablesDatabaseAccessor virtualTables,
                        SidecarCdcStats sidecarCdcStats,
                        Serializer<CdcEvent> avroSerializer,
                        Provider<RangeManager> rangeManagerProvider)
    {
        this.sidecarCdcStats = sidecarCdcStats;
        this.executorPools = executorPools.internal();
        this.conf = conf;
        this.databaseAccessor = databaseAccessor;
        this.virtualTables = virtualTables;

        this.schemaSupplier = schemaSupplier;
        this.sidecarInstancesProvider = sidecarInstancesProvider;
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.clusterConfigProvider = clusterConfigProvider;
        this.clientConfig = clientConfig;
        this.cdcStats = cdcStats;
        this.sidecarConfiguration = sidecarConfiguration;
        this.avroSerializer = avroSerializer;
        this.rangeManagerProvider = rangeManagerProvider;
        vertx.eventBus().localConsumer(RangeManager.RangeManagerEvents.ON_TOKEN_RANGE_CHANGED.address(), this);
        vertx.eventBus().localConsumer(RangeManager.LeadershipEvents.ON_TOKEN_RANGE_GAINED.address(), this);
        vertx.eventBus().localConsumer(RangeManager.LeadershipEvents.ON_TOKEN_RANGE_LOST.address(), this);
        vertx.eventBus().localConsumer(ON_SERVER_STOP.address(), this);
        vertx.eventBus().localConsumer(ON_CDC_CACHE_WARMED_UP.address(), this);
        vertx.eventBus().localConsumer(ON_CDC_CONFIGURATION_CHANGED.address(), new ConfigChangedHandler());
    }

    public SecretsProvider secretsProvider()
    {
        SslConfiguration sslConfiguration = sidecarConfiguration.sidecarClientConfiguration().sslConfiguration();

        if (sslConfiguration == null || !sslConfiguration.enabled())
        {
            return null;
        }

        Map<String, String> sslConfigMap = new HashMap<>();

        sslConfigMap.put(SSL_ENABLED_KEY, sslConfiguration.enabled() + "");
        sslConfigMap.put(SSL_PREFER_OPENSSL_KEY, sslConfiguration.preferOpenSSL() + "");
        sslConfigMap.put(SSL_CLIENT_AUTH_KEY, sslConfiguration.clientAuth());
        sslConfigMap.put(SSL_CIPHER_SUITES_KEY, String.join(",", sslConfiguration.cipherSuites()));
        sslConfigMap.put(SSL_SECURE_TRANSPORT_PROTOCOLS_KEY, String.join(",", sslConfiguration.secureTransportProtocols()));
        sslConfigMap.put(SSL_HANDSHAKE_TIMEOUT_KEY, sslConfiguration.handshakeTimeout().toString());

        if (sslConfiguration.isKeystoreConfigured())
        {
            KeyStoreConfiguration keystore = sslConfiguration.keystore();
            sslConfigMap.put(SSL_KEYSTORE_PATH_KEY, keystore.path());
            sslConfigMap.put(SSL_KEYSTORE_PASSWORD_KEY, keystore.password());
            sslConfigMap.put(SSL_KEYSTORE_TYPE_KEY, keystore.type());
        }

        if (sslConfiguration.isTrustStoreConfigured())
        {
            KeyStoreConfiguration truststore = sslConfiguration.truststore();
            sslConfigMap.put(SSL_TRUSTSTORE_PATH_KEY, truststore.path());
            sslConfigMap.put(SSL_TRUSTSTORE_PASSWORD_KEY, truststore.password());
            sslConfigMap.put(SSL_TRUSTSTORE_TYPE_KEY, truststore.type());
        }

        SslConfig sslConfig = SslConfig.create(sslConfigMap);
        return new SslConfigSecretsProvider(sslConfig);
    }

    public EventConsumer eventConsumer(CdcConfig conf,
                                       Serializer<CdcEvent> avroSerializer)
    {
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(conf.kafkaConfigs());
        KafkaPublisher kafkaPublisher = new KafkaPublisher(TopicSupplier.staticTopicSupplier(conf.kafkaTopic()),
                                                           producer,
                                                           avroSerializer,
                                                           conf.maxRecordSizeBytes(),
                                                           conf.failOnRecordTooLargeError(),
                                                           conf.failOnKafkaError(),
                                                           CdcLogMode.FULL);
        return new CdcEventConsumer(kafkaPublisher);
    }

    private class ConfigChangedHandler implements Handler<Message<Object>>
    {
        public void handle(Message<Object> event)
        {
            sidecarCdcStats.captureCdcConfigChange();
            // Execute restart on worker thread to avoid blocking event loop
            executorPools.executeBlocking(() -> {
                restart();
                return null;
            });
        }
    }

    @SuppressWarnings("resource")
    private synchronized void run() throws IllegalStateException
    {
        if (isRunning)
        {
            return;
        }
        databaseAccessor.session(); // throws IllegalStateException if session unavailable

        cdcManager = new CdcManager(eventConsumer(conf, avroSerializer),
                                    schemaSupplier,
                                    conf,
                                    rangeManagerProvider.get(),
                                    instanceMetadataFetcher,
                                    clusterConfigProvider,
                                    sidecarInstancesProvider,
                                    secretsProvider(),
                                    clientConfig,
                                    cdcStats,
                                    this.executorPools,
                                    databaseAccessor);

        List<SidecarCdc> consumers = cdcManager.buildCdcConsumers();
        cdcManager.startConsumers();
        LOGGER.info("{} CDC iterators started successfully", consumers.size());
        isRunning = true;
        sidecarCdcStats.captureCdcStarted(consumers.size());
    }

    protected synchronized void restart()
    {
        try
        {
            stop();
            initialize();

            LOGGER.info("Iterators restarted.");
            sidecarCdcStats.captureCdcRestart();
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to restart iterators", e);
            sidecarCdcStats.captureCdcStartFailure(e);
        }
    }

    public boolean isRunning()
    {
        return isRunning;
    }

    public synchronized void stop()
    {
        if (!isRunning)
        {
            return;
        }

        try
        {
            cdcManager.stopConsumers();
            sidecarCdcStats.captureCdcStopped();
        }
        catch (Throwable t)
        {
            LOGGER.error("Failed to gracefully shutdown CDC", t);
            sidecarCdcStats.captureCdcStopFailed(t);
        }
        finally
        {
            isRunning = false;
            isInitialized = false;
        }
    }

    private void initialize()
    {
        try
        {
            String localDc = rangeManagerProvider.get().getLocalDcSafe();
            if (conf.datacenter() != null && !conf.datacenter().isEmpty() && !conf.datacenter().equals(localDc))
            {
                LOGGER.info("Cdc not enabled in this DC localDc={} cdcDc={}", localDc, conf.datacenter());
            }
            else if (virtualTables.isCdcOnRepairEnabled())
            {
                LOGGER.warn("Cannot run CDC while cdc on repair is enabled, disable cdc_on_repair_enabled in the yaml file.");
                sidecarCdcStats.captureCdcOnRepairEnabled();
            }
            else if (conf.cdcEnabled())
            {
                try
                {
                    LOGGER.info("Initialization of all delegates complete, attempting to start CDC");
                    isInitialized = true;
                }
                catch (Throwable t)
                {
                    LOGGER.error("Error initializing CDC", t);
                    sidecarCdcStats.captureCdcStartFailure(t);
                }
            }

        }
        catch (Exception e)
        {
            LOGGER.error("Unexpected error initializing CdcPublisher", e);
            sidecarCdcStats.captureCdcStartFailure(e);
        }

    }

    // EventBus handlers
    @Override
    public synchronized void handle(Message<Object> msg)
    {
        if (msg.address().equals(RangeManager.RangeManagerEvents.ON_TOKEN_RANGE_CHANGED.address()))
        {
            handleTokenRangeChange();
        }
        else if (msg.address().equals(RangeManager.LeadershipEvents.ON_TOKEN_RANGE_GAINED.address()))
        {
            handleRangeGained((RangeManager.RangeChangeEvent) msg.body());
        }
        else if (msg.address().equals(RangeManager.LeadershipEvents.ON_TOKEN_RANGE_LOST.address()))
        {
            handleRangeLost((RangeManager.RangeChangeEvent) msg.body());
        }
        else if (msg.address().equals(ON_SERVER_STOP.address()))
        {
            stop();
        }
        else if (msg.address().equals(ON_CDC_CACHE_WARMED_UP.address()))
        {
            cdcCacheWarmedUp = true;
        }
    }

    protected synchronized void handleTokenRangeChange()
    {
        if (isRunning)
        {
            //TODO: detect if topology change affects active cdc iterators
            LOGGER.info("Token Range changed, probably due to a change in cluster topology, restarting iterators");
            sidecarCdcStats.captureCdcClusterTopologyChange();
            restart();
        }
    }

    protected synchronized void handleRangeGained(RangeManager.RangeChangeEvent event)
    {
        if (isRunning)
        {
            //TODO: start/restart consumers based on ranges gained in event
            sidecarCdcStats.captureCdcClusterTopologyChange();
            restart();
        }
    }

    protected synchronized void handleRangeLost(RangeManager.RangeChangeEvent event)
    {
        if (isRunning)
        {
            //TODO: stop/restart consumers based on ranges lost in event
            sidecarCdcStats.captureCdcTokenRangeLost();
            restart();
        }
    }

    public DurationSpec delay()
    {
        return MillisecondBoundConfiguration.parse(INITIALIZATION_LOOP_DELAY_MILLIS + "ms");
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        run();
        promise.complete();
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        return isInitialized && cdcCacheWarmedUp
               ? ScheduleDecision.EXECUTE
               : ScheduleDecision.SKIP;
    }
}
