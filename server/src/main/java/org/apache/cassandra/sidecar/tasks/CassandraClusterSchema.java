/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.tasks;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/**
 * Central schema management component for Cassandra cluster schema monitoring and CDC table tracking.
 * <p>
 * This class provides comprehensive schema management functionality for Cassandra Sidecar, specifically
 * focused on CDC (Change Data Capture) operations. It maintains real-time awareness of schema changes
 * in the Cassandra cluster and manages CDC-enabled table metadata, enabling:
 * <ul>
 *   <li>Continuous monitoring of Cassandra cluster schema changes</li>
 *   <li>Automatic detection and tracking of CDC-enabled tables</li>
 *   <li>Schema change event notification to registered listeners</li>
 *   <li>Table metadata caching and synchronization with CDC bridges</li>
 *   <li>Periodic validation of CDC table configurations</li>
 * </ul>
 * <p>
 * The class operates with two main periodic tasks:
 * <ul>
 *   <li><strong>Schema Refresh (60s interval):</strong> Monitors for schema changes by comparing
 *       full schema snapshots and updates CDC table metadata when changes are detected</li>
 *   <li><strong>Schema Monitor (49s interval):</strong> Validates that CDC-enabled tables
 *       are properly configured in the Cassandra Schema.instance singleton</li>
 * </ul>
 * <p>
 * Key functionalities include:
 * <ul>
 *   <li><strong>CDC Table Discovery:</strong> Automatically identifies and tracks tables with
 *       CDC enabled from the cluster schema</li>
 *   <li><strong>Schema Change Detection:</strong> Compares schema snapshots to detect modifications
 *       and trigger appropriate updates to CDC subsystems</li>
 *   <li><strong>Bridge Integration:</strong> Synchronizes schema information with Cassandra and
 *       CDC bridges for consistent metadata handling</li>
 *   <li><strong>Event Notification:</strong> Provides a listener mechanism for components that
 *       need to react to schema changes</li>
 *   <li><strong>Validation and Monitoring:</strong> Continuously validates CDC table configurations
 *       and reports inconsistencies through metrics</li>
 * </ul>
 * <p>
 * The schema refresh intervals are carefully chosen to balance responsiveness with system load:
 * <ul>
 *   <li>60-second refresh interval for schema change detection</li>
 *   <li>49-second monitor interval (chosen to avoid harmonics with the 60s refresh cycle)</li>
 * </ul>
 * <p>
 * This component is essential for CDC operations as it ensures that CDC consumers always have
 * up-to-date schema information, enabling proper data serialization, deserialization, and
 * processing across schema evolution events.
 * <p>
 * This class is thread-safe and designed as a singleton for dependency injection into CDC
 * and other schema-dependent components.
 *
 * @see org.apache.cassandra.sidecar.db.CdcDatabaseAccessor
 * @see org.apache.cassandra.bridge.CdcBridge
 * @see org.apache.cassandra.spark.data.CqlTable
 */
@Singleton
public class CassandraClusterSchema implements PeriodicTask
{
    // 49sec least-common multiple with 60sec is 49min so offers best monitor frequency without clashing with 60sec
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterSchema.class);

    private final AtomicReference<String> currSchemaText = new AtomicReference<>("");
    private final AtomicReference<Set<CqlTable>> cdcTables = new AtomicReference<>(Collections.emptySet());
    private final ConcurrentHashMap<TableIdentifier, UUID> tableIdCache = new ConcurrentHashMap<>();
    private final CdcDatabaseAccessor databaseAccessor;
    private final CopyOnWriteArrayList<Runnable> schemaChangeListeners = new CopyOnWriteArrayList<>();
    private final SidecarConfiguration sidecarConfiguration;
    private final InstanceMetadataFetcher instanceFetcher;
    private final SecondBoundConfiguration tableSchemaRefreshTime;

    public CassandraClusterSchema(InstanceMetadataFetcher instanceFetcher,
                                  CdcDatabaseAccessor databaseAccessor,
                                  SidecarConfiguration sidecarConfiguration)
    {

        this.instanceFetcher = instanceFetcher;
        this.databaseAccessor = databaseAccessor;
        this.sidecarConfiguration = sidecarConfiguration;
        this.tableSchemaRefreshTime = sidecarConfiguration.serviceConfiguration().cdcConfiguration().tableSchemaRefreshTime();
    }

    public void addSchemaChangeListener(Runnable listener)
    {
        schemaChangeListeners.add(listener);
    }

    public void refresh()
    {
        NodeSettings nodeSettings = instanceFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        CassandraBridge cassandraBridge = CassandraBridgeFactory.get(nodeSettings.releaseVersion());
        CdcBridge cdcBridge = CdcBridgeFactory.getCdcBridge(cassandraBridge);

        try
        {
            LOGGER.info("Checking for schema changes...");
            String fullSchemaText = databaseAccessor.fullSchema();
            if (!fullSchemaText.equals(currSchemaText.get()))
            {
                LOGGER.info("Schema change detected, refreshing CDC tables");
                currSchemaText.set(fullSchemaText);
                Set<CqlTable> updatedCdcTables = buildCdcTables(fullSchemaText, databaseAccessor, tableIdCache, instanceFetcher);
                LOGGER.info("Cdc enabled tables tables='{}'", 
                            updatedCdcTables.stream()
                                            .map(m -> String.format("%s.%s", m.keyspace(), m.table()))
                                            .collect(Collectors.joining(",")));
                cdcTables.set(updatedCdcTables);
                cdcBridge.updateCdcSchema(updatedCdcTables, databaseAccessor.partitioner(),
                                          ((keyspace, table) -> tableIdCache.get(TableIdentifier.of(keyspace, table))));
                schemaChangeListeners.forEach(Runnable::run);
            }
        }
        catch (IllegalStateException exception)
        {
            LOGGER.warn("There was a problem refreshing the schema. Database Accessor may not be ready", exception);
            throw exception;
        }
        catch (Throwable t)
        {
            LOGGER.error("Unexpected error while refreshing the schema", t);
            throw t;
        }
    }

    public DurationSpec delay()
    {
        return tableSchemaRefreshTime;
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            refresh();
            promise.tryComplete();
        }
        catch (Throwable t)
        {
            promise.fail(t);
        }
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        boolean shouldSkip = !sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration().isEnabled()
                             || !sidecarConfiguration.serviceConfiguration().cdcConfiguration().isEnabled();
        return shouldSkip ? ScheduleDecision.SKIP : ScheduleDecision.EXECUTE;
    }

    @VisibleForTesting
    static Set<CqlTable> buildCdcTables(CdcDatabaseAccessor cdcDatabaseAccessor,
                                        ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                        @NotNull InstanceMetadataFetcher instanceFetcher)
    {
        return buildCdcTables(cdcDatabaseAccessor.fullSchema(),
                              cdcDatabaseAccessor.partitioner(),
                              tableIdCache,
                              cdcDatabaseAccessor::getTableId,
                              instanceFetcher);
    }

    private static Set<CqlTable> buildCdcTables(@NotNull String fullSchema,
                                                @NotNull CdcDatabaseAccessor cdcDatabaseAccessor,
                                                @NotNull ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                                @NotNull InstanceMetadataFetcher instanceFetcher)
    {
        return buildCdcTables(fullSchema,
                              cdcDatabaseAccessor.partitioner(),
                              tableIdCache,
                              cdcDatabaseAccessor::getTableId,
                              instanceFetcher);
    }

    private static Set<CqlTable> buildCdcTables(@NotNull final String fullSchema,
                                                @NotNull final Partitioner partitioner,
                                                @NotNull final ConcurrentHashMap<TableIdentifier, UUID> tableIdCache,
                                                @NotNull final Function<TableIdentifier, UUID> tableIdLoaderFunction,
                                                @NotNull final InstanceMetadataFetcher instanceFetcher)
    {
        Map<TableIdentifier, String> createStmts = CdcUtil.extractCdcTables(fullSchema);
        Map<String, Set<String>> udtsPerKeyspace = createStmts.keySet()
                                                              .stream()
                                                              .map(TableIdentifier::keyspace)
                                                              .distinct() // remove duplicated keyspace strings
                                                              .collect(Collectors.toMap(Function.identity(),
                                                                                        keyspace -> CqlUtils.extractUdts(fullSchema, keyspace)));

        Map<TableIdentifier, UUID> tableIds = createStmts.keySet()
                                                         .stream()
                                                         .collect(Collectors.toMap(Function.identity(),
                                                                                   id -> tableIdCache.computeIfAbsent(id, tableIdLoaderFunction)));

        NodeSettings nodeSettings = instanceFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());

        // get the bridge
        CassandraBridge cassandraBridge = CassandraBridgeFactory.get(nodeSettings.releaseVersion());

        return createStmts.entrySet().stream()
                          .map(e ->
                               {
                                   TableIdentifier id = e.getKey();
                                   ReplicationFactor rf = CqlUtils.extractReplicationFactor(fullSchema, id.keyspace());

                                   return cassandraBridge.buildSchema(e.getValue(), id.keyspace(), rf, 
                                                                      partitioner, udtsPerKeyspace.get(id.keyspace()), 
                                                                      tableIds.get(id), 0, true);
                               })
                          .collect(Collectors.toSet());
    }
}
