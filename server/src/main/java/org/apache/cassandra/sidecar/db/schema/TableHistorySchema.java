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

package org.apache.cassandra.sidecar.db.schema;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Schema definition and management for tracking table schema evolution history.
 * <p>
 * This class extends {@link TableSchema} to provide specialized schema management for storing
 * historical versions of table schemas in Cassandra Sidecar. The table schema history tracking
 * is essential for CDC operations and data consistency, enabling:
 * <ul>
 *   <li>Schema version tracking for CDC-enabled tables across time</li>
 *   <li>Historical schema retrieval for data processing and compatibility checks</li>
 *   <li>Schema evolution auditing and change management</li>
 *   <li>Version-aware data processing in CDC pipelines</li>
 * </ul>
 * <p>
 * The table schema is designed with the following characteristics:
 * <ul>
 *   <li><strong>Partitioning:</strong> Data is partitioned by keyspace ({@code ks}) and 
 *       table name ({@code tb}) to organize schemas by table identity</li>
 *   <li><strong>Clustering:</strong> Ordered by schema version ({@code version}) to enable
 *       chronological access to schema changes</li>
 *   <li><strong>Versioning:</strong> Uses UUID-based versioning for unique schema identification</li>
 *   <li><strong>Timestamping:</strong> Automatic creation timestamp tracking with {@code created_at}</li>
 * </ul>
 * <p>
 * The table structure includes:
 * <pre>{@code
 * CREATE TABLE table_schema_history (
 *   ks text,                    -- Keyspace name
 *   tb text,                    -- Table name
 *   version uuid,               -- Schema version identifier
 *   created_at timeuuid,        -- Schema creation timestamp
 *   table_schema text,          -- Complete table schema DDL
 *   PRIMARY KEY ((ks, tb), version)
 * )
 * }</pre>
 * <p>
 * This schema supports CDC operations by maintaining a complete history of table schemas,
 * allowing CDC consumers to process data with the correct schema version that was active
 * when the data was written. This is crucial for maintaining data integrity across schema
 * evolution in long-running CDC pipelines.
 * <p>
 * This class is thread-safe and designed as a singleton for dependency injection into
 * components that require table schema history access.
 *
 * @see TableSchema
 * @see org.apache.cassandra.sidecar.db.CdcDatabaseAccessor
 */
@Singleton
public class TableHistorySchema extends TableSchema
{
    private static final String TABLE_SCHEMA_HISTORY = "table_schema_history";

    private final SchemaKeyspaceConfiguration keyspaceConfig;

    // prepared statements
    private PreparedStatement insertTableSchema;
    private PreparedStatement selectVersionTableSchema;

    @Inject
    public TableHistorySchema(ServiceConfiguration configuration)
    {
        this.keyspaceConfig = configuration.schemaKeyspaceConfiguration();
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        insertTableSchema = prepare(insertTableSchema, session, CqlLiterals.insertTableSchema(keyspaceConfig));
        selectVersionTableSchema = prepare(selectVersionTableSchema, session, CqlLiterals.selectVersionTableSchema(keyspaceConfig));
    }

    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    @Override
    protected String tableName()
    {
        return TABLE_SCHEMA_HISTORY;
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(keyspaceConfig.keyspace());
        if (ksMetadata == null)
        {
            return false;
        }

        return ksMetadata.getTable(TABLE_SCHEMA_HISTORY) != null;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                             "  ks text," +
                             "  tb text," +
                             "  version uuid," +
                             "  created_at timeuuid," +
                             "  table_schema text," +
                             "  PRIMARY KEY ((ks, tb), version)" +
                             ")",
                             keyspaceConfig.keyspace(), TABLE_SCHEMA_HISTORY);
    }

    public PreparedStatement insertTableSchema()
    {
        return insertTableSchema;
    }

    public PreparedStatement selectVersionTableSchema()
    {
        return selectVersionTableSchema;
    }

    private static class CqlLiterals
    {
        static String insertTableSchema(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (ks, tb, version, created_at, table_schema) " +
                             "VALUES (?, ?, ?, NOW(), ?)", config);
        }

        static String selectVersionTableSchema(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT table_schema FROM %s.%s WHERE ks = ? AND tb = ? AND version = ?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), TABLE_SCHEMA_HISTORY);
        }
    }
}
