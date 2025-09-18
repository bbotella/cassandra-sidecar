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

package org.apache.cassandra.sidecar.db;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.db.schema.TableHistorySchema;

/**
 * Database accessor for Table History operations.
 */
@SuppressWarnings("resource")
@Singleton
public class TableHistoryDatabaseAccessor extends DatabaseAccessor<TableHistorySchema>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableHistoryDatabaseAccessor.class);

    @Inject
    public TableHistoryDatabaseAccessor(SidecarSchema sidecarSchema,
                                        CQLSessionProvider sessionProvider)
    {
        super(sidecarSchema.tableSchema(TableHistorySchema.class), sessionProvider);
    }

    public ResultSetFuture insertTableSchemaHistory(String keyspace, String tableName, String schema)
    {
        UUID schemaUuid = UUID.nameUUIDFromBytes(schema.getBytes(StandardCharsets.UTF_8));
        return session().executeAsync(tableSchema
                                      .insertTableSchema()
                                      .bind(keyspace, tableName, schemaUuid, schema));
    }

    public String tableSchemaFromVersion(String keyspace, String tableName, String version)
    {
        UUID schemaUuid = UUID.fromString(version);
        try
        {
            Row row = session()
                      .executeAsync(tableSchema
                                    .selectVersionTableSchema()
                                    .bind(keyspace, tableName, schemaUuid))
                      .get()
                      .one();
            return row == null ? null : row.getString("table_schema");
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
