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

package org.apache.cassandra.sidecar.db;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.CdcStatesSchema;
import org.apache.cassandra.sidecar.db.schema.TableHistorySchema;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TokenSplitUtil;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.TableIdentifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link CdcDatabaseAccessor}
 */
class CdcDatabaseAccessorTest
{
    @Test
    void testTokenSplitUtilLazyInitialization()
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        TokenSplitUtil mockTokenSplitUtil = mock(TokenSplitUtil.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        
        when(mockTokenSplitUtilProvider.get()).thenReturn(mockTokenSplitUtil);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        TokenSplitUtil result = accessor.tokenSplitUtil();
        
        assertThat(result).isEqualTo(mockTokenSplitUtil);
        verify(mockTokenSplitUtilProvider).get();
    }
    
    @Test
    void testTokenSplitUtilProvisionException()
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        
        RuntimeException rootCause = new RuntimeException("Connection failed");
        ProvisionException provisionException = new ProvisionException("Failed to provision", rootCause);
        when(mockTokenSplitUtilProvider.get()).thenThrow(provisionException);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        assertThatThrownBy(() -> accessor.tokenSplitUtil())
        .isInstanceOf(RuntimeException.class)
        .hasCause(rootCause);
    }
    
    @Test
    void testFullSchema()
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        when(mockSession.getCluster()).thenReturn(mockCluster);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        
        String expectedSchema = "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                "AND durable_writes = true;";
        when(mockMetadata.exportSchemaAsString()).thenReturn(expectedSchema);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        String result = accessor.fullSchema();
        
        assertThat(result).isEqualTo(expectedSchema);
    }
    
    @Test
    void testGetTableId()
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        when(mockSession.getCluster()).thenReturn(mockCluster);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        
        UUID expectedId = UUID.randomUUID();
        TableIdentifier tableId = new TableIdentifier("test_keyspace", "test_table");
        
        when(mockMetadata.getKeyspace("test_keyspace")).thenReturn(mockKeyspaceMetadata);
        when(mockKeyspaceMetadata.getTable("test_table")).thenReturn(mockTableMetadata);
        when(mockTableMetadata.getId()).thenReturn(expectedId);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        UUID result = accessor.getTableId(tableId);
        
        assertThat(result).isEqualTo(expectedId);
    }
    
    @Test
    void testPartitioner()
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        when(mockSession.getCluster()).thenReturn(mockCluster);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        when(mockMetadata.getPartitioner()).thenReturn("org.apache.cassandra.dht.Murmur3Partitioner");
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        Partitioner result = accessor.partitioner();
        
        assertThat(result).isEqualTo(Partitioner.Murmur3Partitioner);
    }
    
    @Test
    void testAwaitWithInterruptedException() throws ExecutionException, InterruptedException
    {
        ResultSetFuture mockFuture = mock(ResultSetFuture.class);
        when(mockFuture.get()).thenThrow(new InterruptedException("Interrupted"));
        
        assertThatThrownBy(() -> CdcDatabaseAccessor.await(Stream.of(mockFuture)).collect(java.util.stream.Collectors.toList()))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(InterruptedException.class);
    }
    
    @Test
    void testAwaitWithExecutionException() throws ExecutionException, InterruptedException
    {
        ResultSetFuture mockFuture = mock(ResultSetFuture.class);
        when(mockFuture.get()).thenThrow(new ExecutionException("Execution failed", new RuntimeException()));
        
        assertThatThrownBy(() -> CdcDatabaseAccessor.await(Stream.of(mockFuture)).collect(java.util.stream.Collectors.toList()))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(ExecutionException.class);
    }
    
    @Test
    void testSelectCdcRange()
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        
        String jobId = "test-job";
        int split = 1;
        
        PreparedStatement mockSelectStatement = mock(PreparedStatement.class);
        BoundStatement mockBoundStatement = mock(BoundStatement.class);
        ResultSetFuture mockFuture = mock(ResultSetFuture.class);
        
        when(mockCdcStatesSchema.select()).thenReturn(mockSelectStatement);
        when(mockSelectStatement.bind(jobId, (short) split)).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(mockBoundStatement)).thenReturn(mockFuture);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        ResultSetFuture result = accessor.selectCdcRange(jobId, split);
        
        assertThat(result).isEqualTo(mockFuture);
    }
    
    @Test
    void testInsertTableSchemaHistory()
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        
        String keyspace = "test_keyspace";
        String tableName = "test_table";
        String schema = "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                "AND durable_writes = true;";
        UUID expectedSchemaUuid = UUID.nameUUIDFromBytes(schema.getBytes(StandardCharsets.UTF_8));
        
        PreparedStatement mockInsertStatement = mock(PreparedStatement.class);
        BoundStatement mockBoundStatement = mock(BoundStatement.class);
        ResultSetFuture mockFuture = mock(ResultSetFuture.class);
        
        when(mockTableHistorySchema.insertTableSchema()).thenReturn(mockInsertStatement);
        when(mockInsertStatement.bind(keyspace, tableName, expectedSchemaUuid, schema)).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(mockBoundStatement)).thenReturn(mockFuture);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        ResultSetFuture result = accessor.insertTableSchemaHistory(keyspace, tableName, schema);
        
        assertThat(result).isEqualTo(mockFuture);
        verify(mockInsertStatement).bind(keyspace, tableName, expectedSchemaUuid, schema);
    }
    
    @Test
    void testTableSchemaFromVersionSuccess() throws Exception
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        
        String keyspace = "test_keyspace";
        String tableName = "test_table";
        String version = UUID.randomUUID().toString();
        String expectedSchema = "CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                "AND durable_writes = true;";
        UUID schemaUuid = UUID.fromString(version);
        
        PreparedStatement mockSelectStatement = mock(PreparedStatement.class);
        BoundStatement mockBoundStatement = mock(BoundStatement.class);
        ResultSetFuture mockFuture = mock(ResultSetFuture.class);
        ResultSet mockResultSet = mock(ResultSet.class);
        Row mockRow = mock(Row.class);
        
        when(mockTableHistorySchema.selectVersionTableSchema()).thenReturn(mockSelectStatement);
        when(mockSelectStatement.bind(keyspace, tableName, schemaUuid)).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(mockBoundStatement)).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.getString("table_schema")).thenReturn(expectedSchema);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        String result = accessor.tableSchemaFromVersion(keyspace, tableName, version);
        
        assertThat(result).isEqualTo(expectedSchema);
    }
    
    @Test
    void testTableSchemaFromVersionNotFound() throws Exception
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        
        String keyspace = "test_keyspace";
        String tableName = "test_table";
        String version = UUID.randomUUID().toString();
        UUID schemaUuid = UUID.fromString(version);
        
        PreparedStatement mockSelectStatement = mock(PreparedStatement.class);
        BoundStatement mockBoundStatement = mock(BoundStatement.class);
        ResultSetFuture mockFuture = mock(ResultSetFuture.class);
        ResultSet mockResultSet = mock(ResultSet.class);
        
        when(mockTableHistorySchema.selectVersionTableSchema()).thenReturn(mockSelectStatement);
        when(mockSelectStatement.bind(keyspace, tableName, schemaUuid)).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(mockBoundStatement)).thenReturn(mockFuture);
        when(mockFuture.get()).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(null);
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        String result = accessor.tableSchemaFromVersion(keyspace, tableName, version);
        
        assertThat(result).isNull();
    }
    
    @Test
    void testTableSchemaFromVersionWithException() throws Exception
    {
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CdcStatesSchema mockCdcStatesSchema = mock(CdcStatesSchema.class);
        TableHistorySchema mockTableHistorySchema = mock(TableHistorySchema.class);
        CQLSessionProvider mockSessionProvider = mock(CQLSessionProvider.class);
        Provider<TokenSplitUtil> mockTokenSplitUtilProvider = mock(Provider.class);
        CassandraBridgeFactory mockCassandraBridgeFactory = mock(CassandraBridgeFactory.class);
        Session mockSession = mock(Session.class);
        
        when(mockSessionProvider.get()).thenReturn(mockSession);
        
        String keyspace = "test_keyspace";
        String tableName = "test_table";
        String version = UUID.randomUUID().toString();
        UUID schemaUuid = UUID.fromString(version);
        
        PreparedStatement mockSelectStatement = mock(PreparedStatement.class);
        BoundStatement mockBoundStatement = mock(BoundStatement.class);
        ResultSetFuture mockFuture = mock(ResultSetFuture.class);
        
        when(mockTableHistorySchema.selectVersionTableSchema()).thenReturn(mockSelectStatement);
        when(mockSelectStatement.bind(keyspace, tableName, schemaUuid)).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(mockBoundStatement)).thenReturn(mockFuture);
        when(mockFuture.get()).thenThrow(new ExecutionException("Database error", new RuntimeException()));
        
        CdcDatabaseAccessor accessor = new CdcDatabaseAccessor(mockInstanceMetadataFetcher, mockCdcStatesSchema, 
                                                               mockTableHistorySchema, mockSessionProvider, 
                                                               mockTokenSplitUtilProvider, mockCassandraBridgeFactory);
        
        assertThatThrownBy(() -> accessor.tableSchemaFromVersion(keyspace, tableName, version))
        .isInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(ExecutionException.class);
    }
}
