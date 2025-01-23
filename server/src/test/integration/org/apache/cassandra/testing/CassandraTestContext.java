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

package org.apache.cassandra.testing;

import java.nio.file.Path;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;

/**
 * Passed to integration tests.
 * See {@link CassandraIntegrationTest} for the required annotation
 * See {@link CassandraTestTemplate} for the Test Template
 */
public class CassandraTestContext extends AbstractCassandraTestContext
{

    public CassandraTestContext(SimpleCassandraVersion version,
                                UpgradeableCluster cluster,
                                CertificateBundle ca,
                                Path serverKeystorePath,
                                Path truststorePath,
                                CassandraIntegrationTest annotation)
    {
        super(version, cluster, ca, serverKeystorePath, truststorePath, annotation);
    }

    @Override
    public String toString()
    {
        return "CassandraTestContext{"
               + "version=" + version
               + ", cluster=" + cluster
               + '}';
    }
}
