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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.shared.ShutdownException;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;

/**
 * The base class for all CassandraTestContext implementations
 */
public abstract class AbstractCassandraTestContext implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCassandraTestContext.class);

    public final SimpleCassandraVersion version;
    private final Map<String, String> initialProperties;
    private UpgradeableCluster cluster;
    private Consumer<UpgradeableCluster> onClusterBuilt;

    // certificates created when cluster is started with auth
    public final CertificateBundle ca;
    public final Path serverKeystorePath;
    public final Path truststorePath;

    public CassandraIntegrationTest annotation;

    public AbstractCassandraTestContext(SimpleCassandraVersion version,
                                        UpgradeableCluster cluster,
                                        CertificateBundle ca,
                                        Path serverKeystorePath,
                                        Path truststorePath,
                                        CassandraIntegrationTest annotation)
    {
        this.version = version;
        this.cluster = cluster;
        this.ca = ca;
        this.serverKeystorePath = serverKeystorePath;
        this.truststorePath = truststorePath;
        this.annotation = annotation;
        this.initialProperties = systemStringProperties();
    }

    public AbstractCassandraTestContext(SimpleCassandraVersion version,
                                        CertificateBundle ca,
                                        Path serverKeystorePath,
                                        Path truststorePath,
                                        CassandraIntegrationTest annotation)
    {
        this(version, null, ca, serverKeystorePath, truststorePath, annotation);
    }

    public UpgradeableCluster cluster()
    {
        return cluster;
    }

    public void setClusterBuiltListener(Consumer<UpgradeableCluster> listener)
    {
        this.onClusterBuilt = listener;
        if (cluster != null)
        {
            onClusterBuilt.accept(cluster);
        }
    }

    protected void setCluster(UpgradeableCluster cluster)
    {
        this.cluster = cluster;
        if (onClusterBuilt != null)
        {
            onClusterBuilt.accept(cluster);
        }
    }

    @Override
    public void close()
    {
        if (cluster != null)
        {
            LOGGER.info("Closing cluster={}", cluster);
            try
            {
                cluster.close();
            }
            // ShutdownException may be thrown from a different classloader, and therefore the standard
            // `catch (ShutdownException)` won't always work - compare the canonical names instead.
            catch (Throwable t)
            {
                if (Objects.equals(t.getClass().getCanonicalName(), ShutdownException.class.getCanonicalName()))
                {
                    LOGGER.warn("Encountered shutdown exception which closing the cluster", t);
                }
                else
                {
                    throw t;
                }
            }
            finally
            {
                LOGGER.info("Restoring system properties");
                restoreSystemProperties();
            }
        }
    }

    private void restoreSystemProperties()
    {
        Map<String, String> currentProps = systemStringProperties();
        currentProps.forEach((k, v) -> {
            String initialValue = initialProperties.get(k);
            if (initialValue == null)
            {
                System.clearProperty(k); // remove the added property during test
            }
            else if (!v.equals(initialValue))
            {
                System.setProperty(k, initialValue); // restore to the initial value
            }
            else
            {
                // property is not changed, do nothing
            }
        });
    }

    public int clusterSize()
    {
        return annotation.numDcs() * (annotation.nodesPerDc() + annotation.newNodesPerDc());
    }

    // return a copy of the current system string properties
    private Map<String, String> systemStringProperties()
    {
        Map<String, String> props = new HashMap<>();
        System.getProperties().forEach((k, v) -> {
            if (k instanceof String && v instanceof String)
            {
                props.put((String) k, (String) v);
            }
        });
        return props;
    }
}
