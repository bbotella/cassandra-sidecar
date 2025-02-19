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

package org.apache.cassandra.sidecar.routes.tokenrange;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.sidecar.testing.BootstrapBBUtils;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

/**
 * Multi-DC Host replacement scenario integration tests for token range replica mapping endpoint with the in-jvm
 * dtest framework.
 */
@Tag("heavy")
@ExtendWith(VertxExtension.class)
class ReplacementMultiDCTest extends ReplacementBaseTest
{
    @CassandraIntegrationTest(
    nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, buildCluster = false)
    void retrieveMappingWithNodeReplacementMultiDC(VertxTestContext context,
                                                   ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperReplacementsMultiDC.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperReplacementsMultiDC::install, cassandraTestContext);

        List<IUpgradeableInstance> nodesToRemove = Arrays.asList(cluster.get(3), cluster.get(cluster.size()));
        runReplacementTestScenario(context,
                                   BBHelperReplacementsMultiDC.nodeStart,
                                   BBHelperReplacementsMultiDC.transientStateStart,
                                   BBHelperReplacementsMultiDC.transientStateEnd,
                                   cluster,
                                   nodesToRemove,
                                   generateExpectedRangeMappingReplacementMultiDC());
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 10 node cluster
     * across 2 DCs with the last 2 nodes leaving the cluster (1 per DC), with RF 3
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in
     * subsequent ranges cascade with the next range excluding the first replica, and including the next replica from
     * the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * In a multi-DC scenario, a single range will have nodes from both DCs. The replicas are grouped by DC here
     * to allow per-DC validation as returned from the sidecar endpoint.
     * <p>
     * Ranges that including leaving node replicas will have [RF + no. leaving nodes in replica-set] replicas with
     * the new replicas being the existing nodes in ring-order.
     * <p>
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D (with D being the leaving node)
     * Expected Range 2 - B, C, D, A (With A taking over the range of the leaving node)
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeMappingReplacementMultiDC()
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        int nodeCount = annotation.nodesPerDc() * annotation.numDcs();
        List<Range<BigInteger>> expectedRanges = generateExpectedRanges(nodeCount);
        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();

        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.8"));

        dc1Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.7", "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.8", "127.0.0.10",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.7", "127.0.0.9", "127.0.0.1"));
        dc2Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.2",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.8", "127.0.0.10", "127.0.0.2",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.9", "127.0.0.1", "127.0.0.3",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.10", "127.0.0.2", "127.0.0.4",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.10", "127.0.0.2", "127.0.0.4",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5",
                                                             "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", dc1Mapping);
                put("datacenter2", dc2Mapping);
            }
        };
    }

    /**
     * ByteBuddy helper for multi-DC node replacement
     */
    public static class BBHelperReplacementsMultiDC
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (across 2 DCs) with a 2 replacement nodes
            // We intercept the bootstrap of the replacement nodes to validate token ranges
            if (nodeNumber > 10)
            {
                BootstrapBBUtils.installSetBoostrapStateInterceptor(cl, BBHelperReplacementsMultiDC.class);
            }
        }

        public static void setBootstrapState(SystemKeyspace.BootstrapState state, @SuperCall Callable<Void> orig) throws Exception
        {
            if (state == SystemKeyspace.BootstrapState.COMPLETED)
            {
                nodeStart.countDown();
                // trigger bootstrap start and wait until bootstrap is ready from test
                transientStateStart.countDown();
                awaitLatchOrTimeout(transientStateEnd, 2, TimeUnit.MINUTES, "transientStateEnd");
            }
            orig.call();
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }
}
