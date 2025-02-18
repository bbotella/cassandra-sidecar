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

package org.apache.cassandra.sidecar.client;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.client.SidecarInstanceImpl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Test for the {@link SimpleSidecarInstancesProvider} class
 */
class SimpleSidecarInstancesProviderTest
{
    @Test
    void testConstructionFailsWhenInstancesAreNull()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> new SimpleSidecarInstancesProvider(null))
        .withMessage("The instances parameter must be non-null and must contain at least one element");
    }

    @Test
    void testConstructionFailsWhenInstancesAreEmpty()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> new SimpleSidecarInstancesProvider(Collections.emptyList()))
        .withMessage("The instances parameter must be non-null and must contain at least one element");
    }

    @Test
    void testInstancesAreCopiedDuringConstruction()
    {
        List<SidecarInstanceImpl> instances = Collections.singletonList(new SidecarInstanceImpl("localhost", 9043));
        SidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(instances);
        assertThat(instancesProvider.instances()).isNotSameAs(instances);
    }

    @Test
    void testProviderInstancesAreImmutable()
    {
        List<SidecarInstanceImpl> instances = Collections.singletonList(new SidecarInstanceImpl("localhost", 9043));
        SidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(instances);
        assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> instancesProvider.instances().add(new SidecarInstanceImpl("bad", 80)));
    }
}
