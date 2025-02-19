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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SslConfigurationImpl}
 */
class SslConfigurationImplTest
{
    @Test
    void testDefaults()
    {
        SslConfigurationImpl sslConfiguration = new SslConfigurationImpl();
        assertThat(sslConfiguration).isNotNull();
        assertThat(sslConfiguration.enabled()).isFalse();
        assertThat(sslConfiguration.preferOpenSSL()).isTrue();
        assertThat(sslConfiguration.handshakeTimeout().quantity()).isEqualTo(10);
        assertThat(sslConfiguration.handshakeTimeout().unit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(sslConfiguration.clientAuth()).isEqualTo("NONE");
        assertThat(sslConfiguration.cipherSuites()).isEmpty();
        assertThat(sslConfiguration.secureTransportProtocols()).containsExactly("TLSv1.2", "TLSv1.3");
        assertThat(sslConfiguration.keystore()).isNull();
        assertThat(sslConfiguration.isTrustStoreConfigured()).isFalse();
        assertThat(sslConfiguration.truststore()).isNull();
    }
}
