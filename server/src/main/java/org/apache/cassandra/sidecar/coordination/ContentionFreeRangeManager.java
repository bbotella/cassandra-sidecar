package org.apache.cassandra.sidecar.coordination;

import java.util.Map;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;

/**
 * Stub implementation of the RangeManager that provides contention-free failover for token ranges without coordinating with other Sidecar instances, at the cost of consistency.
 */
@Singleton
public class ContentionFreeRangeManager extends RangeManager
{
    @Inject
    public ContentionFreeRangeManager(Vertx vertx, TokenRingProvider tokenRingProvider)
    {
        super(vertx, tokenRingProvider);
    }

    Future<Boolean> proposeOwnership(SidecarInstance current, Map<String, Set<TokenRange>> ranges)
    {
        return Future.succeededFuture(true);
    }

    Future<Boolean> releaseOwnership(SidecarInstance primaryOwner, Map<String, Set<TokenRange>> ranges)
    {
        return Future.succeededFuture(true);
    }
}
