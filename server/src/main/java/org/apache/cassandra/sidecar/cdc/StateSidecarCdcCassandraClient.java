package org.apache.cassandra.sidecar.cdc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSetFuture;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.sidecar.SidecarCdcCassandraClient;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Class managing the CDC state through the database accessor
 */
public class StateSidecarCdcCassandraClient implements SidecarCdcCassandraClient
{
    final CdcDatabaseAccessor cdcDatabaseAccessor;

    public StateSidecarCdcCassandraClient(CdcDatabaseAccessor cdcDatabaseAccessor)
    {
        this.cdcDatabaseAccessor = cdcDatabaseAccessor;
    }

    public List<ResultSetFuture> storeStateAsync(@NotNull String jobId, @NotNull TokenRange range, @NotNull ByteBuffer buf, long timestamp)
    {
        return cdcDatabaseAccessor.storeStateAsync(jobId, range, buf, timestamp);
    }

    public Stream<byte[]> loadStateForRange(String jobId, @Nullable TokenRange tokenRange)
    {
        if (tokenRange == null)
        {
            return Stream.empty();
        }
        return cdcDatabaseAccessor.loadStateForRange(jobId, tokenRange);
    }
}
