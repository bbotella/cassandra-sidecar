package org.apache.cassandra.sidecar.db;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.TableSchema;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * Database accessor for querying Cassandra virtual tables in the system_views keyspace.
 */
@Singleton
public class VirtualTablesDatabaseAccessor extends DatabaseAccessor<TableSchema>
{
    public static final String SYSTEM_VIEWS_KS = "system_views";
    public static final String SYSTEM_VIEWS_SETTINGS_TBL = "settings";
    public static final String CDC_ON_REPAIR_ENABLED_FLAG = "cdc_on_repair_enabled";

    /**
     * Creates a new virtual tables database accessor.
     */
    @Inject
    public VirtualTablesDatabaseAccessor(TableSchema tableSchema, CQLSessionProvider sessionProvider)
    {
        super(tableSchema, sessionProvider);
    }

    /**
     * Checks if CDC on repair is enabled in the system settings.
     */
    public boolean isCdcOnRepairEnabled()
    {
        Select.Where query = QueryBuilder.select("value")
                                         .from(SYSTEM_VIEWS_KS, SYSTEM_VIEWS_SETTINGS_TBL)
                                         .where(eq("name", CDC_ON_REPAIR_ENABLED_FLAG));
        Row row = session().execute(query).one();
        return row != null && !row.isNull(0) && "true".equalsIgnoreCase(row.getString(0));
    }
}
