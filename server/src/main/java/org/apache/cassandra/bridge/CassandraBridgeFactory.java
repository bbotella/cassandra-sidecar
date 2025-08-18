package org.apache.cassandra.bridge;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.bridge.BaseCassandraBridgeFactory.getCassandraVersion;

/**
 * Factory class for creating Cassandra bridge instances based on version-specific jar files.
 * <p>
 * This factory maintains a cache of CassandraBridge instances mapped by Cassandra version labels
 * and provides static methods to retrieve bridge instances for specific Cassandra versions.
 * Each bridge is loaded from version-specific JAR resources and instantiated using reflection.
 */
public class CassandraBridgeFactory
{
    // maps Cassandra version-specific jar name (e.g. 'four-zero') to matching CassandraBridge
    private static final Map<String, CassandraBridge> CASSANDRA_BRIDGES =
    new ConcurrentHashMap<>(CassandraVersion.values().length);

    @NotNull
    public static CassandraBridge get(@NotNull String version)
    {
        return get(getCassandraVersion(version));
    }

    @NotNull
    public static CassandraBridge get(@NotNull CassandraVersionFeatures features)
    {
        return get(getCassandraVersion(features));
    }

    @NotNull
    public static CassandraBridge get(@NotNull CassandraVersion version)
    {
        String jarBaseName = version.jarBaseName();
        Preconditions.checkNotNull(jarBaseName, "Cassandra version " + version + " is not supported");
        return CASSANDRA_BRIDGES.computeIfAbsent(jarBaseName, CassandraBridgeFactory::create);
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private static CassandraBridge create(@NotNull String label)
    {
        try
        {
            ClassLoader loader = buildClassLoader(
            cassandraResourceName(label),
            bridgeResourceName(label),
            typesResourceName(label));
            Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass(CassandraBridge.IMPLEMENTATION_FQCN);
            Constructor<CassandraBridge> constructor = bridge.getConstructor();
            return constructor.newInstance();
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
               | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException("Failed to create Cassandra bridge for label " + label, exception);
        }
    }

    @NotNull
    static String cassandraResourceName(@NotNull String label)
    {
        return "/bridges/" + label + ".jar";
    }

    @NotNull
    static String bridgeResourceName(@NotNull String label)
    {
        return jarResourceName(label, "bridge");
    }

    @NotNull
    static String typesResourceName(@NotNull String label)
    {
        return jarResourceName(label, "types");
    }

    static String jarResourceName(String... parts)
    {
        return "/bridges/" + String.join("-", parts) + ".jar";
    }

    public static ClassLoader buildClassLoader(String... resourceNames)
    {
        URL[] urls = Arrays.stream(resourceNames)
                           .map(BaseCassandraBridgeFactory::copyClassResourceToFile)
                           .map(jar -> {
                               try
                               {
                                   return jar.toURI().toURL();
                               }
                               catch (MalformedURLException e)
                               {
                                   throw new RuntimeException(e);
                               }
                           }).toArray(URL[]::new);

        return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
        {
            public ClassLoader run()
            {
                return new PostDelegationClassLoader(urls, Thread.currentThread().getContextClassLoader());
            }
        });
    }

}
