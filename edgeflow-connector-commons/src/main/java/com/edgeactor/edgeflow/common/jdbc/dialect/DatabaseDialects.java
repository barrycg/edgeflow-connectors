package com.edgeactor.edgeflow.common.jdbc.dialect;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseDialects {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseDialects.class);

    // Sort lexicographically to maintain order
    private static final ConcurrentMap<String, DatabaseDialect> REGISTRY = new
            ConcurrentSkipListMap<>();

    static {
        loadAllDialects();
    }

    private static void loadAllDialects() {
        LOG.debug("Searching for and loading all JDBC source dialects on the classpath");
        final AtomicInteger count = new AtomicInteger();
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                ServiceLoader<DatabaseDialect> loadedDialects = ServiceLoader.load(
                        DatabaseDialect.class
                );
                // Always use ServiceLoader.iterator() to get lazy loading (see JavaDocs)
                Iterator<DatabaseDialect> dialectIterator = loadedDialects.iterator();
                try {
                    while (dialectIterator.hasNext()) {
                        try {
                            DatabaseDialect provider = dialectIterator.next();
                            REGISTRY.put(provider.getClass().getName(), provider);
                            count.incrementAndGet();
                            LOG.debug("Found '{}' provider {}", provider, provider.getClass());
                        } catch (Throwable t) {
                            LOG.debug("Skipping dialect provider after error while loading", t);
                        }
                    }
                } catch (Throwable t) {
                    LOG.debug("Error loading dialect providers", t);
                }
                return null;
            }
        });
        LOG.debug("Registered {} source dialects", count.get());
    }


    public static DatabaseDialect create(
            String dialectName,
            String config
    ) throws ConnectException {
        LOG.debug("Looking for named dialect '{}'", dialectName);
        Set<String> dialectNames = new HashSet<>();
        for (DatabaseDialect provider : REGISTRY.values()) {
            dialectNames.add(provider.name());
            if (provider.name().equals(dialectName)) {
                // return provider(config);
                return provider.create();
            }
        }
        for (DatabaseDialect provider : REGISTRY.values()) {
            if (provider.name().equalsIgnoreCase(dialectName)) {
                // return provider.create(config);
                return provider.create();
            }
        }
        throw new ConnectException(
                "Unable to find dialect with name '" + dialectName + "' in the available dialects: "
                + dialectNames
        );
    }

}
