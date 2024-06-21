package io.tembo.pgmq;

import java.sql.Connection;

/**
 * Interface for providing {@link java.sql.Connection} instances.
 * Designed to be implemented by a connection pool provider.
 */
public interface ConnectionProvider {
    Connection getConnection();
}
