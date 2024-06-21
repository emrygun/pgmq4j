package io.tembo.pgmq.example.basic;

import io.tembo.pgmq.ConnectionProvider;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class PgConnectionProvider implements ConnectionProvider {
    private final PGConnectionPoolDataSource dataSourcePool;

    public PgConnectionProvider(PGConnectionPoolDataSource dataSourcePool) {
        this.dataSourcePool = dataSourcePool;
    }

    @Override
    public Connection getConnection() {
        try {
            return dataSourcePool.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Error getting connection from pool", e);
        }
    }
}
