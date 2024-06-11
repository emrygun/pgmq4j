package io.tembo;

import io.tembo.pgmq.PGMQueue;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class Main {
    private static PGConnectionPoolDataSource dataSource;

    static {
        dataSource = new PGConnectionPoolDataSource();
        dataSource.setServerNames(new String[] {"localhost"});
        dataSource.setDatabaseName("postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("postgres");
        dataSource.setPortNumbers(new int[] {5432});
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void main(String[] args) {
        try (Connection conn = getConnection()) {
            var pgmq = new PGMQueue(dataSource);
            pgmq.create("test");
            pgmq.createUnlogged("test2");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}