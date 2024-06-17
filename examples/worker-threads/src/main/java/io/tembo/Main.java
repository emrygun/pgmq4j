package io.tembo;

import ch.qos.logback.classic.LoggerContext;
import io.tembo.pgmq.PGMQueue;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.SQLException;

import static ch.qos.logback.classic.Level.INFO;

public class Main {
    private static final DockerImageName dockerImageName = DockerImageName
            .parse("quay.io/tembo/pg16-pgmq:latest")
            .asCompatibleSubstituteFor("postgres");

    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(dockerImageName)
            .withDatabaseName("test")
            .withUsername("postgres")
            .withPassword("postgres")
            .withExposedPorts(5432);

    private static final PGConnectionPoolDataSource dataSource = new PGConnectionPoolDataSource();

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("ROOT").setLevel(INFO);
        // loggerContext.getLogger("io.tembo").setLevel(TRACE);

        dataSource.setDatabaseName("postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("postgres");
        dataSource.setPortNumbers(new int[] {5432});
    }

    public static void main(String[] args) {
        postgresContainer.start();
        dataSource.setURL(postgresContainer.getJdbcUrl());

        String QUEUE_BASIC = "my_basic_queue";
        Integer VISIBILITY_TIMEOUT_SEC = 30;
        Integer BATCH_SIZE = 3;

        try {
            var pgmq = new PGMQueue(dataSource);

            pgmq.create(QUEUE_BASIC);

            //language=json
            String jsonMessage = """
            { "foo": "bar" }
            """;

            pgmq.send(QUEUE_BASIC, jsonMessage);
            var message = pgmq.read(QUEUE_BASIC, VISIBILITY_TIMEOUT_SEC).get();
            System.out.println(message);

            pgmq.delete(QUEUE_BASIC, message.messageId());
            System.out.println(pgmq.read(QUEUE_BASIC, VISIBILITY_TIMEOUT_SEC));

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        postgresContainer.stop();
    }
}