package io.tembo;

import ch.qos.logback.classic.LoggerContext;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static ch.qos.logback.classic.Level.INFO;

public class DatabaseUtils {
    public static final DockerImageName dockerImageName = DockerImageName
            .parse("quay.io/tembo/pg16-pgmq:latest")
            .asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(dockerImageName)
            .withDatabaseName("test")
            .withUsername("postgres")
            .withPassword("postgres")
            .withExposedPorts(5432);

    public static final PGConnectionPoolDataSource dataSource = new PGConnectionPoolDataSource();

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("ROOT").setLevel(INFO);
        // loggerContext.getLogger("io.tembo").setLevel(TRACE);

        dataSource.setDatabaseName("postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("postgres");
        dataSource.setPortNumbers(new int[] {5432});
    }

}
