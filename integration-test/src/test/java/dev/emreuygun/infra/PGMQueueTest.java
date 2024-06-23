package dev.emreuygun.infra;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static dev.emreuygun.utils.DatabaseUtils.dataSource;
import static dev.emreuygun.utils.DatabaseUtils.postgresContainer;

public abstract class PGMQueueTest {

    @BeforeEach
    public void setup() {
        postgresContainer.start();
        dataSource.setURL(postgresContainer.getJdbcUrl());
    }

    @AfterEach
    public void tearDown() {
        postgresContainer.stop();
    }
}
