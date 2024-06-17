package io.tembo.pgmq;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import static io.tembo.pgmq.PGMQQuery.*;
import static java.util.logging.Level.INFO;

/**
 * Base class for interacting with a queue.
 */
public class PGMQueue implements PGMQOperations {
    private static final Logger LOG = Logger.getLogger(PGMQueue.class.getName());

    private final PGConnectionPoolDataSource pool;

    public PGMQueue(PGConnectionPoolDataSource pool) throws SQLException {
        this.pool = pool;
        createExtensionIfNotPresent(pool.getConnection());
    }

    private void createExtensionIfNotPresent(Connection connection) throws SQLException {
        connection
                .prepareStatement("create extension if not exists pgmq cascade;")
                .execute();
    }

    @Override
    public void create(String queueName) throws SQLException {
        LOG.log(INFO, "Create queue with name %s".formatted(queueName));
        try {
            var connection = pool.getConnection();
            for (var statement : initQueueClientOnly(queueName, true)) {
                LOG.log(INFO, "Create queue : Execute statement : %s".formatted(statement));
                connection.prepareStatement(statement).execute();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createUnlogged(String queueName) throws SQLException {
        try {
            var connection = pool.getConnection();
            connection.setAutoCommit(false);

            for (var statement : initQueueClientOnly(queueName, false)) {
                connection.prepareStatement(statement).execute();
            }
            connection.commit();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy(String queueName) {
        try {
            var connection = pool.getConnection();
            connection.setAutoCommit(false);

            for (var statement : destroyQueueClientOnly(queueName)) {
                connection.prepareStatement(statement).execute();
            }
            connection.commit();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer send(String queueName, String message) {
        try {
            var statement = pool.getConnection().prepareStatement(enqueue(queueName, 1, 0));
            statement.setString(1, message);
            var result = statement.executeQuery();
            result.next();
            return result.getInt("msg_id");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        try {
            ResultSet resultSet = pool.getConnection().prepareStatement("SELECT * from pgmq.list_queues();").executeQuery();
            List<PGMQueueMetadata> list = new ArrayList<>();
            while (resultSet.next()) {
                var metadata = new PGMQueueMetadata(
                        resultSet.getString("queue_name"),
                        resultSet.getTimestamp("created_at").toInstant(),
                        resultSet.getBoolean("is_unlogged"),
                        resultSet.getBoolean("is_partitioned")
                );
                list.add(metadata);
            }

            return Optional.of(list);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<DefaultMessage> read(String queueName, int visibilityTime) {
        try {
            var query = PGMQQuery.read(queueName, visibilityTime, 1);
            LOG.log(INFO, "Read queue : Execute statement : %s".formatted(query));
            ResultSet resultSet = pool.getConnection().prepareStatement(query).executeQuery();
            resultSet.next();
            //FIXME: DefaultMessage Wrapper
            var message = new DefaultMessage(
                    resultSet.getInt("msg_id"),
                    resultSet.getInt("read_ct"),
                    resultSet.getTimestamp("enqueued_at").toInstant(),
                    resultSet.getTimestamp("vt").toInstant(),
                    resultSet.getString("message").getBytes(StandardCharsets.UTF_8)
            );

            return Optional.of(message);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<DefaultMessage> read(String queueName) {
        return read(queueName, 30);
    }

    @Override
    public Integer delete(String queueName, Integer messageId) {
        try {
            var statement = pool.getConnection().prepareStatement(deleteBatch(queueName));
            var array = pool.getConnection().createArrayOf("int", new Integer[] {messageId});
            statement.setArray(1, array);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
