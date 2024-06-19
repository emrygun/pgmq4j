package io.tembo.pgmq;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final JsonSerializer jsonSerializer;

    PGMQueue(ExtensionContext context) throws SQLException {
        this.pool = context.getPoolDataSource();
        this.jsonSerializer = context.getSerializer();

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
        return sendDelay(queueName, message, 0);
    }

    @Override
    public Integer sendDelay(String queueName, String message, int delaySec) {
        try {
            var statement = pool.getConnection().prepareStatement(enqueue(queueName, 1, delaySec));
            statement.setString(1, message);
            var result = statement.executeQuery();
            result.next();
            return result.getInt("msg_id");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Integer> sendBatch(String queueName, List<String> messages) {
        try {
            var statement = pool.getConnection().prepareStatement(enqueue(queueName, messages.size(), 0));

            for (int i = 1; i < messages.size() + 1; i++) {
                statement.setString(i, messages.get(i - 1));
            }
            var result = statement.executeQuery();

            List<Integer> messageIds = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                result.next();
                messageIds.add(result.getInt("msg_id"));
            }

            return messageIds;
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
    public Optional<List<DefaultMessage>> readBatch(String queueName, int visibilityTime, int messageCount) {
        try {
            var query = PGMQQuery.read(queueName, visibilityTime, messageCount);
            ResultSet resultSet = pool.getConnection().prepareStatement(query).executeQuery();

            List<DefaultMessage> messages = new ArrayList<>();
            while (resultSet.next()) {
                var message = toMessage(resultSet);
                messages.add(message);
            }
            return Optional.of(messages);
        } catch (SQLException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<List<DefaultMessage>> readBatchWithPool(String queueName, int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval) {
        //FIXME: Implementation
        return Optional.empty();
    }

    @Override
    public Integer delete(String queueName, int messageId) {
        try {
            var statement = pool.getConnection().prepareStatement(PGMQQuery.deleteBatch(queueName));
            var array = pool.getConnection().createArrayOf("int", new Integer[] {messageId});
            statement.setArray(1, array);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer deleteBatch(String queueName, int[] messageIds) {
        try {
            var statement = pool.getConnection().prepareStatement(PGMQQuery.deleteBatch(queueName));
            var array = pool.getConnection().createArrayOf("int", Arrays.stream(messageIds).boxed().toArray(Integer[]::new));
            statement.setArray(1, array);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer purge(String queueName) {
        try {
            return pool.getConnection().prepareStatement(PGMQQuery.purge(queueName)).executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer archive(String queueName, int messageId) {
        return archiveBatch(queueName, new int[] {messageId});
    }

    @Override
    public Integer archiveBatch(String queueName, int[] messageIds) {
        try {
            var statement = pool.getConnection().prepareStatement(PGMQQuery.archiveBatch(queueName));
            var array = pool.getConnection().createArrayOf("int", Arrays.stream(messageIds).boxed().toArray(Integer[]::new));
            statement.setArray(1, array);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Message> pop(String queueName) {
        try {
            var query = PGMQQuery.pop(queueName);
            ResultSet resultSet = pool.getConnection().prepareStatement(query).executeQuery();
            resultSet.next();
            return Optional.of(toMessage(resultSet));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Message> setVisibilityTimeout(String queueName, int messageId, Instant visibilityTimeout) {
        //FIXME: Implementation is missing
        return null;
    }

    private static DefaultMessage toMessage(ResultSet resultSet) {
        try {
            return new DefaultMessage(
                    resultSet.getInt("msg_id"),
                    resultSet.getInt("read_ct"),
                    resultSet.getTimestamp("enqueued_at").toInstant(),
                    resultSet.getTimestamp("vt").toInstant(),
                    resultSet.getString("message").getBytes(StandardCharsets.UTF_8)
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
