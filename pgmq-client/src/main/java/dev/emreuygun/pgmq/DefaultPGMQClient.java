package dev.emreuygun.pgmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static dev.emreuygun.pgmq.ClientErrorFactory.archiveError;
import static dev.emreuygun.pgmq.ClientErrorFactory.createQueueError;
import static dev.emreuygun.pgmq.ClientErrorFactory.deleteMessageError;
import static dev.emreuygun.pgmq.ClientErrorFactory.destroyQueueError;
import static dev.emreuygun.pgmq.ClientErrorFactory.listQueuesError;
import static dev.emreuygun.pgmq.ClientErrorFactory.popError;
import static dev.emreuygun.pgmq.ClientErrorFactory.purgeError;
import static dev.emreuygun.pgmq.ClientErrorFactory.readMessageError;
import static dev.emreuygun.pgmq.ClientErrorFactory.sendMessageError;
import static dev.emreuygun.pgmq.PGMQQuery.destroyQueueClientOnly;
import static dev.emreuygun.pgmq.PGMQQuery.enqueue;
import static dev.emreuygun.pgmq.PGMQQuery.initQueueClientOnly;

/**
 * <p>
 * Core client of the pgmq transactions. This class is the main class to interact with the pgmq.
 * <br>
 * Contains low level database transactions of pgmq over postgresql connection.
 * </p>
 * Example:
 * <pre>
 * {@code
 *     var client = new DefaultPGMQClient(connection);
 *     client.create("queue_name");
 *     client.send("queue_name", "message");
 * }
 * </pre>
 *
 * @see PGMQClient
 * @see PGMQQuery
 * @see PGMQueue
 */
public final class DefaultPGMQClient implements PGMQClient {
    private static final Logger LOG = LoggerFactory.getLogger(PGMQueue.class);

    private final Connection connection;

    DefaultPGMQClient(Connection connection) throws SQLException {
        this.connection = connection;
        createExtensionIfNotPresent(connection);
    }

    private void createExtensionIfNotPresent(Connection connection) throws SQLException {
        connection
                .prepareStatement("create extension if not exists pgmq cascade;")
                .execute();
    }

    @Override
    public void create(String queueName) {
        LOG.trace("Create queue with name %s".formatted(queueName));
        try {
            for (var statement : initQueueClientOnly(queueName, true)) {
                LOG.trace("Create queue : Execute statement : %s".formatted(statement));
                connection.prepareStatement(statement).execute();
            }
        } catch (SQLException e) {
            throw createQueueError(queueName, true, e);
        }
    }

    @Override
    public void createUnlogged(String queueName) {
        try {
            for (var statement : initQueueClientOnly(queueName, false)) {
                connection.prepareStatement(statement).execute();
            }
        } catch (SQLException e) {
            throw createQueueError(queueName, false, e);
        }
    }

    @Override
    public void destroy(String queueName) {
        try {
            connection.setAutoCommit(false);

            for (var statement : destroyQueueClientOnly(queueName)) {
                connection.prepareStatement(statement).execute();
            }
            connection.commit();

        } catch (SQLException e) {
            throw destroyQueueError(queueName, e);
        }
    }

    @Override
    public MessageId send(String queueName, String message, int delaySec) {
        try {
            var statement = connection.prepareStatement(enqueue(queueName, 1, delaySec));
            statement.setString(1, message);
            var result = statement.executeQuery();
            result.next();
            return new MessageId(result.getLong("msg_id"));
        } catch (SQLException e) {
            throw sendMessageError(delaySec, 1, e);
        }
    }

    @Override
    public List<MessageId> sendBatch(String queueName, List<String> messages) {
        try {
            var statement = connection.prepareStatement(enqueue(queueName, messages.size(), 0));

            for (int i = 1; i < messages.size() + 1; i++) {
                statement.setString(i, messages.get(i - 1));
            }
            var result = statement.executeQuery();

            List<MessageId> messageIds = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                result.next();
                messageIds.add(new MessageId(result.getLong("msg_id")));
            }

            return messageIds;
        } catch (SQLException e) {
            throw sendMessageError(0, messages.size(), e);
        }
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        try {
            ResultSet resultSet = connection.prepareStatement("SELECT * from pgmq.list_queues();").executeQuery();
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
            throw listQueuesError(e);
        }
    }

    @Override
    public Optional<ByteArrayMessage> read(String queueName, int visibilityTime) {
        try {
            var query = PGMQQuery.read(queueName, visibilityTime, 1);
            LOG.trace("Read queue : Execute statement : %s".formatted(query));
            ResultSet resultSet = connection.prepareStatement(query).executeQuery();
            if (resultSet.next()) {
                var message = toMessage(resultSet);
                return Optional.of(message);
            } else {
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw readMessageError(queueName, visibilityTime, 1, e);
        }
    }

    @Override
    public List<ByteArrayMessage> readBatch(String queueName, int visibilityTime, int messageCount) {
        try {
            var query = PGMQQuery.read(queueName, visibilityTime, messageCount);
            ResultSet resultSet = connection.prepareStatement(query).executeQuery();

            List<ByteArrayMessage> messages = new ArrayList<>();
            while (resultSet.next()) {
                var message = toMessage(resultSet);
                messages.add(message);
            }
            return messages;
        } catch (SQLException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public Integer delete(String queueName, MessageId messageId) {
        try {
            var statement = connection.prepareStatement(PGMQQuery.deleteBatch(queueName));
            var array = connection.createArrayOf("long", new Long[] {messageId.getValue()});
            statement.setArray(1, array);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw deleteMessageError(e);
        }
    }

    @Override
    public Integer deleteBatch(String queueName, List<MessageId> messageIds) {
        try {
            var statement = connection.prepareStatement(PGMQQuery.deleteBatch(queueName));
            var array = connection.createArrayOf("long", messageIds.stream().map(MessageId::getValue).toArray(Long[]::new));
            statement.setArray(1, array);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw deleteMessageError(e);
        }
    }

    @Override
    public Integer purge(String queueName) {
        try {
            return connection.prepareStatement(PGMQQuery.purge(queueName)).executeUpdate();
        } catch (SQLException e) {
            throw purgeError(queueName, e);
        }
    }

    @Override
    public Integer archive(String queueName, List<MessageId> messageIds) {
        try {
            var statement = connection.prepareStatement(PGMQQuery.archiveBatch(queueName));
            var array = connection.createArrayOf("long", messageIds.stream().map(MessageId::getValue).toArray(Long[]::new));
            statement.setArray(1, array);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw archiveError(queueName, e);
        }
    }

    @Override
    public Optional<ByteArrayMessage> pop(String queueName) {
        try {
            var query = PGMQQuery.pop(queueName);
            ResultSet resultSet = connection.prepareStatement(query).executeQuery();
            resultSet.next();
            return Optional.of(toMessage(resultSet));
        } catch (SQLException e) {
            throw popError(queueName, e);
        }
    }

    @Override
    public Optional<ByteArrayMessage> setVisibilityTimeout(String queueName, MessageId messageId, Instant visibilityTimeout) {
        //FIXME: Implementation is missing
        return null;
    }

    private static ByteArrayMessage toMessage(ResultSet resultSet) {
        try {
            return new ByteArrayMessage(
                    new MessageId(resultSet.getLong("msg_id")),
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
