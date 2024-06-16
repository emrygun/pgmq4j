package io.tembo.pgmq;

import java.util.List;
import java.util.StringJoiner;

public interface PGMQClient {

    String PGMQ_SCHEMA = "pgmq";
    String QUEUE_PREFIX = "q";
    String ARCHIVE_PREFIX = "a";

    default List<String> initQueueClientOnly(String queueName, boolean isUnlogged) {
        return List.of(
            createSchema(),
            createMeta(),
            createQueue(queueName, isUnlogged),
            createIndex(queueName),
            createArchive(queueName),
            createArchiveIndex(queueName),
            insertMeta(queueName, false, isUnlogged)
            //grantPgmonMeta(),
            //grantPgmonQueue(name)
        );
    }

    default List<String> destroyQueueClientOnly(String queueName) {
        return List.of(
                createSchema(),
                dropQueue(queueName),
                dropQueueArchive(queueName),
                deleteQueueMetadata(queueName)
        );
    }

    default String deleteQueueMetadata(String queueName) {
        return """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'meta' and table_schema = 'pgmq')
            THEN
                DELETE
                FROM %s.meta
                WHERE queue_name = '%s';
            END IF;
        END $$;
        """.formatted(PGMQ_SCHEMA, queueName);
    }

    default String dropQueueArchive(String queueName) {
        return "DROP TABLE IF EXISTS %s.%s_%s;".formatted(PGMQ_SCHEMA, ARCHIVE_PREFIX, queueName);
    }

    default String dropQueue(String queueName) {
        return "DROP TABLE IF EXISTS %s.%s_%s;".formatted(PGMQ_SCHEMA, QUEUE_PREFIX, queueName);
    }

    default String createMeta() {
        return """
        CREATE TABLE IF NOT EXISTS %s.meta (
                queue_name VARCHAR UNIQUE NOT NULL,
                is_partitioned BOOLEAN NOT NULL,
                is_unlogged BOOLEAN NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
        );
        """.formatted(PGMQ_SCHEMA);
    }

    default String createSchema() {
        return "CREATE SCHEMA IF NOT EXISTS %s;".formatted(PGMQ_SCHEMA);
    }

    default String createQueue(String queueName, boolean isUnlogged) {
        return """
        CREATE %s TABLE IF NOT EXISTS %s.%s_%s (
            msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            read_ct INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt TIMESTAMP WITH TIME ZONE NOT NULL,
            message JSONB
        );
        """.formatted(
                isUnlogged ? "UNLOGGED" : "",
                PGMQ_SCHEMA,
                QUEUE_PREFIX,
                queueName
        );
    }

    // indexes are created ascending to support FIFO
    default String createIndex(String queueName) {
        return "CREATE INDEX IF NOT EXISTS %s_%s_vt_idx ON %s.%s_%s (vt ASC);".formatted(
                QUEUE_PREFIX,
                queueName,
                PGMQ_SCHEMA,
                QUEUE_PREFIX,
                queueName);
    }

    default String createArchive(String queueName) {
        return """
        CREATE TABLE IF NOT EXISTS %s.%s_%s (
            msg_id BIGINT PRIMARY KEY,
            read_ct INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt TIMESTAMP WITH TIME ZONE NOT NULL,
            message JSONB
        );
        """.formatted(PGMQ_SCHEMA, ARCHIVE_PREFIX, queueName);
    }

    default String createArchiveIndex(String queueName) {
        return "CREATE INDEX IF NOT EXISTS archived_at_idx_%s ON %s.%s_%s (archived_at);".formatted(
                queueName,
                PGMQ_SCHEMA,
                ARCHIVE_PREFIX,
                queueName
        );
    }

    default String insertMeta(String queueName, boolean isPartitioned, boolean isUnlogged) {
        return """
        INSERT INTO %s.meta (queue_name, is_partitioned, is_unlogged)
        VALUES ('%s', %s, %s)
        ON CONFLICT
        DO NOTHING;
        """.formatted(
                PGMQ_SCHEMA,
                queueName,
                isPartitioned,
                isUnlogged
        );
    }

    default String enqueue(String queueName, int messageCount, int delaySecond) {
        StringJoiner sj = new StringJoiner(",");
        for (int i = 1; i < messageCount + 1; i++) {
            sj.add("((now() + interval '%s seconds'), ?::json)".formatted(delaySecond));
        }

        return """
        INSERT INTO %s.%s_%s (vt, message)
        VALUES %s
        RETURNING msg_id;
        """.formatted(
                PGMQ_SCHEMA,
                QUEUE_PREFIX,
                queueName,
                sj.toString()
        );
    }

    default String read(String queueName, int visibilityTimeSecond, int limit) {
        return """
        WITH cte AS
            (
                SELECT msg_id
                FROM %s.%s_%s
                WHERE vt <= clock_timestamp()
                ORDER BY msg_id ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
        UPDATE %S.%s_%s t
        SET
            vt = clock_timestamp() + interval '%s seconds',
            read_ct = read_ct + 1
        FROM cte
        WHERE t.msg_id=cte.msg_id
        RETURNING *;
        """.formatted(
                PGMQ_SCHEMA,
                QUEUE_PREFIX,
                queueName,
                limit,
                PGMQ_SCHEMA,
                QUEUE_PREFIX,
                queueName,
                visibilityTimeSecond
        );
    }

    default String deleteBatch(String queueName) {
        /*
        return """
        DELETE FROM %s.%s_%s
        WHERE msg_id = ANY(?)
        RETURNING msg_id;
        """.formatted(
                PGMQ_SCHEMA,
                QUEUE_PREFIX,
                queueName
        );
         */
        return """
        DELETE FROM %s.%s_%s
        WHERE msg_id = ANY(?);
        """.formatted(
                PGMQ_SCHEMA,
                QUEUE_PREFIX,
                queueName
        );
    }
}
