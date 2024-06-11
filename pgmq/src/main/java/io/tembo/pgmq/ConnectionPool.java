package io.tembo.pgmq;

public class ConnectionPool<T> {
    private final T pool;

    public ConnectionPool(T pool) {
        this.pool = pool;
    }

    public T getPool() {
        return pool;
    }
}
