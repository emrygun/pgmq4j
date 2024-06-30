package dev.emreuygun.pgmq;

public class PGMQError extends RuntimeException {
    private final String code;

    public PGMQError(String code, String message) {
        super(message);
        this.code = code;
    }

    public PGMQError(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }
}
