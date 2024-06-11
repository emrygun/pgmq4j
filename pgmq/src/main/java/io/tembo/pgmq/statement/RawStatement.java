package io.tembo.pgmq.statement;


public enum RawStatement {
    CREATE("SELECT pgmq.create(?)"),
    CREATE_UNLOGGED("SELECT pgmq.create_unlogged(?)");

    private final String statement;

    RawStatement(String statement) {
        this.statement = statement;
    }

    public String getStatement() {
        return statement;
    }
}
