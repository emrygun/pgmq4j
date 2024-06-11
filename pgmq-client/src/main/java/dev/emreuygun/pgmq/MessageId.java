package dev.emreuygun.pgmq;

import java.util.Objects;

public class MessageId {
    private final long id;


    public MessageId(long id) {
        this.id = id;
    }

    Long getValue() {
        return id;
    }

    @Override
    public String toString() {
        return Long.toString(id);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        MessageId messageId = (MessageId) object;
        return id == messageId.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
