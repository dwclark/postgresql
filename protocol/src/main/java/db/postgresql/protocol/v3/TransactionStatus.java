package db.postgresql.protocol.v3;

public enum TransactionStatus {

    IDLE('I'), IN_BLOCK('T'), FAILED('E');

    private TransactionStatus(final char id) {
        this.id = (byte) id;
    }

    final byte id;

    public static TransactionStatus from(final byte c) {
        if(c == IDLE.id) {
            return TransactionStatus.IDLE;
        }
        else if(c == IN_BLOCK.id) {
            return TransactionStatus.IN_BLOCK;
        }
        else if(c == FAILED.id) {
            return TransactionStatus.FAILED;
        }
        else {
            throw new IllegalArgumentException("Not a legal transaction status: " + (char) c);
        }
    }
}
