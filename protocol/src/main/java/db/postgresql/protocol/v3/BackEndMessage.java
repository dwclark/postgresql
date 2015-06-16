package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class BackEndMessage {

    public BackEnd getBackEnd() {
        return backEnd;
    }

    private final BackEnd backEnd;

    protected BackEndMessage(final BackEnd backEnd) {
        this.backEnd = backEnd;
    }

    public static final BackEndBuilder builder = new BackEndBuilder() {
            public BackEndMessage read(BackEnd backEnd, int size, Session session) {
                if(size > 0) {
                    session.read(size);
                }
                
                return new BackEndMessage(backEnd);
            }
        };
}
