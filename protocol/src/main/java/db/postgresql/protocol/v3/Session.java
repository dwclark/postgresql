package db.postgresql.protocol.v3;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.nio.channels.ScatteringByteChannel;
import java.nio.ByteBuffer;
import java.io.IOException;

public class Session {
    
    public static final int HEADER_SIZE = 5;

    private static final Map<BackEnd, BackEndBuilder> defaultBuilders = defaultBuilders();

    private static Map<BackEnd, BackEndBuilder> defaultBuilders() {
        Map<BackEnd, BackEndBuilder> ret = new LinkedHashMap();
        ret.put(BackEnd.Authentication, AuthenticationMessage.builder);
        ret.put(BackEnd.BackendKeyData, KeyDataMessage.builder);
        ret.put(BackEnd.BindComplete, BackEndMessage.builder);
        ret.put(BackEnd.CloseComplete, BackEndMessage.builder);
        ret.put(BackEnd.CommandComplete, CommandCompleteMessage.builder);
        ret.put(BackEnd.CopyData, CopyDataMessage.builder);
        ret.put(BackEnd.CopyDone, BackEndMessage.builder);
        ret.put(BackEnd.NoData, BackEndMessage.builder);
        ret.put(BackEnd.PortalSuspended, BackEndMessage.builder);
        return Collections.unmodifiableMap(ret);
    }
    
    private final Map<BackEnd, BackEndBuilder> builders;

    private static Map<BackEnd, BackEndBuilder> mergeBuilders(Map<BackEnd, BackEndBuilder> others) {
        Map<BackEnd, BackEndBuilder> ret = new LinkedHashMap(defaultBuilders);
        ret.putAll(others);
        return Collections.unmodifiableMap(ret);
    }

    public Session(Map<BackEnd, BackEndBuilder> builders) {
        this.builders = mergeBuilders(builders);
    }

    public BackEndMessage read(final ScatteringByteChannel channel) {
        ByteBuffer header = BackEndFormatter.read(HEADER_SIZE, channel);
        BackEnd backEnd = BackEnd.find(header.get(0));
        int size = header.getInt(1) - 4; //subtract the size of the size field

        BackEndBuilder builder = builders.get(backEnd);
        if(builder != null) {
            return builder.read(backEnd, size, channel);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
