package db.postgresql.protocol.v3;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

public class Session implements AutoCloseable {
    
    public static final int HEADER_SIZE = 5;

    private final String user;
    private final String password;
    private final String database;
    private final String host;
    private final int port;
    private final String application;
    private final Charset charset;
    private final String postgresCharset;
    private final Map<BackEnd, BackEndBuilder> builders;
    private final SocketChannel channel;
    private final Formatter formatter;
    private final Map<String,String> parameterStatuses = new LinkedHashMap<>();

    private TransactionStatus lastStatus;
    
    private Session(final String user,
                    final String password,
                    final String database,
                    final String host,
                    final int port,
                    final String application,
                    final Charset charset,
                    final String postgresCharset,
                    final Map<BackEnd,BackEndBuilder> builders,
                    final SocketChannel channel) {
        Map<BackEnd,BackEndBuilder> finalBuilders = new LinkedHashMap<>(builders);
        finalBuilders.put(BackEnd.ParameterStatus, parameterStatusBuilder);
        finalBuilders.put(BackEnd.ReadyForQuery, readyForQueryBuilder);
        
        this.user = user;
        this.password = password;
        this.database = database;
        this.host = host;
        this.port = port;
        this.application = application;
        this.charset = charset;
        this.postgresCharset = postgresCharset;
        this.builders = Collections.unmodifiableMap(finalBuilders);
        this.channel = channel;
        this.formatter = new Formatter(charset);
    }

    public boolean compatible(Session rhs) {
        return (user.equals(rhs.user) &&
                database.equals(rhs.database) &&
                host.equals(rhs.host) &&
                port == rhs.port &&
                application.equals(rhs.application) &&
                postgresCharset.equals(rhs.postgresCharset));
    }

    public String getUser() {
        return user;
    }
    
    public String getPassword() {
        return password;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public String getApplication() {
        return application;
    }
    
    public Charset getCharset() {
        return charset;
    }
    
    public String getPostgresCharset() {
        return postgresCharset;
    }
        
    public Map<BackEnd, BackEndBuilder> getBuilders() {
        return builders;
    }

    public Formatter getFormatter() {
        return formatter;
    }

    public Map<String,String> getParameterStatuses() {
        return Collections.unmodifiableMap(parameterStatuses);
    }

    public TransactionStatus getLastStatus() {
        return lastStatus;
    }

    public BackEndMessage next() {
        ByteBuffer header = read(HEADER_SIZE);
        BackEnd backEnd = BackEnd.find(header.get(0));
        int size = header.getInt(1) - 4; //subtract the size of the size field

        BackEndBuilder builder = builders.get(backEnd);
        if(builder != null) {
            return builder.read(backEnd, size, this);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    public ByteBuffer read(final int toRead) {
        try {
            final ByteBuffer buffer = ByteBuffer.allocate(toRead);
            long read = 0;
            while(read != toRead) {
                read += channel.read(buffer);
            }
            
            return (ByteBuffer) buffer.flip();
        }
        catch(IOException ioe) {
            throw new ProtocolException(ioe);
        }
    }

    public void write(final ByteBuffer buffer) {
        ByteBuffer[] single = { buffer };
        write(single);
    }

    public void write(final ByteBuffer[] buffers) {
        long total = 0;
        for(ByteBuffer buffer : buffers) {
            total += buffer.remaining();
        }
        
        try {
            long read = 0L;
            while(read < total) {
                read += channel.write(buffers);
            }
        }
        catch(IOException ioe) {
            throw new ProtocolException(ioe);
        }
    }

    private static boolean legalValue(String str) {
        return (str != null && str.length() > 0);
    }

    public static class Builder {
        private Map<BackEnd, BackEndBuilder> defaultBuilders() {
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
            return ret;
        }

        private Map<BackEnd, BackEndBuilder> builders = defaultBuilders();

        public Builder builders(Map<BackEnd, BackEndBuilder> val) {
            builders.putAll(val);
            return this;
        }

        private String postgresCharset = "UTF8";
        private Charset charset = Charset.forName("UTF-8");

        public Builder charset(final String val) {
            assert(legalValue(val));

            String tmp = val.toUpperCase();
            
            if(tmp.equals("UTF8")) {
                charset = Charset.forName("UTF-8");
                postgresCharset = tmp;
            }
            else if(tmp.equals("SQL_ASCII")) {
                charset = Charset.forName("US-ASCII");
                postgresCharset = tmp;
            }
            else {
                throw new ProtocolException(val + " is not a supported charset");
            }

            return this;
        }

        private String user = "";

        public Builder user(String val) {
            assert(legalValue(val));
            user = val;
            return this;
        }

        private String database = "";
        
        public Builder database(String val) {
            assert(legalValue(val));
            database = val;
            return this;
        }

        private String application = "db.postgres.v3";

        public Builder application(String val) {
            assert(legalValue(val));
            application = val;
            return this;
        }

        private String password = "";

        public Builder password(String val) {
            assert(legalValue(val));
            password = val;
            return this;
        }

        private String host = "";

        public Builder host(String val) {
            assert(legalValue(val));
            host = val;
            return this;
        }

        private int port = 5432;

        public Builder port(int val) {
            port = val;
            return this;
        }

        public Session build() {
            try {
                return new Session(user,
                                   password,
                                   database,
                                   host,
                                   port,
                                   application,
                                   charset,
                                   postgresCharset,
                                   Collections.unmodifiableMap(builders),
                                   SocketChannel.open());
            }
            catch(IOException ioe) {
                throw new ProtocolException(ioe);
            }
        }
    }

    private static final BackEndMessage parameterStatusInstance = new BackEndMessage(BackEnd.ParameterStatus);

    private final BackEndBuilder parameterStatusBuilder = new BackEndBuilder() {
            public BackEndMessage read(BackEnd backEnd, int size, Session session) {
                Map.Entry<String,String> pair = formatter.nullPair(session.read(size));
                parameterStatuses.put(pair.getKey(), pair.getValue());
                return parameterStatusInstance;
            }
        };

    private static final BackEndMessage readyForQueryInstance = new BackEndMessage(BackEnd.ReadyForQuery);
    
    public final BackEndBuilder readyForQueryBuilder = new BackEndBuilder() {
            public BackEndMessage read(BackEnd backEnd, int size, Session session) {
                lastStatus = TransactionStatus.from(session.read(size).get());
                return readyForQueryInstance;
            }
        };

    public Session connect() {
        try {
            channel.connect(new InetSocketAddress(InetAddress.getByName(host), port));
            return this;
        }
        catch(IOException ioe) {
            throw new ProtocolException(ioe);
        }
    }

    public void close() {
        try {
            channel.close();
        }
        catch(IOException ioe) {
            throw new ProtocolException(ioe);
        }
    }

    private Map<String,String> initKeysValues() {
        Map<String,String> ret = new LinkedHashMap<>();
        ret.put("user", user);

        if(legalValue(database)) {
            ret.put("database", database);
        }
        
        if(legalValue(application)) {
            ret.put("application_name", application);
        }

        if(legalValue(postgresCharset)) {
            ret.put("client_encoding", postgresCharset);
        }

        return Collections.unmodifiableMap(ret);
    }

    public Session authenticate() {
        write(formatter.startup(initKeysValues()));
        BackEndMessage m = next();
        if((m instanceof AuthenticationMessage) &&
           ((AuthenticationMessage) m).negotiate(this)) {
            while((m = next()).getBackEnd() != BackEnd.ReadyForQuery) { }
        }
        else {
            throw new ProtocolException("Could not authenticate with those credentials");
        }
    
        return this;
    }
}
