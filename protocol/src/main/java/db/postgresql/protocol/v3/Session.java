package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.serializers.*;
import db.postgresql.protocol.v3.typeinfo.*;
import db.postgresql.protocol.v3.types.*;
import db.postgresql.protocol.v3.io.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import javax.net.ssl.SSLContext;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class Session extends PostgresqlStream implements AutoCloseable {
    
    private final String user;
    private final String password;
    private final String database;
    private final String host;
    private final int port;
    private final String application;
    private final Charset encoding;
    private final String postgresEncoding;
    private final Map<BackEnd, ResponseHandler> handlers;
    private final Builder builder;
    private final NumericSerializer numericSerializer;
    private final MoneySerializer moneySerializer;
    public final StringSerializer stringSerializer;
    
    //mutable state
    private final Map<String,String> parameterStatuses = new ConcurrentHashMap<>(32, 0.75f, 1);
    private final ConcurrentLinkedQueue<Notification> notificationQueue = new ConcurrentLinkedQueue<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicLong counter = new AtomicLong();
    private final Map<String,String> statementCache = new ConcurrentHashMap<>(32, 0.75f, 1);
    private volatile TransactionStatus lastStatus;
    private volatile int pid;
    private volatile int secretKey;
    
    private Session(final Builder builder,
                    final String user,
                    final String password,
                    final String database,
                    final String host,
                    final int port,
                    final String application,
                    final Charset encoding,
                    final Locale numericLocale,
                    final Locale moneyLocale,
                    final String postgresEncoding,
                    final Map<BackEnd,ResponseHandler> handlers,
                    final SSLContext sslContext) {
        
        super(makeIo(sslContext, host, port), encoding);
        this.builder = builder;
        this.user = user;
        this.password = password;
        this.database = database;
        this.host = host;
        this.port = port;
        this.application = application;
        this.encoding = encoding;
        this.postgresEncoding = postgresEncoding;

        //setup serializers
        this.moneySerializer = new MoneySerializer(moneyLocale);
        this.numericSerializer = new NumericSerializer(numericLocale);
        this.stringSerializer = new StringSerializer(encoding);
        builtinTypes();

        Map<BackEnd,ResponseHandler> finalHandlers = new LinkedHashMap<>();
        
        finalHandlers.put(BackEnd.AuthenticationOk, (Response r) -> {});

        finalHandlers.put(BackEnd.AuthenticationCleartextPassword, (Response r) -> password(password));

        finalHandlers.put(BackEnd.AuthenticationMD5Password, (Response r) -> {
                Authentication.Md5 md5 = (Authentication.Md5) r;
                md5(user, password, ByteBuffer.wrap(md5.getSalt())); });
        
        finalHandlers.put(BackEnd.NoticeResponse, (Response r) -> ((Notice) r).throwMe());

        finalHandlers.put(BackEnd.ErrorResponse, (Response r) -> ((Notice) r).throwMe());
        
        finalHandlers.put(BackEnd.NotificationResponse, (Response r) -> notificationQueue.add((Notification) r));
        
        finalHandlers.put(BackEnd.ParameterStatus, (Response r) -> {
                final ParameterStatus ps = (ParameterStatus) r;
                parameterStatuses.put(ps.getName(), ps.getValue()); });
        
        finalHandlers.put(BackEnd.BackendKeyData, (Response r) -> {
                KeyData data = (KeyData) r;
                pid = data.getPid();
                secretKey = data.getSecretKey(); });
        
        finalHandlers.put(BackEnd.ReadyForQuery, (Response r) -> lastStatus = ((ReadyForQuery) r).getStatus());

        finalHandlers.put(BackEnd.ParseComplete, (Response r) -> {});

        finalHandlers.put(BackEnd.BindComplete, (Response r) -> {});

        finalHandlers.put(BackEnd.NoData, (Response r) -> {});
        
        finalHandlers.putAll(handlers); //specified overrides default
        this.handlers = Collections.unmodifiableMap(finalHandlers);
    }
    
    private static IO makeIo(final SSLContext sslContext, String host, int port) {
        if(sslContext == null) {
            return new ClearIO(host, port);
        }
        else {
            return new SslIO(host, port, sslContext);
        }
    }

    public boolean compatible(final Session rhs) {
        return (user.equals(rhs.user) &&
                database.equals(rhs.database) &&
                host.equals(rhs.host) &&
                port == rhs.port &&
                application.equals(rhs.application) &&
                encoding.equals(rhs.encoding));
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
    
    public Charset getEncoding() {
        return encoding;
    }
    
    public Map<String,String> getParameterStatuses() {
        return Collections.unmodifiableMap(parameterStatuses);
    }

    public TransactionStatus getLastStatus() {
        return lastStatus;
    }

    public int getPid() {
        return pid;
    }

    public int getSecretKey() {
        return secretKey;
    }

    private static boolean legalValue(String val) {
        return (val != null) && !"".equals(val);
    }

    public static class Builder {

        private String postgresEncoding = "UTF8";
        private Charset encoding = Charset.forName("UTF-8");

        public Builder encoding(final String val) {
            assert(legalValue(val));

            String tmp = val.toUpperCase();
            
            if(tmp.equals("UTF8")) {
                encoding = Charset.forName("UTF-8");
                postgresEncoding = tmp;
            }
            else if(tmp.equals("SQL_ASCII")) {
                encoding = Charset.forName("US-ASCII");
                postgresEncoding = tmp;
            }
            else {
                throw new ProtocolException(val + " is not a supported charset");
            }

            return this;
        }

        private static Locale localeFromString(final String str) {
            String[] ary = str.split("_");

            if(ary.length == 1) {
                return new Locale(ary[0]);
            }
            else if(ary.length == 2) {
                return new Locale(ary[0], ary[1]);
            }
            else {
                return new Locale(ary[0], ary[1], ary[2]);
            }
        }
        
        private Locale numericLocale = Locale.getDefault();

        public Builder numericLocale(final String val) {
            numericLocale = localeFromString(val);
            return this;
        }

        private Locale moneyLocale = Locale.getDefault();

        public Builder moneyLocale(final String val) {
            moneyLocale = localeFromString(val);
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

        private SSLContext sslContext = null;

        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        private Map<BackEnd, ResponseHandler> handlers = Collections.emptyMap();

        public Builder handlers(Map<BackEnd, ResponseHandler> handlers) {
            this.handlers = handlers;
            return this;
        }

        public Session build() {
            return new Session(this, user, password, database, host, port, application,
                               encoding, numericLocale, moneyLocale, postgresEncoding,
                               handlers, sslContext).initialize();
        }
    }

    public Map<String,String> getInitKeysValues() {
        Map<String,String> ret = new LinkedHashMap<>();
        ret.put("user", user);
        
        if(legalValue(database)) {
            ret.put("database", database);
        }
        
        if(legalValue(application)) {
            ret.put("application_name", application);
        }

        if(legalValue(postgresEncoding)) {
            ret.put("client_encoding", postgresEncoding);
        }

        return Collections.unmodifiableMap(ret);
    }

        //back end events
    //next can return null if io timeout happens
    private final AtomicBoolean continueBackground = new AtomicBoolean(true);
    private final ReentrantLock pollingLock = new ReentrantLock();

    public Response next(final EnumSet<BackEnd> willHandle) {
        assert(pollingLock.isHeldByCurrentThread());

        while(true) {
            BackEnd backEnd;
            try {
                //be sure to not wait forever for response
                backEnd = BackEnd.find(get(1));
            }
            catch(NoData noData) {
                //definitely nothing there
                return null;
            }
            
            //something is there, we have to finish the action
            Response r = builders.get(backEnd).build(backEnd, getInt() - 4, this);
            if(willHandle.contains(r.getBackEnd())) {
                return r;
            }
            else {
                ResponseHandler h = handlers.get(r.getBackEnd());
                if(h == null) {
                    throw new ProtocolException("Could not find handler for " + r.getBackEnd());
                }
                
                h.handle(r);
            }
        }
    }

    private Session initialize() {
        foreground();
        startup(getInitKeysValues());
        next(EnumSet.of(BackEnd.ReadyForQuery));
        return this;
    }
    
    public Session foreground() {
        continueBackground.set(false);
        wakeup();
        pollingLock.lock();
        return this;
    }

    public void inForeground(Runnable r) {
        try {
            foreground();
            r.run();
        }
        finally {
            background();
        }
    }

    public Session background() {
        if(pollingLock.isLocked()) {
            pollingLock.unlock();
        }
        
        continueBackground.set(true);
        executor.submit(() -> {
                try {
                    pollingLock.lock();
                    EnumSet<BackEnd> none = EnumSet.noneOf(BackEnd.class);
                    while(continueBackground.get()) {
                        Response r = next(none);
                        if(r != null) {
                            handlers.get(r.getBackEnd()).handle(r);
                        }
                    }
                }
                finally {
                    pollingLock.unlock();
                } });

        return this;
    }

    public Session duplicate() {
        return builder.build();
    }

    public void withDuplicateSession(Consumer<Session> consumer) {
        try(Session dup = duplicate()) {
            consumer.accept(dup);
        }
    }

    public void close() {
        continueBackground.set(false);
        wakeup();
        pollingLock.lock();
        super.close();
        pollingLock.unlock();
    }

    private final void builtinType(final PgType tmp, Serializer s) {
        PgType pgType = new PgType.Builder(tmp).database(getDatabase()).build();
        Registry.add(pgType);
        Registry.add(pgType, s);
    }
    
    private final void builtinTypes() {
        builtinType(BitSetSerializer.PGTYPE_BIT, BitSetSerializer.instance);
        builtinType(BitSetSerializer.PGTYPE_VARBIT, BitSetSerializer.instance);
        builtinType(BooleanSerializer.PGTYPE, BooleanSerializer.instance);
        builtinType(BytesSerializer.PGTYPE, BytesSerializer.instance);
        builtinType(DateSerializer.PGTYPE, DateSerializer.instance);
        builtinType(DoubleSerializer.PGTYPE, DoubleSerializer.instance);
        builtinType(FloatSerializer.PGTYPE, FloatSerializer.instance);
        builtinType(IntSerializer.PGTYPE, IntSerializer.instance);
        builtinType(LocalDateTimeSerializer.PGTYPE, LocalDateTimeSerializer.instance);
        builtinType(LocalTimeSerializer.PGTYPE, LocalTimeSerializer.instance);
        builtinType(LongSerializer.PGTYPE, LongSerializer.instance);
        builtinType(MoneySerializer.PGTYPE, moneySerializer);
        builtinType(NumericSerializer.PGTYPE, numericSerializer);
        builtinType(OffsetDateTimeSerializer.PGTYPE, OffsetDateTimeSerializer.instance);
        builtinType(OffsetTimeSerializer.PGTYPE, OffsetTimeSerializer.instance);
        builtinType(ShortSerializer.PGTYPE, ShortSerializer.instance);
        builtinType(StringSerializer.PGTYPE_TEXT, stringSerializer);
        builtinType(StringSerializer.PGTYPE_VARCHAR, stringSerializer);
        builtinType(UUIDSerializer.PGTYPE, UUIDSerializer.instance);

        //geometry types
        builtinType(Box.PGTYPE, new BoxSerializer(this));
        builtinType(Circle.PGTYPE, new GeometrySerializer<>(this, Circle.class));
        builtinType(Line.PGTYPE, new GeometrySerializer<>(this, Line.class));
        builtinType(LineSegment.PGTYPE, new GeometrySerializer<>(this, LineSegment.class));
        builtinType(Path.PGTYPE, new GeometrySerializer<>(this, Path.class));
        builtinType(Point.PGTYPE, new GeometrySerializer<>(this, Point.class));
        builtinType(Polygon.PGTYPE, new GeometrySerializer<>(this, Polygon.class));
    }

    private Map<BackEnd,ResponseBuilder> builders() {
        Map<BackEnd, ResponseBuilder> ret = new LinkedHashMap<>();
        ret.put(BackEnd.Authentication, Authentication.builder);
        ret.put(BackEnd.BackendKeyData, KeyData.builder);
        ret.put(BackEnd.BindComplete, Response.builder);
        ret.put(BackEnd.CloseComplete, Response.builder);
        ret.put(BackEnd.CommandComplete, CommandComplete.builder);
        ret.put(BackEnd.CopyData, CopyData.builder);
        ret.put(BackEnd.CopyDone, Response.builder);
        ret.put(BackEnd.DataRow, DataRow.builder);
        ret.put(BackEnd.EmptyQueryResponse, Response.builder);
        ret.put(BackEnd.ErrorResponse, Notice.builder);
        ret.put(BackEnd.NoData, Response.builder);
        ret.put(BackEnd.NoticeResponse, Notice.builder);
        ret.put(BackEnd.NotificationResponse, Notification.builder);
        ret.put(BackEnd.ParameterDescription, ParameterDescription.builder);
        ret.put(BackEnd.ParameterStatus, ParameterStatus.builder);
        ret.put(BackEnd.ParseComplete, Response.builder);
        ret.put(BackEnd.PortalSuspended, Response.builder);
        ret.put(BackEnd.ReadyForQuery, ReadyForQuery.builder);
        ret.put(BackEnd.RowDescription, RowDescription.builder);
        return Collections.unmodifiableMap(ret);
    }

    protected final Map<BackEnd,ResponseBuilder> builders = builders();

    public String queryId() {
        return String.format("_%d", counter.getAndIncrement());
    }

    public SimpleQuery simple(final String query) {
        return new SimpleQuery(query, this);
    }

    public ExtendedQuery extended(final String query) {
        return extended(query, false);
    }
    
    public ExtendedQuery extended(final String query, final boolean useCache) {
        if(useCache) {
            final String id = statementCache.get(query);
            if(id == null) {
                //need to prepare the query
                ExtendedQuery eq = new ExtendedQuery(queryId(), this).prepare(query);
                statementCache.put(query, eq.getId());
                return eq;
            }
            else {
                return new ExtendedQuery(id, this).prepare(query);
            }
        }
        else {
            return new ExtendedQuery("", this).prepare(query);
        }
    }
}

