package db.postgresql.protocol.v3;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.net.ssl.SSLContext;

public class Session implements AutoCloseable {
    
    private final String user;
    private final String password;
    private final String database;
    private final String host;
    private final int port;
    private final String application;
    private final Charset encoding;
    private final String postgresEncoding;
    private final Map<BackEnd, ResponseHandler> handlers;
    private final PostgresqlStream stream;

    //mutable state
    private final Map<String,String> parameterStatuses = new ConcurrentHashMap<>(32, 0.75f, 1);
    private final ConcurrentLinkedQueue<Notification> notificationQueue = new ConcurrentLinkedQueue<>();
    private volatile TransactionStatus lastStatus;
    private volatile int pid;
    private volatile int secretKey;
    
    private Session(final String user,
                    final String password,
                    final String database,
                    final String host,
                    final int port,
                    final String application,
                    final Charset encoding,
                    final String postgresEncoding,
                    final Map<BackEnd,ResponseHandler> handlers,
                    final SSLContext sslContext) {
        
        Map<BackEnd,ResponseHandler> finalHandlers = new LinkedHashMap<>();
        finalHandlers.put(BackEnd.Authentication, authenticationHandler);
        finalHandlers.put(BackEnd.AuthenticationOk, authenticationHandler);
        finalHandlers.put(BackEnd.AuthenticationCleartextPassword, authenticationHandler);
        finalHandlers.put(BackEnd.AuthenticationMD5Password, authenticationHandler);
        finalHandlers.put(BackEnd.NotificationResponse, notificationHandler);
        finalHandlers.put(BackEnd.ParameterStatus, parameterStatusHandler);
        finalHandlers.put(BackEnd.BackendKeyData, keyDataHandler);
        finalHandlers.put(BackEnd.ReadyForQuery, readyForQueryHandler);
        finalHandlers.putAll(handlers); //specified overrides default
        
        this.user = user;
        this.password = password;
        this.database = database;
        this.host = host;
        this.port = port;
        this.application = application;
        this.encoding = encoding;
        this.postgresEncoding = postgresEncoding;
        this.handlers = Collections.unmodifiableMap(finalHandlers);
        this.stream = new PostgresqlStream(makeIo(sslContext), encoding, finalHandlers);
    }

    public PostgresqlStream getStream() {
        return stream;
    }
    
    private IO makeIo(final SSLContext sslContext) {
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

    public void close() {
        stream.close();
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
            return new Session(user,
                               password,
                               database,
                               host,
                               port,
                               application,
                               encoding,
                               postgresEncoding,
                               handlers,
                               sslContext);
        }

        public Session foreground() {
            Session session = build();
            session.stream.foreground().startup(session.getInitKeysValues());
            while(session.getStream().next(EnumSet.noneOf(BackEnd.class)).getBackEnd() != BackEnd.ReadyForQuery) { }
            return session;
        }

        public Session background() {
            Session session = foreground();
            session.stream.background();
            return session;
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

    private final ResponseHandler authenticationHandler = new ResponseHandler() {
            public void handle(Response r) {
                final Authentication a = (Authentication) r;
                final BackEnd be = a.getBackEnd();
                if(be == BackEnd.AuthenticationOk) {
                    return;
                }
                else if(be == BackEnd.AuthenticationCleartextPassword) {
                    stream.password(password);
                }
                else if(be == BackEnd.AuthenticationMD5Password) {
                    Authentication.Md5 md5 = (Authentication.Md5) a;
                    stream.md5(user, password, ByteBuffer.wrap(md5.getSalt()));
                }
                else {
                    stream.terminate();
                    throw new ProtocolException("Could not authenticate with those credentials");
                }
            }
        };

    private final ResponseHandler parameterStatusHandler = new ResponseHandler() {
            public void handle(Response r) {
                final ParameterStatus ps = (ParameterStatus) r;
                parameterStatuses.put(ps.getName(), ps.getValue());
            }
        };

    private final ResponseHandler notificationHandler = new ResponseHandler() {
            public void handle(Response r) {
                notificationQueue.add((Notification) r);
            }
        };

    private final ResponseHandler readyForQueryHandler = new ResponseHandler() {
            public void handle(Response r) {
                lastStatus = ((ReadyForQuery) r).getStatus();
            }
        };

    private final ResponseHandler keyDataHandler = new ResponseHandler() {
            public void handle(Response r) {
                KeyData data = (KeyData) r;
                pid = data.getPid();
                secretKey = data.getSecretKey();
            }
        };

    public static void main(String[] args) {
        Session.Builder sb = new Session.Builder().host("localhost").port(5432).database("testdb");
        Session session = sb.user("noauth").build();
        session.getStream().startup(session.getInitKeysValues());
    }
}

