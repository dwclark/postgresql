package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.EnumMap;
import java.nio.ByteBuffer;
import java.util.Collections;

public class StartProtocol extends SubProtocol {

    private final Notice.Errors errors = Notice.errors();
    private final Map<BackEnd,BiPredicate<PostgresqlStream,Response>> handlers;
    private final Map<String,String> parameterStatuses = new LinkedHashMap<>();
    private final Map<NoticeType,String> notices = new LinkedHashMap<>(1);
    private final Map<String,String> initKeysValues;
    
    private TransactionStatus status;
    private String password;
    private KeyData keyData;
    
    private final static ResponseBuilder responseBuilder = ResponseBuilder.standard();

    public ResponseBuilder getResponseBuilder() {
        return responseBuilder;
    }

    public Map<BackEnd,BiPredicate<PostgresqlStream,Response>> getHandlers() {
        return handlers;
    }

    public Notice.Errors getErrors() {
        return errors;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public KeyData getKeyData() {
        return keyData;
    }

    public Map<String,String> getParameterStatuses() {
        return Collections.unmodifiableMap(parameterStatuses);
    }

    public Map<NoticeType,String> getNotices() {
        return Collections.unmodifiableMap(noticies);
    }
    
    private BiPredicate<PostgresqlStream,Response> clear = (s, r) -> {
        s.password(password);
        return true;
    };

    private BiPredicate<PostgresqlStream,Response> md5 = (s, r) -> {
        Md5 m = (Md5) r;
        s.md5(user, password, ByteBuffer.wrap(m.getSalt()));
        return true;
    };

    private BiPredicate<PostgresqlStream,Response> keyData = (s, r) -> {
        keyData = (KeyData) r;
        return true;
    };

    private BiPredicate<PostgresqlStream,Response> readyForQuery = (s, r) -> {
        status = ((ReadyForQuery) r).getStatus();
        return false;
    };
    
    public StartProtocol(final String password, final Map<String,String> initKeysValues) {
        this.password = password;
        this.initKeysValues = initKeysValues;
        EnumMap<BackEnd,BiPredicate<PostgresqlStream,Response>> tmp = new EnumMap<>();
        tmp.put(BackEnd.AuthenticationMD5Password, this::md5);
        tmp.put(BackEnd.AuthenticationOk, (s,r) -> true);
        tmp.put(BackEnd.AuthenticationCleartextPassword, this::clear);
        tmp.put(BackEnd.BackendKeyData, this::keyData);
        tmp.put(BackEnd.ErrorResponse, errors);
        tmp.put(BackEnd.NoticeResponse, Notice.addMap(notices));
        tmp.put(BackEnd.ParameterStatus, ParameterStatus.addMap(parameterStatuses));
        tmp.put(BackEnd.ReadyForQuery, this::readyForQuery);
        handlers = Collections.unmodifiableMap(tmp);
    }

    public boolean start(final PostgresqlStream stream) {
        stream.startup(initKeysValues);
        return true;
    }
}
