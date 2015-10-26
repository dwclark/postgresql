package db.postgresql.protocol.v3;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import static db.postgresql.protocol.v3.BackEnd.*;
import db.postgresql.protocol.v3.io.PostgresqlStream;

public class ResponseBuilder {
    
    private final Map<BackEnd,Response.Build> builders;
    
    private static final Response authenticationOk = new Response(AuthenticationOk, 0);
    private static final Response bindComplete = new Response(BindComplete, 0);
    private static final Response closeComplete = new Response(CloseComplete, 0);
    private static final Response copyDone = new Response(CopyDone, 0);
    private static final Response emptyQueryResponse = new Response(EmptyQueryResponse, 0);
    private static final Response noData = new Response(NoData, 0);
    private static final Response parseComplete = new Response(ParseComplete, 0);
    private static final Response password = new Response(AuthenticationCleartextPassword, 0);
    private static final Response portalSuspended = new Response(PortalSuspended, 0);
    private static final Response.Build notImplemented = (PostgresqlStream s, int i) -> { throw new UnsupportedOperationException(); };
    
    private ResponseBuilder(final Map<BackEnd,Response.Build> builders) {
        this.builders = builders;
    }

    private static final Map<BackEnd,Response.Build> _defaults;
    private static final ResponseBuilder _standard;
    
    static {
        EnumMap<BackEnd, Response.Build> tmp = new EnumMap<>();
        tmp.put(AuthenticationCleartextPassword, (s, i) -> password);
        tmp.put(AuthenticationGSS, notImplemented);
        tmp.put(AuthenticationGSSContinue, notImplemented);
        tmp.put(AuthenticationKerberosV5, notImplemented);
        tmp.put(AuthenticationMD5Password, Md5::new);
        tmp.put(AuthenticationOk, (s, i) -> authenticationOk);
        tmp.put(AuthenticationSCMCredential, notImplemented);
        tmp.put(AuthenticationSSPI, notImplemented);
        tmp.put(BackendKeyData, KeyData::new);
        tmp.put(BindComplete, (s, i) -> bindComplete);
        tmp.put(CloseComplete, (s, i) -> closeComplete);
        tmp.put(CommandComplete, CommandComplete::new);
        tmp.put(CopyBothResponse, (s, i) -> new CopyResponse(CopyBothResponse, s, i));
        tmp.put(CopyData, CopyData::new);
        tmp.put(CopyDone, (s, i) -> copyDone);
        tmp.put(CopyInResponse, (s, i) -> new CopyResponse(CopyInResponse, s, i));
        tmp.put(CopyOutResponse, (s, i) -> new CopyResponse(CopyOutResponse, s, i));
        tmp.put(DataRow, DataRow::new);
        tmp.put(EmptyQueryResponse, (s, i) -> emptyQueryResponse);
        tmp.put(ErrorResponse, (s, i) -> new Notice(ErrorResponse, s, i));
        tmp.put(FunctionCallResponse, FunctionCallResponse::new);
        tmp.put(NoData, (s, i) -> noData);
        tmp.put(NoticeResponse, (s, i) -> new Notice(NoticeResponse, s, i));
        tmp.put(NotificationResponse, Notification::new);
        tmp.put(ParameterDescription, ParameterDescription::new);
        tmp.put(ParameterStatus, ParameterStatus::new);
        tmp.put(ParseComplete, (s, i) -> parseComplete);
        tmp.put(PortalSuspended, (s, i) -> portalSuspended);
        tmp.put(ReadyForQuery, (s, i) -> new ReadyForQuery(s));
        tmp.put(RowDescription, RowDescription::new);
        _defaults = Collections.unmodifiableMap(tmp);
        _standard = new ResponseBuilder(_defaults);
    }
    
    public static ResponseBuilder standard() {
        return _standard;
    }

    public static ResponseBuilder overrides(final Map<BackEnd,Response.Build> map) {
        Map<BackEnd,Response.Build> tmp = new EnumMap<>(_defaults);
        for(Map.Entry<BackEnd,Response.Build> entry : map.entrySet()) {
            tmp.put(entry.getKey(), entry.getValue());
        }

        return new ResponseBuiler(Collections.unmodifiableMap(tmp));
    }

    public Response build(final BackEnd backEnd, final PostgresqlStream stream, final int size) {
        return builders.get(backEnd).build(stream, size);
    }
}
