package db.postgresql.protocol.v3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum BackEnd {

    Authentication('R', -1), //dummy placeholder, never returned
    AuthenticationOk('R', 0),
    AuthenticationKerberosV5('R', 8),
    AuthenticationCleartextPassword('R', 3),
    AuthenticationMD5Password('R', 5),
    AuthenticationSCMCredential('R', 6),
    AuthenticationGSS('R', 7),
    AuthenticationSSPI('R', 9),
    AuthenticationGSSContinue('R', 8),
    BackendKeyData('K'),
    BindComplete('2'),
    CloseComplete('3'),
    CommandComplete('C'),
    CopyData('d'),
    CopyDone('c'),
    CopyInResponse('G'),
    CopyOutResponse('H'),
    CopyBothResponse('W'),
    DataRow('D'),
    EmptyQueryResponse('I'),
    ErrorResponse('E'),
    FunctionCallResponse('V'),
    NoData('n'),
    NoticeResponse('N'),
    NotificationResponse('A'),
    ParameterDescription('t'),
    ParameterStatus('S'),
    ParseComplete('1'),
    PortalSuspended('s'),
    ReadyForQuery('Z'),
    RowDescription('T');

    private BackEnd(char id, int subId) {
        this.id = (byte) id;
        this.subId = (byte) subId;
    }

    private BackEnd(char id) {
        this(id, 0);
    }

    public final byte id;
    public final byte subId;

    public static final List<BackEnd> backends = Collections.unmodifiableList(new ArrayList(Arrays.asList(BackEnd.values())));

    public static BackEnd find(final byte id) {
        for(BackEnd b : backends) {
            if(b.id == id) {
                return b;
            }
        }

        throw new IllegalArgumentException("No BackEnd enum matches id: " + ((0xFF) & id));
    }

    public static BackEnd find(final byte id, final byte subId) {
        for(BackEnd b : backends) {
            if(b.id == id && b.subId == subId) {
                return b;
            }
        }

        throw new IllegalArgumentException("No BackEnd enum matches id: " + ((0xFF) & id) +
                                           ", subId: " + ((0xFF) & subId));
    }
}
