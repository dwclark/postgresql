package db.postgresql.protocol.v3;

public enum BackEnd {

    AuthenticationOk('R'),
    AuthenticationKerberosV5('R', 8),
    AuthenticationCleartextPassword('R', 5),
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
    EmptyQueryResponse('E'),
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
        this.id = id;
        this.subId = subId;
    }

    private BackEnd(char id) {
        this(id, 0);
    }

    final char id;
    final int subId;
}
