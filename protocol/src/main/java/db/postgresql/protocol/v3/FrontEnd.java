package db.postgresql.protocol.v3;

public enum FrontEnd {

    Bind('B'),
    CancelRequest(80877102),
    Close('C'),
    CopyData('d'),
    CopyDone('c'),
    CopyFail('f'),
    Describe('D'),
    Execute('E'),
    Flush('H'),
    FunctionCall('F'),
    Parse('P'),
    Password('p'),
    Query('Q'),
    SSLRequest(80877103),
    StartupMessage(196608),
    Sync('S'),
    Terminate('X');
    
    private FrontEnd(int code) {
        this.code = code;
        this.size = 4;
    }

    private FrontEnd(char code) {
        this.code = 0xFFFF & code;
        this.size = 1;
    }

    public final int code;
    public final int size;

    public byte toByte() {
        return (byte) code;
    }
}
