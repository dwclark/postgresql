package db.postgresql.protocol.v3.typeinfo;

public class PgType {

    final private int oid;
    final private String name;
    final private int arrayId;
    final private int relId;

    public PgType(final int oid, final String name, final int arrayId, final int relId) {
        this.oid = oid;
        this.name = name;
        this.arrayId = arrayId;
        this.relId = relId;
    }

    public int getOid() { return oid; }
    public String getName() { return name; }
    public int getArrayId() { return arrayId; }
    public int getRelId() { return relId; }
    public boolean isComposite() { return relId != 0; }

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof PgType)) {
            return false;
        }

        PgType o = (PgType) rhs;
        return (oid == o.oid &&
                name.equals(o.name) &&
                arrayId == o.arrayId &&
                relId == o.relId);
    }

    @Override
    public int hashCode() {
        return oid + name.hashCode() + arrayId + relId;
    }
}

