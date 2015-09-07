package db.postgresql.protocol.v3.typeinfo;

import db.postgresql.protocol.v3.Session;

public class PgAttribute {

    private final int relId;
    private final String name;
    private final int typeId;
    private final int num;

    public PgAttribute(final int relId, final String name, final int typeId, final int num) {
        this.relId = relId;
        this.name = name;
        this.typeId = typeId;
        this.num = num;
    }

    public int getRelId() { return relId; }
    public String getName() { return name; }
    public int getTypeId() { return typeId; }
    public int getNum() { return num; }

    public PgType pgType(final Session session) {
        return Registry.pgType(session, typeId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(96);
        sb.append("PgAttribute(");
        sb.append("name: " + getName() + ", ");
        sb.append("relId: " + getRelId() + ", ");
        sb.append("typeId: " + getTypeId() + ", ");
        sb.append("num: " + getNum() + ")");
        return sb.toString();
    }
}
