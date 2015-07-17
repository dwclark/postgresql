package db.postgresql.protocol.v3.typeinfo;

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

    @Override
    public boolean equals(final Object rhs) {
        if(!(rhs instanceof PgAttribute)) {
            return false;
        }

        final PgAttribute o = (PgAttribute) rhs;
        return (relId == o.relId &&
                name.equals(o.name) &&
                typeId == o.typeId &&
                num == o.num);
    }

    @Override
    public int hashCode() {
        return relId + name.hashCode() + typeId + num;
    }
}
