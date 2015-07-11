package db.postgresql.protocol.v3.serializers;

import java.math.BigDecimal;

public class Money {

    private final BigDecimal amount;

    public Money(final BigDecimal amount) {
        this.amount = amount;
    }

    public static Money wrap(final BigDecimal amount) {
        return new Money(amount);
    }

    public BigDecimal unwrap() {
        return amount;
    }

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof Money)) {
            return false;
        }

        Money money = (Money) rhs;
        return amount == money.amount;
    }

    @Override
    public int hashCode() {
        return amount.hashCode();
    }

    @Override
    public String toString() {
        return amount.toString();
    }
}
