package org.springframework.x.data.cassandra.core.cql;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import org.springframework.data.cassandra.core.cql.CqlProvider;
import org.springframework.data.cassandra.core.cql.PreparedStatementCreator;
import org.springframework.data.cassandra.core.cql.SimplePreparedStatementCreator;
import org.springframework.util.Assert;

public class SimpleRegularStatementPreparedStatementCreator implements PreparedStatementCreator, CqlProvider {


    private final RegularStatement regStatement;

    /**
     * Create a {@link SimplePreparedStatementCreator} given {@code cql}.
     *
     * @param regStatement must not be {@literal null}.
     */
    public SimpleRegularStatementPreparedStatementCreator(RegularStatement regStatement) {

        Assert.notNull(regStatement, "CQL is required to create a PreparedStatement");

        this.regStatement = regStatement;
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.cqlProvider#getCql()
     */
    @Override
    public String getCql() {
        return this.regStatement.getQueryString();
    }

    /* (non-Javadoc)
     * @see org.springframework.data.cassandra.core.cql.PreparedStatementCreator#createPreparedStatement(com.datastax.driver.core.Session)
     */
    @Override
    public PreparedStatement createPreparedStatement(Session session) throws DriverException {
        return session.prepare(this.regStatement);
    }
}
