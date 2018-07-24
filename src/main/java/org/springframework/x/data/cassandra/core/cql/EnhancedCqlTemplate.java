package org.springframework.x.data.cassandra.core.cql;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.PreparedStatementBinder;
import org.springframework.data.cassandra.core.cql.PreparedStatementCreator;
import org.springframework.data.cassandra.core.cql.ResultSetExtractor;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.cql.SimplePreparedStatementCreator;
import org.springframework.data.cassandra.core.cql.support.CachedPreparedStatementCreator;
import org.springframework.data.cassandra.core.cql.support.PreparedStatementCache;

import java.util.List;

/**
 * Two open issues and PRs regarding CQL template in Spring Data Cassandra
 * https://github.com/spring-projects/spring-data-cassandra/pull/129
 * https://github.com/spring-projects/spring-data-cassandra/pull/131
 * https://github.com/spring-projects/spring-data-cassandra/pull/133
 */
public class EnhancedCqlTemplate extends CqlTemplate implements EnhancedCqlOperations {

    private PreparedStatementCache preparedStatementCache;

    public EnhancedCqlTemplate() {
        super();
    }

    public EnhancedCqlTemplate(Session session) {
        super(session);
    }

    /**
     * overrides CqlTemplate
     */
    @Override
    protected PreparedStatementCreator newPreparedStatementCreator(String cql) {
        if (preparedStatementCache == null) {
            return new SimplePreparedStatementCreator(cql);
        } else {
            return CachedPreparedStatementCreator.of(preparedStatementCache, cql);
        }
    }

    protected PreparedStatementCreator newPreparedStatementCreator(RegularStatement regStatement) {
        if (preparedStatementCache == null) {
            return new SimpleRegularStatementPreparedStatementCreator(regStatement);
        } else {
            return CachedPreparedStatementCreator.of(preparedStatementCache, regStatement);
        }
    }

    public void setPreparedStatementCache(PreparedStatementCache psCache) {
        this.preparedStatementCache = psCache;
    }


    // -------------------------------------------------------------------------
    // Methods enhancing Prepared Statement usage (implements EnhancedCqlOperations
    // -------------------------------------------------------------------------


    @Override
    public boolean execute(RegularStatement regStatement, Object... args) throws DataAccessException {
        return execute(regStatement, newPreparedStatementBinder(args));
    }

    @Override
    public boolean execute(RegularStatement regStatement, PreparedStatementBinder psb) throws DataAccessException {
        return query(newPreparedStatementCreator(regStatement), psb, ResultSet::wasApplied);
    }

    @Override
    public <T> T query(RegularStatement regStatement, ResultSetExtractor<T> resultSetExtractor, Object... args) throws DataAccessException {
        return query(newPreparedStatementCreator(regStatement), newPreparedStatementBinder(args), resultSetExtractor);
    }

    @Override
    public <T> List<T> query(RegularStatement regStatement, RowMapper<T> rowMapper, Object... args) throws DataAccessException {
        return query(newPreparedStatementCreator(regStatement), newPreparedStatementBinder(args), newResultSetExtractor(rowMapper));
    }

    @Override
    public ResultSet queryForResultSet(RegularStatement regStatement, Object... args) throws DataAccessException {
        return query(newPreparedStatementCreator(regStatement), newPreparedStatementBinder(args), rs -> rs);
    }

}
