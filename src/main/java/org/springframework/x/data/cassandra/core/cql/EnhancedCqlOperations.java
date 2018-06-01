package org.springframework.x.data.cassandra.core.cql;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.PreparedStatementBinder;
import org.springframework.data.cassandra.core.cql.ResultSetExtractor;
import org.springframework.data.cassandra.core.cql.RowMapper;

import java.util.List;

/**
 * These should be moved to CqlOperations if they are accepted by Spring Data Cassandra
 */
public interface EnhancedCqlOperations extends CqlOperations {

    public boolean execute(RegularStatement regStatement, Object... args) throws DataAccessException;

    public boolean execute(RegularStatement regStatement, PreparedStatementBinder psb) throws DataAccessException;

    public <T> T query(RegularStatement regStatement, ResultSetExtractor<T> resultSetExtractor, Object... args) throws DataAccessException;

    public <T> List<T> query(RegularStatement regStatement, RowMapper<T> rowMapper, Object... args) throws DataAccessException;

    public ResultSet queryForResultSet(RegularStatement regStatement, Object... args) throws DataAccessException;

}
