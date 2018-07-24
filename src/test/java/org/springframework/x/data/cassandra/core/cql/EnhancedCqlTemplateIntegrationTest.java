package org.springframework.x.data.cassandra.core.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.support.MapPreparedStatementCache;
import org.springframework.data.cassandra.core.cql.support.PreparedStatementCache;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EnhancedCqlTemplateIntegrationTest {

    private static EnhancedCqlTemplate template;

    @BeforeClass
    public static void beforeClass() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);

        Cluster cluster = Cluster.builder()
            .addContactPoints(EmbeddedCassandraServerHelper.getHost())
            .withPort(EmbeddedCassandraServerHelper.getNativeTransportPort())
            .build();

        Session session = cluster.connect();

        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(new ClassPathCQLDataSet("tables.cql", true, "cassandra_unit_keyspace"));

        session.execute("INSERT INTO user (id, username) VALUES ('WHITE', 'Walter');");

        template = new EnhancedCqlTemplate(session);
        template.setPreparedStatementCache(MapPreparedStatementCache.create());
        template.setConsistencyLevel(ConsistencyLevel.ONE);
    }

    @AfterClass
    public static void afterClass() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Test // DATACASS-556
    public void queryResultSetRegularStatementShouldUsePreparedStatement() throws Exception {

        Select userSelect = QueryBuilder.select("id").from("user");
        userSelect.where(QueryBuilder.eq("id", QueryBuilder.bindMarker()));

        ResultSet rs = template.queryForResultSet(userSelect, "WHITE");
        assertNotNull(rs);
        Row row = rs.one();
        assertThat(row.get("id", String.class)).contains("WHITE");
    }

    @Test // DATACASS-556
    public void queryRegularStatementShouldUsePreparedStatement() throws Exception {

        Select userSelect = QueryBuilder.select("id").from("user");
        userSelect.where(QueryBuilder.eq("id", QueryBuilder.bindMarker()));

        List<String> result = template.query(userSelect, (row, index) -> row.getString(0), "WHITE");

        assertThat(result).contains("WHITE");
    }

    @Test // DATACASS-556
    public void executeRegularStatementShouldUsePreparedStatement() throws Exception {

        Insert userInsert = QueryBuilder.insertInto("user");
        userInsert.value("id", QueryBuilder.bindMarker());
        userInsert.value("username", QueryBuilder.bindMarker());

        template.execute(userInsert, "SAUL", "CRIMINAL_LAWYER");

        Select userSelect = QueryBuilder.select("id", "username").from("user");
        userSelect.where(QueryBuilder.eq("id", QueryBuilder.bindMarker()));

        ResultSet rs = template.queryForResultSet(userSelect, "SAUL");
        assertNotNull(rs);
        Row row = rs.one();
        assertThat(row.get("username", String.class)).contains("CRIMINAL_LAWYER");

        rs = template.queryForResultSet(userSelect, "WHITE");
        assertNotNull(rs);
        row = rs.one();
        assertThat(row.get("username", String.class)).contains("Walter");
    }

    @Test
    public void query_string_prepStmt() {
        ResultSet rs = ((CqlTemplate)template).queryForResultSet("SELECT username FROM user WHERE id = ?", "WHITE");

        String username = this.resultSetHandler(rs);
        assertNotNull(username);
        assertEquals("Walter", username);
    }

    /**
     * this version does not run with the consistency level
     */
    @Test
    public void query_statement_prepStmt_queryString_and_options() {
        Select select = QueryBuilder.select("username").from("user");
        select.where(QueryBuilder.eq("id", QueryBuilder.bindMarker()))
            .setConsistencyLevel(ConsistencyLevel.QUORUM);

        select.enableTracing();

        ResultSet rs = ((CqlTemplate)template).queryForResultSet(select.getQueryString(), "WHITE");

        String username = this.resultSetHandler(rs);
        assertNotNull(username);
        assertEquals("Walter", username);
    }

    /**
     * this version does not run with the prepared statements
     * but will do consistency level and tracing
     */
    @Test
    public void query_statement_no_prepStmt_queryString_and_options() {
        Select select = QueryBuilder.select("username").from("user");
        select.where(QueryBuilder.eq("id", "WHITE"))
            .setConsistencyLevel(ConsistencyLevel.QUORUM);

        select.enableTracing();

        ResultSet rs = ((CqlTemplate)template).queryForResultSet(select);

        String username = this.resultSetHandler(rs);
        assertNotNull(username);
        assertEquals("Walter", username);
        String cLevel = rs.getExecutionInfo().getQueryTrace().getParameters().get("consistency_level");
        assertEquals("QUORUM", cLevel);
    }

    /**
     * this version is supported in CqlTemplate
     * and uses PreparedStatement & Consistency Levels
     */
    @Test
    public void query_statement_prepStmtCreator_and_options() {
        Select select = QueryBuilder.select("username").from("user");
        select.where(QueryBuilder.eq("id", QueryBuilder.bindMarker()))
            .setConsistencyLevel(ConsistencyLevel.QUORUM);

        select.enableTracing();

        PreparedStatementCache cache = MapPreparedStatementCache.create();

        String username = ((CqlTemplate)template).query(
               session -> cache.getPreparedStatement(session, select),
               ps -> ps.bind("WHITE"),
               this::resultSetHandlerWithTracing);

        assertNotNull(username);
        assertEquals("Walter", username);
    }

    /**
     * updated capability in Enahnced CQLTemplate
     */
    @Test
    public void query_statement_prepStmt_with_options() {
        Select select = QueryBuilder.select("username").from("user");
        select.where(QueryBuilder.eq("id", QueryBuilder.bindMarker()))
            .setConsistencyLevel(ConsistencyLevel.QUORUM)
            .enableTracing();

        ResultSet rs = template.queryForResultSet(select, "WHITE");

        String username = this.resultSetHandler(rs);
        assertNotNull(username);
        assertEquals("Walter", username);
        String cLevel = rs.getExecutionInfo().getQueryTrace().getParameters().get("consistency_level");
        assertEquals("QUORUM", cLevel);
    }


    private String resultSetHandlerWithTracing(ResultSet resultSet) {
        String theId = null;
        if (resultSet != null) {
            Row row = resultSet.one();
            if (row != null) {
                theId = row.getString("username");
                String cLevel = resultSet.getExecutionInfo().getQueryTrace().getParameters().get("consistency_level");
                assertEquals("QUORUM", cLevel);
            }
        }
        return theId;
    }

    private String resultSetHandler(ResultSet resultSet) {
        String theId = null;
        if (resultSet != null) {
            Row row = resultSet.one();
            if (row != null) {
                theId = row.getString("username");
            }
        }
        return theId;
    }

}
