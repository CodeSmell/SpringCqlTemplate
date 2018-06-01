package org.springframework.x.data.cassandra.core.cql;

import com.datastax.driver.core.Cluster;
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
import org.springframework.data.cassandra.core.cql.support.MapPreparedStatementCache;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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
    }

    @AfterClass
    public static void afterClass() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @Test // DATACASS-556
    public void queryResultSetRegularStatementShouldUsePreparedStatement() throws Exception {

        Select userSelect = QueryBuilder.select("id").from("user");
        userSelect.where(QueryBuilder.eq("id", ""));

        ResultSet rs = template.queryForResultSet(userSelect, "WHITE");
        assertNotNull(rs);
        Row row = rs.one();
        assertThat(row.get("id", String.class)).contains("WHITE");
    }

    @Test // DATACASS-556
    public void queryRegularStatementShouldUsePreparedStatement() throws Exception {

        Select userSelect = QueryBuilder.select("id").from("user");
        userSelect.where(QueryBuilder.eq("id", ""));

        List<String> result = template.query(userSelect, (row, index) -> row.getString(0), "WHITE");

        assertThat(result).contains("WHITE");
    }

    @Test // DATACASS-556
    public void executeRegularStatementShouldUsePreparedStatement() throws Exception {

        Insert userInsert = QueryBuilder.insertInto("user");
        userInsert.value("id", "");
        userInsert.value("username", "");

        template.execute(userInsert, "SAUL", "CRIMINAL_LAWYER");

        Select userSelect = QueryBuilder.select("id", "username").from("user");
        userSelect.where(QueryBuilder.eq("id", ""));

        ResultSet rs = template.queryForResultSet(userSelect, "SAUL");
        assertNotNull(rs);
        Row row = rs.one();
        assertThat(row.get("username", String.class)).contains("CRIMINAL_LAWYER");

        rs = template.queryForResultSet(userSelect, "WHITE");
        assertNotNull(rs);
        row = rs.one();
        assertThat(row.get("username", String.class)).contains("Walter");
    }

}
