package org.springframework.x.data.cassandra.core.cql;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.cassandra.core.cql.support.PreparedStatementCache;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EnhancedCqlTemplateTest {

    @Mock
    Session session;
    @Mock
    ResultSet resultSet;
    @Mock
    Row row;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    BoundStatement boundStatement;
    @Mock
    ColumnDefinitions columnDefinitions;
    @Mock
    PreparedStatementCache mockCache;

    EnhancedCqlTemplate template;

    @Before
    public void setup() {
        this.template = new EnhancedCqlTemplate();
        this.template.setSession(session);
    }

    @Test // DATACASS-555
    public void queryWithArgsAndWithCache() {

        template.setPreparedStatementCache(mockCache);
        when(mockCache.getPreparedStatement(Mockito.any(Session.class), Mockito.any(RegularStatement.class))).thenReturn(preparedStatement);

        when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
        when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

        List<Object> resultList = template.query("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK", "Walter");
        assertNotNull(resultList);
        assertEquals(1, resultList.size());
        assertThat((String) resultList.get(0)).isEqualTo("OK");

        verify(mockCache, times(1)).getPreparedStatement(Mockito.any(Session.class), Mockito.any(RegularStatement.class));
    }

    @Test // DATACASS-555
    public void queryForObjectPreparedStatementShouldReturnRecordWithCache() {

        template.setPreparedStatementCache(mockCache);
        when(mockCache.getPreparedStatement(Mockito.any(Session.class), Mockito.any(RegularStatement.class))).thenReturn(preparedStatement);

        when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
        when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(Collections.singleton(row).iterator());

        String result = template.queryForObject("SELECT * FROM user WHERE username = ?", (row, rowNum) -> "OK", "Walter");
        assertThat(result).isEqualTo("OK");

        verify(mockCache, times(1)).getPreparedStatement(Mockito.any(Session.class), Mockito.any(RegularStatement.class));
    }

    @Test // DATACASS-555
    public void queryForListPreparedStatementWithTypeShouldReturnRecordWithCache() {

        template.setPreparedStatementCache(mockCache);
        when(mockCache.getPreparedStatement(Mockito.any(Session.class), Mockito.any(RegularStatement.class))).thenReturn(preparedStatement);

        when(session.prepare("SELECT * FROM user WHERE username = ?")).thenReturn(preparedStatement);
        when(preparedStatement.bind("Walter")).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(Arrays.asList(row, row).iterator());
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(columnDefinitions.size()).thenReturn(1);
        when(row.getString(0)).thenReturn("OK", "NOT OK");

        List<String> result = template.queryForList("SELECT * FROM user WHERE username = ?", String.class, "Walter");

        assertThat(result).contains("OK", "NOT OK");

        verify(mockCache, times(1)).getPreparedStatement(Mockito.any(Session.class), Mockito.any(RegularStatement.class));
    }

}
