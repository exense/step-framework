package step.core.collections.postgresql;

import javax.sql.DataSource;
import java.sql.*;

public class StreamingQuery implements AutoCloseable {
    private static final int FETCH_SIZE = 1000;
    private final Connection connection;
    private final PreparedStatement statement;
    public final ResultSet resultSet;

    public StreamingQuery(DataSource ds, String sql) throws SQLException {
        this(ds, sql, 0);
    }

    public StreamingQuery(DataSource ds, String sql, int timeoutSeconds) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(FETCH_SIZE);
            if (timeoutSeconds > 0) {
                stmt.setQueryTimeout(timeoutSeconds);
            }
            resultSet = stmt.executeQuery();
            statement = stmt;
            connection = conn;
        } catch (SQLException sqlEx) {
            try {
                close(stmt, conn);
            } catch (Exception ex) {
                sqlEx.addSuppressed(ex);
            }
            throw sqlEx;
        }
    }

    private void close(AutoCloseable... closeables) throws SQLException {
        SQLException exception = null;

        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (exception == null) {
                        if (e instanceof SQLException) {
                            exception = (SQLException) e;
                        } else {
                            exception = new SQLException(e.getMessage(), e);
                        }
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void close() throws SQLException {
        close(resultSet, statement, connection);
    }
}
