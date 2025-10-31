package step.core.collections.postgresql;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class StreamingQuery implements AutoCloseable {
    private static final int FETCH_SIZE = 1000;
    private final Connection connection;
    private final PreparedStatement statement;
    // The resultSet is the only thing our "users" are interested in, the rest is just plumbing
    public final ResultSet resultSet;

    public StreamingQuery(DataSource ds, String sql) throws SQLException {
        this(ds, sql, 0);
    }

    public StreamingQuery(DataSource ds, String sql, int timeoutSeconds) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = ds.getConnection();
            conn.setReadOnly(true); // Should result in a "BEGIN READ ONLY" transaction which can help psql with optimizations
            conn.setAutoCommit(false); // required to obtain server-side cursor (as are the 2 next lines)
            stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(FETCH_SIZE);

            if (timeoutSeconds > 0) {
                stmt.setQueryTimeout(timeoutSeconds);
            }
            resultSet = stmt.executeQuery();
            // only initialize these fields after the resultSet was successfully created
            // (if something went wrong, they're closed via the catch below)
            statement = stmt;
            connection = conn;
        } catch (SQLException sqlEx) {
            try {
                closeAll(stmt, conn);
            } catch (Exception ex) {
                // if the closing throws something too, add that to the existing exception
                sqlEx.addSuppressed(ex);
            }
            throw sqlEx;
        }
    }


    @Override
    public void close() throws SQLException {
        closeAll(resultSet, statement, connection);
    }

    @FunctionalInterface
    private interface Task {
        void run() throws Exception;
    }

    private void closeAll(AutoCloseable... closeables) throws SQLException {
        // We really want to make sure that each of the tasks is at least attempted,
        // but every single one of them could in theory throw exceptions.
        // If something fails, we capture the exception but continue with the rest of the tasks.
        // Whatever else fails afterward (which might be possible) gets added to the existing exception.
        List<Task> tasks = new ArrayList<>();

        for (AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }
            if (closeable instanceof Connection) {
                Connection conn = (Connection) closeable;
                // Additional steps to perform for Connections, before closing it:
                // Explicitly return the connection to the state in which we received it from the pool.
                // This avoids overhead from the pool rolling back unfinished transactions
                // and re-initializing the connection state again.
                tasks.add(() -> {
                    if (!conn.isClosed() && !conn.getAutoCommit()) {
                        // Required to release the cursor, even if technically there's nothing to commit
                        conn.commit();
                    }
                });
                tasks.add(() -> conn.setReadOnly(false)); // Don't leak read-only connections!
                tasks.add(() -> conn.setAutoCommit(true)); // Reset to pool connection default
            }
            // this is the actual close() call
            tasks.add(closeable::close);
        }

        // Now, run all of that
        try {
            runAll(tasks);
        } catch (Exception ex) {
            if (ex instanceof SQLException) {
                // normally, this is the kind of exception we expect (if any)
                throw (SQLException) ex;
            } else {
                // Otherwise wrap
                throw new SQLException(ex);
            }
        }
    }

    // Helper method to run all tasks in sequence and handle exceptions as described above
    private static void runAll(List<Task> tasks) throws Exception {
        Exception mainException = null;
        for (Task task : tasks) {
            try {
                task.run();
            } catch (Exception e) {
                if (mainException == null) {
                    mainException = e;
                } else {
                    mainException.addSuppressed(e);
                }
            }
        }
        if (mainException != null) throw mainException;
    }
}
