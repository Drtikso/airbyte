package io.airbyte.cdk.db.jdbc;

import io.airbyte.cdk.db.jdbc.streaming.JdbcStreamingQueryConfig;
import io.airbyte.commons.functional.CheckedFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryableSpliterator<T> extends Spliterators.AbstractSpliterator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryableSpliterator.class);
    private static final int MAX_RETRIES = 5;

    private final DataSource dataSource;
    private final CheckedFunction<Connection, PreparedStatement, SQLException> statementCreator;
    private final CheckedFunction<ResultSet, T, SQLException> mapper;
    private final JdbcStreamingQueryConfig streamingConfig;

    private Exception streamException;
    private boolean isStreamFailed;

    private Connection connection = null;
    private ResultSet resultSet = null;
    private int retryCount = 0;
    private int recordsRead = 0;

    public RetryableSpliterator(DataSource dataSource,
                                CheckedFunction<Connection, PreparedStatement, SQLException> statementCreator,
                                CheckedFunction<ResultSet, T, SQLException> mapper,
                                JdbcStreamingQueryConfig streamingConfig) {
        super(Long.MAX_VALUE, Spliterator.ORDERED);
        this.dataSource = dataSource;
        this.statementCreator = statementCreator;
        this.mapper = mapper;
        this.streamingConfig = streamingConfig;

    }

    private void initializeStreamConnection() {
        try {
            connection = dataSource.getConnection();
            PreparedStatement statement = statementCreator.apply(connection);
            streamingConfig.initialize(connection, statement);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override public boolean tryAdvance(Consumer<? super T> action) {
        try {
            if (connection == null) {
                initializeStreamConnection();
            }
            if (!resultSet.next()) {
                LOGGER.info("{} Records emitted in total, closing result set", recordsRead);
                resultSet.close();
                return false;
            }
            recordsRead++;
            final T dataRow = mapper.apply(resultSet);
            streamingConfig.accept(resultSet, dataRow);
            action.accept(dataRow);
            return true;
        } catch (SQLException e) {
            if (retryCount < MAX_RETRIES) {
                LOGGER.warn("Retrying statement SQLState: {}, Message: {}", e.getSQLState(), e.getMessage());
                LOGGER.warn("{} records alredy emitted, starting from the beginning", recordsRead);
                recordsRead = 0;
                retryCount++;
                initializeStreamConnection();
                return tryAdvance(action);
            } else {
                LOGGER.error("Retrying statement failed after retrying SQLState: {}, Message: {}", e.getSQLState(), e.getMessage());
                streamException = e;
                isStreamFailed = true;
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        try {
            if (!connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
            connection.close();
            if (isStreamFailed) {
                throw new RuntimeException(streamException);
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
