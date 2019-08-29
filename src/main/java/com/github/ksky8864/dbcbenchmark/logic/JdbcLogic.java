package com.github.ksky8864.dbcbenchmark.logic;

//import static java.lang.System.*;
import static com.github.ksky8864.dbcbenchmark.TestUtil.*;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class JdbcLogic {

	private final HikariDataSource ds;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final ExecutorService executors;

	public JdbcLogic() {
		final var config = new HikariConfig();
		config.setDriverClassName("org.postgresql.Driver");
		config.setUsername(getProperty("db.username"));
		config.setPassword(getProperty("db.password"));
		config.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s", getProperty("db.host"),
				getProperty("db.port", "5432"), getProperty("db.database")));
		final int maxPoolSize = Integer.parseInt(getProperty("db.poolsize", "64"));
		config.setMaximumPoolSize(maxPoolSize);
		config.setAutoCommit(false);
		this.ds = new HikariDataSource(config);
		this.executors = Executors.newFixedThreadPool(maxPoolSize);
	}

	public Future<Integer> insert() {
		return this.executors.submit(() -> doInsert());
	}

	private int doInsert() {
		int updCount = -1;
		try (var conn = this.ds.getConnection()) {
			try (var pstmt = conn.prepareStatement("insert into benchmark_test values (?)")) {
				pstmt.setString(1, "TEST");
				updCount = pstmt.executeUpdate();
				conn.commit();
			} catch (final SQLException e1) {
				conn.rollback();
				this.log.error("error while inserting a record", e1);
				throw new RuntimeException(e1);
			}
		} catch (final SQLException e2) {
			this.log.error("error while inserting a record", e2);
			throw new RuntimeException(e2);
		}
		return updCount;
	}

	public void truncate() {
		try (var conn = this.ds.getConnection();
				var pstmt = conn.prepareStatement("truncate table benchmark_test cascade");) {
			pstmt.execute();
		} catch (final SQLException e) {
			this.log.error("error while truncating test table", e);
			throw new RuntimeException(e);
		}
	}

	public void shutdown() {
		if (this.executors.isTerminated() == false) {
			this.executors.shutdownNow();
		}
		this.ds.close();
	}

	public void joinAll() {
		this.executors.shutdown();
		try {
			this.executors.awaitTermination(10, TimeUnit.SECONDS);
		} catch (final InterruptedException e) {
			this.log.error("error while awaiting termination of worker threads");
		}
	}
}
