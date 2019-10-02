package com.github.ksky8864.dbcbenchmark.logic;

import static com.github.ksky8864.dbcbenchmark.TestUtil.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcLogic {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final ExecutorService executors;

	private final String url;

	public JdbcLogic() {
		this.executors = Executors.newFixedThreadPool(Integer.parseInt(getProperty("db.poolsize", "64")));
		this.url = String.format("jdbc:postgresql://%s:%s/%s", getProperty("db.host"),
				getProperty("db.port", "5432"), getProperty("db.database"));
	}

	public Future<Integer> insert() {
		return this.executors.submit(() -> doInsert());
	}

	private int doInsert() {
		int updCount = -1;
		try (var conn = DriverManager.getConnection(this.url, "rtfa", "rtfa")) {
			conn.setAutoCommit(false);
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
		try (var conn = DriverManager.getConnection(this.url, "rtfa", "rtfa");
				final var pstmt = conn.prepareStatement("truncate table benchmark_test cascade");) {
			conn.setAutoCommit(false);
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
	}

	public void joinAll() {
		this.executors.shutdown();
		try {
			this.executors.awaitTermination(120, TimeUnit.SECONDS);
		} catch (final InterruptedException e) {
			this.log.error("error while awaiting termination of worker threads");
		}
	}
}
