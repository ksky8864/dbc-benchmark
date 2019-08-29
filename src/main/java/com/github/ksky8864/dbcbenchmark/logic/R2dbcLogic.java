package com.github.ksky8864.dbcbenchmark.logic;

//import static java.lang.System.*;
import static com.github.ksky8864.dbcbenchmark.TestUtil.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.r2dbc.client.R2dbc;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.pool.SimplePoolMetricsRecorder;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import reactor.core.publisher.Flux;

public class R2dbcLogic {

	private final R2dbc r2dbc;

	private final ConnectionPool pool;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	public R2dbcLogic() {
		final PostgresqlConnectionConfiguration pgConfig = PostgresqlConnectionConfiguration.builder()
				.host(getProperty("db.host"))
				.port(Integer.parseInt(getProperty("db.port", "5432")))
				.username(getProperty("db.username"))
				.password(getProperty("db.password"))
				.database(getProperty("db.database"))
				.build();

		final ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration
				.builder(new PostgresqlConnectionFactory(pgConfig))
				.metricsRecorder(new SimplePoolMetricsRecorder())
				.maxSize(Integer.parseInt(getProperty("db.poolsize", "64")))
				.build();
		this.pool = new ConnectionPool(poolConfig);
		this.r2dbc = new R2dbc(this.pool);
	}

	public Flux<Integer> insert() {
		return this.r2dbc.<Integer> withHandle(handle -> {
			return handle.createUpdate("insert into benchmark_test values ($1)")
					.bind("$1", "TEST")
					.execute();
		}).doOnError(t -> this.log.error("error while inserting a record", t));
	}

	public void truncate() {
		this.r2dbc.useHandle(handle -> {
			return handle.createUpdate("truncate table benchmark_test cascade")
					.execute();
		}).doOnError(t -> this.log.error("error while truncating test table", t)).block();
	}

	public void shutdown() {
		this.pool.close();
	}
}
