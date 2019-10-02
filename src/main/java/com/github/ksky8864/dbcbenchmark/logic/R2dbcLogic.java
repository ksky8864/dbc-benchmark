package com.github.ksky8864.dbcbenchmark.logic;

import static com.github.ksky8864.dbcbenchmark.TestUtil.*;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.r2dbc.client.R2dbc;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;

public class R2dbcLogic {

	private R2dbc r2dbc = null;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final ConnectionFactory factory;

	public R2dbcLogic() {
		final PostgresqlConnectionConfiguration pgConfig = PostgresqlConnectionConfiguration.builder()
				.host(getProperty("db.host"))
				.port(Integer.parseInt(getProperty("db.port", "5432")))
				.username(getProperty("db.username"))
				.password(getProperty("db.password"))
				.database(getProperty("db.database"))
				.connectTimeout(Duration.ofSeconds(30))
				.build();
		this.factory = new PostgresqlConnectionFactory(pgConfig);
		this.r2dbc = new R2dbc(new PostgresqlConnectionFactory(pgConfig));
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
}
