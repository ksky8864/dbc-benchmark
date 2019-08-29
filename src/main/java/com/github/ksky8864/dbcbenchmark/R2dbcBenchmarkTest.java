package com.github.ksky8864.dbcbenchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.github.ksky8864.dbcbenchmark.logic.R2dbcLogic;

import reactor.core.publisher.Flux;

@BenchmarkMode(Mode.SingleShotTime)
@State(Scope.Benchmark)
public class R2dbcBenchmarkTest {

	private R2dbcLogic benchmarkLogic;

	@State(Scope.Benchmark)
	public static class Parameters {
		@Param({ "50" })
		String count;
	}

	@Setup(Level.Iteration)
	public void setUp() {
		this.benchmarkLogic = new R2dbcLogic();
		this.benchmarkLogic.truncate();
	}

	@TearDown(Level.Iteration)
	public void tearDown() {
		this.benchmarkLogic.shutdown();
	}

	@Benchmark
	@Warmup(iterations = 1)
	@Measurement(iterations = 3)
	@Fork(0)
	public void run(final Parameters param) {
		Flux.range(0, Integer.parseInt(param.count)).flatMap(i -> this.benchmarkLogic.insert()).blockLast();
	}
}
