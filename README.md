# dbc-benchmark

a simple benchmark test code for JDBC and R2DBC.

## Building from Source

```shell
mvn clean package
```

After running maven command, benchmarks.jar will be made in `target/`

## Run benchmark test

Please set database properties in `src/main/resources/database.properties` before you run benchmark.

```shell
java -Dapp.properties=/path/to/benchmark.properties -jar benchmarks.jar
```
