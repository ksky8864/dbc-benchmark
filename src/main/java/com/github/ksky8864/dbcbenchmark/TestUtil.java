package com.github.ksky8864.dbcbenchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtil {
	private static final String INIT_FILE_PATH = "./benchmark.properties";

	private static final Properties properties;

	private static Logger log = LoggerFactory.getLogger(TestUtil.class);

	private TestUtil() throws Exception {
	}

	static {
		properties = new Properties();
		try {
			final var propFilePath = System.getProperty("app.properties");
			properties.load(Files.newBufferedReader(Paths.get(propFilePath), StandardCharsets.UTF_8));
		} catch (final IOException e) {
			log.error("failed to load properties {}", INIT_FILE_PATH);
			throw new RuntimeException(e);
		}
	}

	public static String getProperty(final String key) {
		return getProperty(key, "");
	}

	public static String getProperty(final String key, final String defaultValue) {
		return properties.getProperty(key, defaultValue);
	}

}
