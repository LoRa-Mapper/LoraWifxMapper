package com.app.spark.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to read property file
 * 
 * @author abaghel
 *
 */
public class LoadConfig {
	private static final Logger logger = Logger.getLogger(LoadConfig.class);
	private static Properties prop = new Properties();
	public static Properties readPropertyFile() throws Exception {
		if (prop.isEmpty()) {
			InputStream input = LoadConfig.class.getClassLoader().getResourceAsStream("spark.properties");
			try {
				prop.load(input);
			} catch (IOException ex) {
				logger.error(ex);
				throw ex;
			} finally {
				if (input != null) {
					input.close();
				}
			}
		}
		return prop;
	}
}
