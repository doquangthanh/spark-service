/**
 * 
 */
package vn.aitech.data.sparkservice.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cglib.beans.BeanGenerator;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import vn.aitech.data.sparkservice.entities.JsonKey;
import vn.aitech.data.sparkservice.utils.SparkServiceUtils;

/**
 * @author thanhdq
 *
 */
@SpringBootApplication
public class MainSparkService {
	private static final Logger logger = Logger.getLogger(MainSparkService.class);
	private static final Properties props = SparkServiceUtils.loadConfig("datacom-spark.properties");

	public static void main(String[] args) {
		logger.debug("Loading port of server in the config file.");
		try {
			int port = Integer.valueOf(props.get("spark.service.server.port").toString());
			System.getProperties().put("server.port", port);
		} catch (NumberFormatException e) {
			logger.error("there is not port or port is incorrect(must be number): " + e.getMessage());
		}
		SpringApplication.run(MainSparkService.class, args);

	}

	


}
