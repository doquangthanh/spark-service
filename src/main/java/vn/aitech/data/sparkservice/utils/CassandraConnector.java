/**
 * 
 */
package vn.aitech.data.sparkservice.utils;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import vn.aitech.data.sparkservice.config.SparkConfig;
import vn.aitech.data.sparkservice.entities.PageviewRowReader.PageviewRowReaderFactory;

/**
 * @author thanhdq
 *
 */
public class CassandraConnector implements Serializable {
	
	private static final JavaSparkContext instance= new JavaSparkContext(new SparkConfig().getSparkConfig());

	private CassandraConnector() {
		

	}

	public static JavaSparkContext getInstance() {
		return instance;
	}	
	public  PageviewRowReaderFactory getPageviewRowReaderFactory(){
		return new PageviewRowReaderFactory();
	}

}
