/**
 * 
 */
package vn.aitech.data.sparkservice.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.google.gson.Gson;

import vn.aitech.data.entities.Pageview;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.Serializable;

/**
 * @author thanhdq
 *
 */
public class MainCassandraSparkTemplate implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public <T> JavaRDD<String> getEntities(JavaSparkContext javaSparkContext,String path,Function<T, Boolean> filter){
		return javaSparkContext.textFile(path);
	}
	public  JavaRDD<Pageview> getPageviews(JavaSparkContext javaSparkContext,String path,Function<Pageview, Boolean> filter){
		JavaRDD<String> rdd=javaSparkContext.textFile(path);
		return rdd.map(new Function<String, Pageview>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Pageview call(String pageviewJson) throws Exception {				
				return new Gson().fromJson(pageviewJson,Pageview.class);
			}
		}).filter(filter);
		 
	}
	public <T> JavaRDD<T> getEntities(JavaRDD<T> javaRDD,Function<T, Boolean> filter){
		return javaRDD.filter(filter);
	}
}
