/**
 * 
 */
package vn.aitech.data.sparkservice.service;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraRow;

import vn.aitech.data.entities.Pageview;
import vn.aitech.data.entities.User;
import vn.aitech.data.sparkservice.entities.Footprint;
import vn.aitech.data.sparkservice.entities.JsonKey;
import vn.aitech.data.sparkservice.entities.Visitor;

/**
 * @author thanhdq
 *
 */
public interface PageviewService extends Serializable{
	
	public List<Map<String, Integer>> summaryPhoneByTelco(JavaRDD<Pageview> pageviewRDD);
	public String summaryPhoneByTelco(JavaSparkContext javaSparkContext,List<String> domains);	
	public JavaRDD<Pageview> search(JavaSparkContext javaSparkContext,String path,Map<String,Object> condMap);
	public JavaRDD<Pageview> search(JavaRDD<Pageview> pageviewRDD,Map<String,Object> condMap);	
	public JavaRDD<Visitor> getVisitor(JavaSparkContext context,List<String> domains);	
	public List<JsonKey> getVisitors(JavaSparkContext context, List<String> domains);
	public List<Pageview>  getFootPrint(JavaSparkContext javaSparkContext,List<String> domains,String clientId,boolean isForward);
	public String summaryByTelco(JavaSparkContext javaSparkContext, List<String> domains);
}
