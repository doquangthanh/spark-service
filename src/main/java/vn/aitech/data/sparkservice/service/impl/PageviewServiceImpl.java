/**
 * 
 */
package vn.aitech.data.sparkservice.service.impl;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import scala.Tuple2;
import vn.aitech.data.entities.Pageview;
import vn.aitech.data.entities.User;
import vn.aitech.data.sparkservice.entities.Footprint;
import vn.aitech.data.sparkservice.entities.FootprintRowReader.FootprintRowReaderFactory;
import vn.aitech.data.sparkservice.entities.JsonKey;
import vn.aitech.data.sparkservice.entities.PageviewRowReader.PageviewRowReaderFactory;
import vn.aitech.data.sparkservice.entities.SummaryPhoneTelco;
import vn.aitech.data.sparkservice.entities.SummaryPhoneTelcoRowReader.SummaryPhoneTelcoRowReaderFactory;
import vn.aitech.data.sparkservice.entities.SummaryTelco;
import vn.aitech.data.sparkservice.entities.SummaryTelcoRowReader;
import vn.aitech.data.sparkservice.entities.SummaryTelcoRowReader.SummaryTelcoRowReaderFactory;
import vn.aitech.data.sparkservice.entities.UserRowReader.UserRowReaderFactory;
import vn.aitech.data.sparkservice.entities.Visitor;
import vn.aitech.data.sparkservice.entities.VisitorRowReader.VisitorRowReaderFactory;
import vn.aitech.data.sparkservice.service.PageviewService;
import vn.aitech.data.sparkservice.utils.Constants;
import vn.aitech.data.sparkservice.utils.MainCassandraSparkTemplate;
import vn.aitech.data.sparkservice.utils.SparkServiceUtils;

/**
 * @author thanhdq
 *
 */
public class PageviewServiceImpl implements PageviewService, Serializable {

	private static final long serialVersionUID = 1L;
	private PageviewRowReaderFactory pageviewRowReader;
	private static final Logger LOGGER = Logger.getLogger(PageviewServiceImpl.class);
	private static final Properties sparkProps = SparkServiceUtils.loadConfig("datacom-spark.properties");

	public PageviewServiceImpl(PageviewRowReaderFactory pageviewRowReader) {
		this.pageviewRowReader = pageviewRowReader;
	}

	@Override
	public JavaRDD<Pageview> search(JavaSparkContext javaSparkContext, String path, Map<String, Object> condMap) {
		MainCassandraSparkTemplate cassandraSparkTmpl = new MainCassandraSparkTemplate();
		JavaRDD<Pageview> pageviewRDD = cassandraSparkTmpl.getPageviews(javaSparkContext, path,
				new Function<Pageview, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Pageview v1) throws Exception {

						return SparkServiceUtils.isValid(v1, condMap);// &&isvalidDateRange;
					}
				});

		return pageviewRDD.cache();
	}

	@Override
	public List<Map<String, Integer>> summaryPhoneByTelco(JavaRDD<Pageview> pageviewRDD) {
		pageviewRDD.repartition(pageviewRDD.getNumPartitions());
		LOGGER.debug("Begin grouping pageview by telco...");
		JavaPairRDD<String, Iterable<Pageview>> pageviewPairRDD = pageviewRDD.groupBy(new Function<Pageview, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Pageview v1) throws Exception {
				return v1.getTelco();
			}

		});
		LOGGER.debug("Grouping Unique phone by telco...");
		JavaRDD<Map<String, Integer>> result = pageviewPairRDD
				.map(new Function<Tuple2<String, Iterable<Pageview>>, Map<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, Integer> call(Tuple2<String, Iterable<Pageview>> v1) throws Exception {
						Iterator<Pageview> iterator = v1._2().iterator();
						Set<String> uniquePhone = new HashSet<String>();
						while (iterator.hasNext()) {
							uniquePhone.add(iterator.next().getTelephone());
						}
						Map<String, Integer> uniqePhonemap = new HashMap<String, Integer>();
						uniqePhonemap.put(v1._1(), uniquePhone.size());
						return uniqePhonemap;
					}
				});

		return result.collect();
	}

	@Override
	public List<Pageview> getFootPrint(JavaSparkContext javaSparkContext, List<String> domains,String clientId,
			boolean isForward) {		
		JavaRDD<Footprint> rdd = javaFunctions(javaSparkContext)
				.cassandraTable(sparkProps.getProperty(Constants.BATCHLAYER_KEYSPACE), sparkProps.getProperty(Constants.BATCHLAYER_FOOTPRINT_TABLE),new FootprintRowReaderFactory()).filter(new Function<Footprint, Boolean>() {
					@Override
					public Boolean call(Footprint v1) throws Exception {
						return SparkServiceUtils.isValidDomain(domains, v1.getDomain())&&StringUtils.equalsIgnoreCase(clientId, v1.getClientId());						
					}
				});
		 List<Pageview>  result=new ArrayList<Pageview>();
		for(Footprint footprint:rdd.collect()){
			result.addAll(new Gson().fromJson(footprint.getHistory(),new TypeToken<List<Pageview>>(){}.getType()));
		}
		result.sort(new Comparator<Pageview>() {

			@Override
			public int compare(Pageview o1, Pageview o2) {				
				return o1.getTc().compareTo(o2.getTc());
			}
		});
		if(!isForward)
			result=Lists.reverse(result);
		return result;
	}

	@Override
	public JavaRDD<Pageview> search(JavaRDD<Pageview> pageviewRDD, Map<String, Object> condMap) {
		MainCassandraSparkTemplate cassandraSparkTmpl = new MainCassandraSparkTemplate();
		return cassandraSparkTmpl.getEntities(pageviewRDD, new Function<Pageview, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Pageview arg0) throws Exception {

				return SparkServiceUtils.isValid(arg0, condMap);
			}

		});

	}

	@Override
	public String summaryByTelco(JavaSparkContext context, List<String> domains) {
		 JavaRDD<SummaryTelco> rdd=javaFunctions(context).cassandraTable(sparkProps.getProperty(Constants.BATCHLAYER_KEYSPACE), sparkProps.getProperty(Constants.BATCHLAYER_TELCO_TABLE), new SummaryTelcoRowReaderFactory()).filter(new Function<SummaryTelco, Boolean>() {

			@Override
			public Boolean call(SummaryTelco v1) throws Exception {				
				return SparkServiceUtils.isValidDomain(domains,v1.getDomain());
			}
		});
		return new Gson().toJson(rdd.mapToPair(new PairFunction<SummaryTelco, String, Long>() {
			@Override
			public Tuple2<String, Long> call(SummaryTelco t) throws Exception {				
				return new Tuple2<>(t.getTelco(), t.getPageview());
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			@Override
			public Long call(Long v1, Long v2) throws Exception {				
				return v1+v2;
			}
		}).collectAsMap());
	}

	@Override
	public JavaRDD<Visitor> getVisitor(JavaSparkContext context, List<String> domains) {
		return javaFunctions(context).cassandraTable(sparkProps.getProperty(Constants.BATCHLAYER_KEYSPACE), sparkProps.getProperty(Constants.BATCHLAYER_VISITOR_TABLE), new VisitorRowReaderFactory())
				.filter(new Function<Visitor, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Visitor v1) throws Exception {
						if (domains == null || domains.isEmpty()) {
							return true;
						} else {
							return SparkServiceUtils.isOverLap(v1.getDomains(), domains);
						}
					}
				});
		
	}
	
	public List<JsonKey> getVisitors(JavaSparkContext context, List<String> domains) {
		JavaRDD<Visitor> rdd=getVisitor(context, domains);
		List<JsonKey> list=new ArrayList<JsonKey>();
		JsonKey jsonKey;
		for(Visitor visitor: rdd.collect()){
			if(visitor.getInternetInfor()!=null&&!visitor.getInternetInfor().isEmpty()){
				jsonKey=new JsonKey(visitor.getClientId()+"|"+visitor.getPhone(),visitor.getInternetInfor());
			}else{
				jsonKey=new JsonKey(visitor.getClientId()+"|"+visitor.getPhone(),new ArrayList<User>());
			}			
			list.add(jsonKey);
		}
		return list;
	}

	@Override
	public String summaryPhoneByTelco(JavaSparkContext javaSparkContext, List<String> domains) {
		JavaRDD<SummaryPhoneTelco> rdd=javaFunctions(javaSparkContext).cassandraTable(sparkProps.getProperty(Constants.BATCHLAYER_KEYSPACE),
				sparkProps.getProperty(Constants.BATCHLAYER_PHONETELCO_TABLE),new SummaryPhoneTelcoRowReaderFactory()).filter(new Function<SummaryPhoneTelco, Boolean>() {
					@Override
					public Boolean call(SummaryPhoneTelco v1) throws Exception {						
						return SparkServiceUtils.isValidDomain(domains,v1.getDomain());
					}
				});
		
		return new Gson().toJson(rdd.groupBy(new Function<SummaryPhoneTelco, String>() {
			@Override
			public String call(SummaryPhoneTelco v1) throws Exception {				
				return v1.getTelco();
			}
		}).map(new Function<Tuple2<String,Iterable<SummaryPhoneTelco>>, Map<String,Integer>>() {

			@Override
			public Map<String, Integer> call(Tuple2<String, Iterable<SummaryPhoneTelco>> t) throws Exception {				
					Iterator<SummaryPhoneTelco> iterator=t._2.iterator();
					Set<String> set=new HashSet<String>();
					while(iterator.hasNext()){
						set.addAll(iterator.next().getPhones());
					}
					Map<String,Integer> result=new HashMap<String,Integer>();
					result.put(t._1, set.size());
					return result;
				
			}
		}).collect());		
	}

}
