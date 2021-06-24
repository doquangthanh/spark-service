/**
 * 
 */
package vn.aitech.data.sparkservice.api;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.gson.Gson;

import vn.aitech.data.entities.Pageview;
import vn.aitech.data.entities.User;
import vn.aitech.data.sparkservice.entities.JsonKey;
import vn.aitech.data.sparkservice.entities.PageviewRowReader.PageviewRowReaderFactory;
import vn.aitech.data.sparkservice.entities.Report;
import vn.aitech.data.sparkservice.entities.ReportRowReader.ReportRowReaderFactory;
import vn.aitech.data.sparkservice.entities.UserRowReader.UserRowReaderFactory;
import vn.aitech.data.sparkservice.entities.Visitor;
import vn.aitech.data.sparkservice.service.PageviewService;
import vn.aitech.data.sparkservice.service.impl.PageviewServiceImpl;
import vn.aitech.data.sparkservice.utils.CassandraConnector;
import vn.aitech.data.sparkservice.utils.Constants;
import vn.aitech.data.sparkservice.utils.SparkServiceUtils;

/**
 * @author thanhdq
 *
 */

@Controller
@RequestMapping("/data/db/api")
public class SparkAPI implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final JavaSparkContext sparkCcontext = CassandraConnector.getInstance();
	private static final Gson gson = new Gson();
	private static final Logger logger = Logger.getLogger(SparkAPI.class);
	private static final Properties props=SparkServiceUtils.loadConfig("data-spark.properties");
	private static final PageviewService pageviewService = new PageviewServiceImpl(new PageviewRowReaderFactory());

	@RequestMapping(method = RequestMethod.POST, value = "/report/search")
	public ResponseEntity<String> search(@RequestBody Map<String, Object> qMap) {

		logger.debug("Input parameter for filter :" + gson.toJson(qMap));
		@SuppressWarnings("unchecked")
		Map<String, Object> conditionMap = (Map<String, Object>) qMap.get("search");
		
		JavaRDD<Pageview> select = loadAllPageview(props.getProperty(Constants.BATCHLAYER_KEYSPACE),props.getProperty(Constants.BATCHLAYER_REPORT_TABLE));		
		Map<String, String> pageMap = (Map<String, String>) qMap.get("page");
		int pagesize = Integer.valueOf(pageMap.get("pagesize"));
		int pageNumber = Integer.valueOf(pageMap.get("pagenumber"));
		int total = 5000;// Integer.parseInt(select.count()+"");
		Map<String, Object> resultMap = SparkServiceUtils.paging(select, total, pagesize, pageNumber);
		return new ResponseEntity<String>(gson.toJson(resultMap), HttpStatus.OK);

	}

	@RequestMapping(method = RequestMethod.POST, value = "/report/inforClientWithPhone")
	public ResponseEntity<String> inforClientWithPhone(@RequestBody Map<String, Object> qMap) {
		logger.debug("Input parameters for getting information of Customers:" + gson.toJson(qMap));
		Map<String, String> searchMap = (Map<String, String>) qMap.get("search");
		List<String> domains = null;
		if (searchMap != null && !searchMap.isEmpty()) {
			String domainString = searchMap.get("dm") == null ? null : searchMap.get("dm").toString();
			domains = StringUtils.isEmpty(domainString) ? null
					: Arrays.asList(domainString.replace("(", "").replace(")", "").split(","));
		}
		Map<String, String> pageMap = (Map<String, String>) qMap.get("page");
		int pagesize = Integer.valueOf(pageMap.get("pagesize"));
		int pageNumber = Integer.valueOf(pageMap.get("pagenumber"));
		List<JsonKey> result = pageviewService.getVisitors(sparkCcontext, domains);
		//JavaRDD<JsonKey> list=
		int total = result.size();
		return new ResponseEntity<String>(gson.toJson(SparkServiceUtils.paging(result, total, pagesize, pageNumber)),
				HttpStatus.OK);

	}

	@RequestMapping(method = RequestMethod.POST, value = "/report/summaryPhoneByTelco")
	public ResponseEntity<String> summaryPhoneByTelco(@RequestBody Map<String, Object> domainMap) {
		logger.debug("Input parameter for summary of unique phone by telco :" + gson.toJson(domainMap));
		Map<String,String> searchMap=(Map<String,String>)domainMap.get("search");
		List<String> domains = searchMap.isEmpty() ? null : Arrays.asList(searchMap.get("dm").split(","));
		String result = pageviewService.summaryPhoneByTelco(sparkCcontext, domains);
		return new ResponseEntity<String>(result, HttpStatus.OK);

	}

	@RequestMapping(method = RequestMethod.POST, value = "/report/summaryByTelco")
	public ResponseEntity<String> summaryByTelco(@RequestBody Map<String, Object> domainMap) {
		// try {
		logger.debug("Input parameter for summary of unique phone by telco :" + gson.toJson(domainMap));
		Map<String,String> searchMap=(Map<String,String>)domainMap.get("search");
		List<String> domains = domainMap.isEmpty()||searchMap.get("dm")==null ? null : Arrays.asList(searchMap.get("dm").split(","));
		String result = pageviewService.summaryByTelco(sparkCcontext, domains);
		return new ResponseEntity<String>(result, HttpStatus.OK);

	}

	@RequestMapping(method = RequestMethod.POST, value = "/report/footprint")
	public ResponseEntity<String> footprint(@RequestBody Map<String, Object> clientIdMap) {
		// try {
		logger.debug("Input parameter for retriving footprint of client :" + gson.toJson(clientIdMap));
		Map<String, Object> map = (Map<String, Object>) clientIdMap.get("search");
		Map<String, Object> sortMap = (Map<String, Object>) clientIdMap.get("sort");
		List<String> domains = map.isEmpty()||map.get("dm")==null ? null : Arrays.asList(map.get("dm").toString().split(","));
		String clientId=map.get("clientid").toString();
		boolean asc = sortMap == null || sortMap.isEmpty() ? true
				: StringUtils.equalsIgnoreCase(sortMap.get("tc").toString(), "asc");
		List<Pageview> result = pageviewService.getFootPrint(sparkCcontext, domains,clientId, asc);
		return new ResponseEntity<String>(gson.toJson(result), HttpStatus.OK);

	}

	private JavaRDD<Report> loadReports(String keyspace, String table) {
		JavaRDD<Report> rdd = javaFunctions(sparkCcontext).cassandraTable(keyspace, table,
				new ReportRowReaderFactory());
		return rdd.sortBy(new Function<Report, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Report arg0) throws Exception {

				return arg0.getReportDate();
			}
		}, false, rdd.getNumPartitions());
	}

	private JavaRDD<Pageview> loadAllPageview(String keyspace, String table) {
		JavaRDD<Report> sortedReportRDD = loadReports(keyspace, table);
		List<Report> reports = sortedReportRDD.collect();
		JavaRDD<String> all = sparkCcontext.parallelize(new ArrayList<String>());
		for (Report report : reports) {
			all = all.union(sparkCcontext.textFile(report.getSearchResultPath()));
		}
		// System.out.println("total: "+all.count());
		return all.map(new Function<String, Pageview>() {

			@Override
			public Pageview call(String v1) throws Exception {
				return new Gson().fromJson(v1, Pageview.class);
			}
		}).cache();

	}

}
