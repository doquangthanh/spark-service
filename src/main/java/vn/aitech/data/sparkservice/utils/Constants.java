/**
 * 
 */
package vn.aitech.data.sparkservice.utils;

import java.io.Serializable;

/**
 * @author thanhdq
 *
 */
public class Constants implements Serializable{
	
	public static final String BATCHLAYER_KEYSPACE="cassandra.batchlayer.keyspace.name";
	public static final String BATCHLAYER_REPORT_TABLE="cassandra.batchlayer.report.name";	
	public static final String BATCHLAYER_VISITOR_TABLE="cassandra.batchlayer.visitor.name";
	public static final String BATCHLAYER_HOUR_TABLE="cassandra.batchlayer.hour.name";
	public static final String BATCHLAYER_HOURTELCO_TABLE="cassandra.batchlayer.hourtelco.name";
	public static final String BATCHLAYER_FOOTPRINT_TABLE="cassandra.batchlayer.footprint.name";
	public static final String BATCHLAYER_PHONETELCO_TABLE="cassandra.batchlayer.phonetelco.name";
	public static final String BATCHLAYER_TELCO_TABLE="cassandra.batchlayer.telco.name";
	public static final String REPORT_VISITOR="visitor";
	public static final String USER_TABLE="user";
	public static final String PHONE_CRAWLER="phonecrawler";
	public static final String USER="user";
	public static final String DATE_FORMAT="yyyyy-MM-dd";
	public static final String KEY_SPACE="datacom";
	public static final String VALUE_NOT_FOUND="undefined";
	public static final String PHONE_NOT_FOUND_3G="3G";
	public static final String TELCO_NOT_FOUND="other";
	public static final String SORT_TYPE_ASC="asc";
	

}
