/**
 * 
 */
package vn.aitech.data.sparkservice.utils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import vn.aitech.data.sparkservice.entities.ConditionBuilder;

/**
 * @author thanhdq
 *
 */
public class SparkServiceUtils implements Serializable {
	public static <T> Type  createType(Class<T> clazz){	
		return new TypeToken<T>(){}.getType();
	}
	public static boolean isOverLap( List<String> list1, List<String> list2) {   
	    ArrayList<String> result = new ArrayList<String>(list1);

	    result.retainAll(list2);

	    return !result.isEmpty();
	}
	public static boolean set(Object object, String fieldName, Object fieldValue) {
		Class<?> clazz = object.getClass();
		while (clazz != null) {
			try {
				Field field = clazz.getDeclaredField(fieldName);
				field.setAccessible(true);
				field.set(object, fieldValue);
				return true;
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public static <V> V get(Object object, String fieldName) {
		Class<?> clazz = object.getClass();
		while (clazz != null) {
			try {
				Field field = clazz.getDeclaredField(fieldName);
				field.setAccessible(true);
				return (V) field.get(object);
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		return null;
	}

	public static <T> Boolean checkValid(Object object, List<ConditionBuilder> conditionBuilderArray) {

		Object originValue = null;
		boolean result = true;

		if (conditionBuilderArray == null || conditionBuilderArray.isEmpty()) {
			return true;
		} else {
			for (ConditionBuilder conditionBuilder : conditionBuilderArray) {
				originValue = get(object, conditionBuilder.getName());
				if (originValue == null) {
					return false;
				}
				if (conditionBuilder.getTarget() instanceof String) {
					if (conditionBuilder.getOperator().equalsIgnoreCase("!")) {
						result = !Arrays.asList(conditionBuilder.getTarget().toString().toUpperCase().split(","))
								.contains(originValue.toString().toUpperCase());
					} else {
						result = Arrays
								.asList(conditionBuilder.getTarget().toString().toString().toUpperCase().split(","))
								.contains(originValue.toString().toUpperCase());
					}

				} else if (conditionBuilder.getTarget() instanceof Boolean) {
					return Boolean.parseBoolean(originValue.toString()) == Boolean
							.parseBoolean(conditionBuilder.getTarget().toString());

				} else {
					if (conditionBuilder.getOperator() != null
							&& conditionBuilder.getOperator().equalsIgnoreCase("!")) {
						result = !(Long.parseLong(originValue.toString()) == Long
								.parseLong(conditionBuilder.getTarget().toString()));
					} else if (conditionBuilder.getOperator().equalsIgnoreCase(">=")) {
						result = Long.parseLong(originValue.toString()) >= Long
								.parseLong(conditionBuilder.getTarget().toString());

					} else if (conditionBuilder.getOperator().equalsIgnoreCase("<")) {
						result = Long.parseLong(originValue.toString()) < Long
								.parseLong(conditionBuilder.getTarget().toString());

					} else if (conditionBuilder.getOperator().equalsIgnoreCase("<=")) {
						result = Long.parseLong(originValue.toString()) <= Long
								.parseLong(conditionBuilder.getTarget().toString());
					} else {
						result = Long.parseLong(originValue.toString()) == Long
								.parseLong(conditionBuilder.getTarget().toString());
					}
				}
				if (!result)
					return result;
			}
			return result;
		}
	}

	public static List<ConditionBuilder> buildCondition(Map<String, Object> conditionMap) {

		if (conditionMap == null || conditionMap.isEmpty()) {
			return null;
		} else {
			List<ConditionBuilder> conditionBuilders = new ArrayList<ConditionBuilder>();
			Iterator<String> iterator = conditionMap.keySet().iterator();
			String key;
			ConditionBuilder conditionBuilder;
			Object target;
			String operator;
			while (iterator.hasNext()) {
				conditionBuilder = new ConditionBuilder();
				key = iterator.next();
				target = conditionMap.get(key);
				if (key.equals("daterange")) {
					if (conditionMap.get("daterange") != null) {
						Map<String, String> dateRangeMap = (Map<String, String>) conditionMap.get("daterange");
						Long from = SparkServiceUtils.convertStringToDate(dateRangeMap.get("from")).getMillis() / 1000;
						Long to = SparkServiceUtils.convertStringToDate(dateRangeMap.get("to")).plusDays(1).getMillis()
								/ 1000;
						conditionBuilder.setName("tc");
						conditionBuilder.setOperator(">=");
						conditionBuilder.setTarget(from);
						conditionBuilders.add(conditionBuilder);
						conditionBuilder = new ConditionBuilder();
						conditionBuilder.setName("tc");
						conditionBuilder.setOperator("<");
						conditionBuilder.setTarget(to);
						conditionBuilders.add(conditionBuilder);
					}

				} /*
					 * else if(key.equals("detectedmobile")){
					 * conditionBuilder.setName("detectedmobile");
					 * conditionBuilder.setOperator("=");
					 * conditionBuilder.setTarget(target);
					 * conditionBuilders.add(conditionBuilder); }
					 */else {
					if (target instanceof String) {
						if (StringUtils.trim(target.toString()).startsWith("!")) {
							operator = "!";
							target = ((String) target).replace("!", "").replace("(", "").replace(")", "").toUpperCase();

						} else {
							operator = "in";
						}
					} else {
						operator = "=";
					}
					conditionBuilder.setName(key);
					conditionBuilder.setOperator(operator);
					conditionBuilder.setTarget(target);
					conditionBuilders.add(conditionBuilder);
				}

			}
			return conditionBuilders;
		}
	}

	/**
	 * 
	 * @param object
	 * @param conditionMap
	 * @return
	 */
	public static boolean isValid(Object object, Map<String, Object> conditionMap) {
		List<ConditionBuilder> list = buildCondition(conditionMap);
		return checkValid(object, list);
	}

	public static Properties loadConfig(String path) {
		Properties props = new Properties();
		try {
			// load a properties file from class path, inside static method
			props.load(SparkServiceUtils.class.getClassLoader().getResourceAsStream(path));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return props;
	}

	public static String getTelcoByPhone(String phone, String jsonTelcoMap) {

		if (StringUtils.isEmpty(phone) || StringUtils.endsWithIgnoreCase(phone, Constants.VALUE_NOT_FOUND)) {
			return Constants.TELCO_NOT_FOUND;
		}
		Gson gson = new Gson();
		Map<String, String> telcoMap = gson.fromJson(jsonTelcoMap, Map.class);
		Iterator<String> iterator = telcoMap.keySet().iterator();
		String telco;
		StringBuffer matchingPhones = new StringBuffer("^");
		String originnalPhones;
		while (iterator.hasNext()) {
			matchingPhones = new StringBuffer("^");
			telco = iterator.next();
			originnalPhones = telcoMap.get(telco);
			matchingPhones.append(originnalPhones.replaceAll(",", "|^"));
			// add prefix 0
			matchingPhones.append("|^");
			originnalPhones = "0" + telcoMap.get(telco);
			matchingPhones.append(originnalPhones.replaceAll(",", "|^0"));
			// add prefix 84
			matchingPhones.append("|^");
			originnalPhones = "84" + telcoMap.get(telco);
			matchingPhones.append(originnalPhones.replaceAll(",", "|^84"));
			Pattern r = Pattern.compile(matchingPhones.toString());
			Matcher m = r.matcher(phone);
			if (m.find()) {
				return telco;
			}
		}

		return Constants.TELCO_NOT_FOUND;
	}

	public static boolean isValidPhone(String phone) {
		return !(StringUtils.isEmpty(phone) && StringUtils.equalsIgnoreCase(phone, Constants.VALUE_NOT_FOUND)
				&& StringUtils.equalsIgnoreCase(phone, Constants.PHONE_NOT_FOUND_3G));
	}

	/**
	 * 
	 * @param list
	 * @param pagesize
	 * @param pagenumber
	 * @return
	 */
	public static <T> Map<String, Object> paging(List<T> list, int pagesize, int pagenumber) {
		Map<String, Object> result = new HashMap<String, Object>();
		if (list != null && list.size() > 0) {
			Integer total = list.size();
			Integer totalOfPages = total % pagesize != 0 ? total / pagesize + 1 : total / pagesize;
			pagenumber = pagenumber > totalOfPages ? totalOfPages : pagenumber;
			int end = pagenumber * pagesize;
			end = end < list.size() ? end : list.size();
			result.put("total", total);
			result.put("totalOfPages", totalOfPages);
			result.put("items", list.subList((pagenumber - 1) * pagesize, end));
			return result;
		} else {
			result.put("total", 0);
			result.put("totalOfPages", 0);
			result.put("items", new ArrayList<>());
			return result;
		}
	}

	public static <T> Map<String, Object> paging(JavaRDD<T> list, int total, int pagesize, int pagenumber) {
		Map<String, Object> result = new HashMap<String, Object>();
		int totalOfPages = total % pagesize != 0 ? total / pagesize + 1 : total / pagesize;
		pagenumber = pagenumber > totalOfPages ? totalOfPages : pagenumber;
		int end = pagenumber * pagesize;
		end = end < total ? end : total;
		result.put("total", total);
		result.put("totalOfPages", totalOfPages);
		result.put("items", list.take(end).subList((pagenumber - 1) * pagesize, end));
		return result;

	}
	public static <T> Map<String, Object> paging(List<T> list, int total, int pagesize, int pagenumber) {
		Map<String, Object> result = new HashMap<String, Object>();
		int totalOfPages = total % pagesize != 0 ? total / pagesize + 1 : total / pagesize;
		pagenumber = pagenumber > totalOfPages ? totalOfPages : pagenumber;
		int end = pagenumber * pagesize;
		end = end < total ? end : total;
		result.put("total", total);
		result.put("totalOfPages", totalOfPages);
		result.put("items", list.subList((pagenumber - 1) * pagesize, end));
		return result;

	}

	public static DateTime convertStringToDate(String dateStr) {
		DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern(Constants.DATE_FORMAT);
		DateTime dateTime = DateTime.parse(dateStr, dateTimeFormat);
		return dateTime;
	}

	/**
	 * 
	 * @param from
	 * @param to
	 * @param tc
	 * @return
	 */
	public static boolean isValidDateRange(Long from, Long to, Long tc) {
		if (from == null || to == null || to < from) {
			return true;
		} else {
			return from <= tc && tc < to;
		}

	}

	/**
	 * 
	 * @param domains
	 * @param domain
	 * @return
	 */
	public static boolean isValidDomain(List<String> domains, String domain) {
		if (domains == null || domains.isEmpty()) {
			return true;
		} else {
			return domains.contains(domain);
		}
	}
}
