/**
 * 
 */
package vn.aitech.data.entities;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

/**
 * @author thanhdq
 *
 */

public class Pageview implements Serializable{
	
	private UUID idu=UUID.randomUUID();
	
	private String id;

	private String ids;
	
	private String ip;
	
	private String dm;
	
	private String title;
	
	private String url;
	
	private String location;
	
	private String city;
	
	private String telephone;
	
	private String telco;
	
	private String referrer;
	
	private String lang;
	
	private String browser;
	
	private String browser_version;
	
	private String browser_type;

	private String os;
	
	private String os_version;
	
	private String user_agent;
	
	private String res;
	
	private Long tp;
	
	private Long tc;
	
	private Long tgl;	
	
	private Long tb;
	
	private Long nv;	
	
	private Date createdtime;
	
	
	private Boolean detectedmobile=false;
	
	private Boolean detectedinternet=false;
	
	private Integer hour;
	public UUID getIdu() {
		return idu;
	}
	public void setIdu(UUID idu) {
		this.idu = idu;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	public String getIds() {
		return ids;
	}
	public void setIds(String ids) {
		this.ids = ids;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getDm() {
		return dm;
	}
	public void setDm(String dm) {
		this.dm = dm;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getTelephone() {
		return telephone;
	}
	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}
	public String getTelco() {
		return telco;
	}
	public void setTelco(String telco) {
		this.telco = telco;
	}	
	public String getReferrer() {
		return referrer;
	}
	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}
	public String getBrowser() {
		return browser;
	}
	public void setBrowser(String browser) {
		this.browser = browser;
	}
	public String getBrowser_version() {
		return browser_version;
	}
	public void setBrowser_version(String browser_version) {
		this.browser_version = browser_version;
	}
	public String getBrowser_type() {
		return browser_type;
	}
	public void setBrowser_type(String browser_type) {
		this.browser_type = browser_type;
	}
	public String getOs() {
		return os;
	}
	public void setOs(String os) {
		this.os = os;
	}
	public String getOs_version() {
		return os_version;
	}
	public void setOs_version(String os_version) {
		this.os_version = os_version;
	}
	public String getUser_agent() {
		return user_agent;
	}
	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}
	public String getRes() {
		return res;
	}
	public void setRes(String res) {
		this.res = res;
	}
	public Long getTp() {
		return tp;
	}
	public void setTp(Long tp) {
		this.tp = tp;
	}
	public Long getTc() {
		return tc;
	}
	public void setTc(Long tc) {
		this.tc = tc;
	}
	public Long getTgl() {
		return tgl;
	}
	public void setTgl(Long tgl) {
		this.tgl = tgl;
	}
	public Long getTb() {
		return tb;
	}
	public void setTb(Long tb) {
		this.tb = tb;
	}
	public Long getNv() {
		return nv;
	}
	public void setNv(Long nv) {
		this.nv = nv;
	}
	public Date getCreatedtime() {
		return createdtime;
	}
	public void setCreatedtime(Date createdtime) {
		this.createdtime = createdtime;
	}
	
	public Boolean getDetectedmobile() {
		return detectedmobile;
	}
	public void setDetectedmobile(Boolean detectedmobile) {
		this.detectedmobile = detectedmobile;
	}
	public Boolean getDetectedinternet() {
		return detectedinternet;
	}
	public void setDetectedinternet(Boolean detectedinternet) {
		this.detectedinternet = detectedinternet;
	}
	public Integer getHour() {
		return hour;
	}
	public void setHour(Integer hour) {
		this.hour = hour;
	}
	@Override
	public boolean equals(Object obj) {
		if(obj==null){
			return false;
		}
		return StringUtils.equalsIgnoreCase(this.getTelephone(),((Pageview)obj).getTelephone());
	}
	

}
