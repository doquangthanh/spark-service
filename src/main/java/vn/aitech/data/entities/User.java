/**
 * 
 */
package vn.aitech.data.entities;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;



/**
 * @author thanhdq
 *
 */

public class User implements Serializable {

	
	private static final long serialVersionUID = 1L;

	private UUID id=UUID.randomUUID();
	
	private String workPlaces;
	
	private String birthPlaces;
	
	private String education;
	
	private String fullName;
	
	private String facebook;
	
	private String avatarPath;
	
	private String phone;
	
	private String birthDay;
	
	private String marriedStatus;
	private String type="facebook";
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	private Timestamp createdTime;
	
	
	public UUID getId() {
		return id;
	}
	public void setId(UUID id) {
		this.id = id;
	}
	
	public String getWorkPlaces() {
		return workPlaces;
	}
	public void setWorkPlaces(String workPlaces) {
		this.workPlaces = workPlaces;
	}
	public String getEducation() {
		return education;
	}
	public void setEducation(String education) {
		this.education = education;
	}
	public String getFullName() {
		return fullName;
	}
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	public String getFacebook() {
		return facebook;
	}
	public void setFacebook(String facebook) {
		this.facebook = facebook;
	}
	public String getAvatarPath() {
		return avatarPath;
	}
	public void setAvatarPath(String avatarPath) {
		this.avatarPath = avatarPath;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getBirthDay() {
		return birthDay;
	}
	public void setBirthDay(String birthDay) {
		this.birthDay = birthDay;
	}
	public String getMarriedStatus() {
		return marriedStatus;
	}
	public void setMarriedStatus(String marriedStatus) {
		this.marriedStatus = marriedStatus;
	}
	public String getBirthPlaces() {
		return birthPlaces;
	}
	public void setBirthPlaces(String birthPlaces) {
		this.birthPlaces = birthPlaces;
	}
	
	public Timestamp getCreatedTime() {
		return createdTime;
	}
	public void setCreatedTime(Timestamp createdTime) {
		this.createdTime = createdTime;
	}
	@Override 
	 public String toString() { 
	  return "user [id=" + id + ", id=" + id + ", fullname=" + fullName  + ", phone=" + phone  + ", facebook=" + facebook+"]"; 
	 } 

}
