/**
 * 
 */
package vn.aitech.data.sparkservice.entities;

import com.google.gson.annotations.SerializedName;

/**
 * @author thanhdq
 *
 */
public class JsonKey {
	@SerializedName("id") 
	private String name;
	@SerializedName("data") 
	private Object value;
	public JsonKey(String name,Object value){
		this.name=name;
		this.value=value;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	

}
