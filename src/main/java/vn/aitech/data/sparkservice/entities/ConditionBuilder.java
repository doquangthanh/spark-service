/**
 * 
 */
package vn.aitech.data.sparkservice.entities;

import java.io.Serializable;

/**
 * @author thanhdq
 *
 */
public class ConditionBuilder implements Serializable {
	private String name;
	private String operator;
	private Object target;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public Object getTarget() {
		return target;
	}
	public void setTarget(Object target) {
		this.target = target;
	}
	

}
