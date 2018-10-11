package io.github.luzzu.qml.datatypes.conditions;

import java.util.ArrayList;
import java.util.List;


public class DefinedCondition extends Condition {
	
	private String lhs;
	private String booleanOperator;
	private String rhs;
	private String logicalOperator;
	private Boolean isIRI = false;
	private List<String> params = new ArrayList<String>();
	
	public List<String> getParams() {
		return params;
	}

	public void setParams(List<String> params) {
		this.params = params;
	}

	public String getLhs() {
		return lhs;
	}

	public void setLhs(String lhs) {
		this.lhs = lhs;
	}

	public String getBooleanOperator() {
		return booleanOperator;
	}

	public void setBooleanOperator(String booleanOperator) {
		this.booleanOperator = booleanOperator;
	}

	public String getRhs() {
		return rhs;
	}

	public void setRhs(String rhs) {
		this.rhs = rhs;
	}

	public String getLogicalOperator() {
		return logicalOperator;
	}

	public void setLogicalOperator(String logicalOperator) {
		this.logicalOperator = logicalOperator;
	}

	public Boolean getIsIRI() {
		return isIRI;
	}

	public void setIsIRI(Boolean isIRI) {
		this.isIRI = isIRI;
	}
}

