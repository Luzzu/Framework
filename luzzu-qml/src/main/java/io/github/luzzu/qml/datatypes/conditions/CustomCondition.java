package io.github.luzzu.qml.datatypes.conditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomCondition extends Condition{
	private List<String> params = new ArrayList<String>();
	
	public void addParam(String param) {
		params.add(param);
	}
	
	public Iterator<String> getParamsIterator() {
		return params.iterator();
	}
	
	public void setClassName(String clsName) {
		super.setClassName(clsName);
	}
	public String getClassName() {
		return super.getClassName();
	}
}
