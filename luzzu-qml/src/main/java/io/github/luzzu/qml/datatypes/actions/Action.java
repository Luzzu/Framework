package io.github.luzzu.qml.datatypes.actions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Action {
	private String fullClassName;
	private String className;
	private ActionType actionType;
	private List<String> params = new ArrayList<String>();

	public void setActionType(ActionType actType) {
		actionType = actType;
		switch (actionType) {
		case COUNT:
			className = "Count";
			fullClassName = "io.github.luzzu.qml.datatypes.actions.ImplementedActions.Count";
			break;
		case COUNTUNIQUE:
			className = "CountUnique";
			fullClassName = "io.github.luzzu.qml.datatypes.actions.ImplementedActions.CountUnique";
			break;
		default:
			break;
		}
	}
	public ActionType getActionType() {
		return actionType;
	}
	
	public void setParams(List<String> params) {
		this.params = params;
	}
	public void addParam(String param) {
		params.add(param);
	}
	public Iterator<String> getParamIterator() {
		return params.iterator();
	}
	
	public void setClassName(String clsName) {
		className = clsName;
	}
	public String getClassName() {
		return className;
	}
	
	
	public String createVariable(String varName) throws IllegalStateException {
		if(className == null || className.trim().equals("")
				|| fullClassName == null || fullClassName.trim().equals("")) {
			throw new IllegalStateException("Class Name for Action is not set");
		}
		return "private "+fullClassName+" action_var_"+varName+" = new "+fullClassName+"();";
	}
	public String actionToJava(String varName) {
		Iterator<String> paramIterator = getParamIterator();
		StringBuilder sb = new StringBuilder();
		while (paramIterator.hasNext()) {
			if(sb.length() > 0) {
				sb.append(",");
			}
			String param = paramIterator.next();
			switch (param) {
			case "?s":
				sb.append("quad.getSubject()");
				break;
			case "?p":
				sb.append("quad.getPredicate()");
				break;
			default:
				sb.append("quad.getObject()");
				break;
			}
		}
		return "action_var_"+varName+".perform("+sb.toString()+")";
	}
}
