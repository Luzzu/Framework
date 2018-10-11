package io.github.luzzu.qml.datatypes.conditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.github.luzzu.qml.datatypes.Metric;


public class Condition {
	private ConditionType conditionType;
	private List<String> params = new ArrayList<String>();
	private String fullClassName;
	private String className;
	
	/*public Condition(ConditionType condType) {
		setConditionType(condType);
		createVariable();
	}*/
	
	public ConditionType getConditionType() {
		return conditionType;
	}
	
	public void setConditionType(ConditionType conditionType) {
		this.conditionType = conditionType;
		switch (this.conditionType) {
		case ISBLANK:
			fullClassName = "io.github.luzzu.qml.datatypes."+
					"conditions.ImplementedConditions.IsBlank";
			className = "IsBlank";
			break;
		case ISURI:
			fullClassName = "io.github.luzzu.qml.datatypes."+
					"conditions.ImplementedConditions.IsUri";
			className = "IsUri";
			break;
		case ISLITERAL:
			fullClassName = "io.github.luzzu.qml.datatypes."+
					"conditions.ImplementedConditions.IsLiteral";
			className = "IsLiteral";
			break;
		case TYPEOF:
			fullClassName = "io.github.luzzu.qml.datatypes."+
					"conditions.ImplementedConditions.TypeOf";
			className = "TypeOf";
			break;
		default:
			break;
		}
		createVariable();
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
//		fullClassName = className;
		className = clsName;
	}
	
	public String getClassName() {
		return className;
	}
	
	public void setFullClassName(String fullClassName){
		this.fullClassName = fullClassName;
	}
	
	public String getFullClassName(){
		return this.fullClassName;
	}
	
	private void createVariable() throws IllegalStateException {
		if (this.conditionType == ConditionType.NORMAL){
			
		} else {
			if((className == null || className.trim().equals("") ||
					fullClassName == null || fullClassName.trim().equals(""))) {
				throw new IllegalStateException("Class Name for Condition is not set");
			}
			Metric.variableTable.put(fullClassName, "cond_var_"+className);
		}
	}
	public String conditionToJava() {
		if (this.conditionType == ConditionType.NORMAL){
			StringBuilder cnd = new StringBuilder();
			boolean isO = false;
			
			cnd.append("(");
			String lhs = ((DefinedCondition)this).getLhs();
			if (lhs.equals("?s")) cnd.append("quad.getSubject().getURI().equals(\"");
			if (lhs.equals("?p")) cnd.append("quad.getPredicate().getURI().equals(\"");
			if (lhs.equals("?o")) {
				isO = true;
				if (((DefinedCondition)this).getIsIRI())
					cnd.append("quad.getObject().equals(\"");
				else 
					cnd.append("quad.getObject().getValue()");
			}
			
			if (isO && !((DefinedCondition)this).getIsIRI()){
				cnd.append(" ");
				cnd.append(((DefinedCondition)this).getBooleanOperator());
				cnd.append(" ");
				cnd.append(((DefinedCondition)this).getRhs());
				cnd.append(")");
			} else {
				cnd.append(((DefinedCondition)this).getRhs().replaceFirst("<", "").replaceFirst(">", ""));
				cnd.append("\")");
			}
			cnd.append(")");

			return cnd.toString();
		} else {
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
			return Metric.variableTable.get(fullClassName)+".compute("+sb.toString()+")";
		}
	}
}
