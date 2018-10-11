package io.github.luzzu.qml.datatypes.functions;

import java.util.Iterator;
import java.util.List;

import io.github.luzzu.qml.datatypes.Metric;

public class Function {
	private List<FunctionParam> params = null;
	private FunctionType type;
	
	private String fullClassName = "";
	private String className = "";
	
	public void setFunctionType(FunctionType _type) {
		type = _type;
		switch (type) {
		case ADD:
			className = "Add";
			fullClassName = "io.github.luzzu.qml.datatypes.functions.implemented.Add";
			break;
		case RATIO:
			className = "Ratio";
			fullClassName = "io.github.luzzu.qml.datatypes.functions.implemented.Ratio";
			break;		
		case NORMALISE:
			className = "Normalise";
			fullClassName = "io.github.luzzu.qml.datatypes.functions.implemented.Normalise";
			break;		
		default:
			break;
		}
		createVariables();
	}
	public void setParams(List<FunctionParam> _params) {
		params = _params; 
	}
	
	private void createVariables() throws IllegalStateException {
		if((className == null || className.trim().equals("") ||
				fullClassName == null || fullClassName.trim().equals(""))) {
			throw new IllegalStateException("Class Name for Condition is not set");
		}
		Metric.variableTable.put(fullClassName, "finally_var_"+className);
		
		//return "private "+fullClassName+" finally_var_"+className+" = new "+ fullClassName+"();"+System.getProperty("line.separator");
	}
	
	public String toJava() {
		
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(Metric.variableTable.get(fullClassName)+".execute(");
		Iterator<FunctionParam> paramIterator =  params.iterator();
		
		if(paramIterator.hasNext()) {
			FunctionParam param = paramIterator.next();
			strBuilder.append(param.toJava());
		}
			
		
		while(paramIterator.hasNext())
		{
			strBuilder.append(",");
			FunctionParam param = paramIterator.next();
			strBuilder.append(param.toJava());
		}
		strBuilder.append(")");
		
		return  strBuilder.toString();
	}
}
