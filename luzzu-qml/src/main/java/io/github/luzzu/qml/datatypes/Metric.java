package io.github.luzzu.qml.datatypes;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map.Entry;

import io.github.luzzu.qml.datatypes.functions.Function;

import java.util.Set;
import java.util.Iterator;


public class Metric {

	
	
	public static Hashtable<String, String> variableTable =
			new Hashtable<String, String>();
	
	private Set<String> imports = new HashSet<String>();
	private RuleSet rules;
	private Function finalVal;
	public Metric(RuleSet rules, Function finalVal) {
		this.rules = rules;
		this.finalVal = finalVal;
	}
	public String getImports(){
		StringBuilder sb = new StringBuilder();
		for(String s : imports){
			sb.append(s);
			sb.append(System.getProperty("line.separator"));
		}
		return sb.toString();
	}
	
	public String getVariables() {		
		Iterator<Entry<String, String>> itr = variableTable.entrySet().iterator();
		StringBuilder varString = new StringBuilder();
		while(itr.hasNext()) {
			Entry<String,String> entry = itr.next();
			//"private "+fullClassName+" cond_var_"+className+" = new "+ fullClassName+"();"+System.getProperty("line.separator");
			varString.append("private " + entry.getKey() + " " + entry.getValue() 
					+" = new "+ entry.getKey()+"();" + System.getProperty("line.separator"));
		}
		varString.append(rules.createVariables() + System.getProperty("line.separator"));
		return varString.toString();
	}
	
	public String getComputeFunction(){
		
		return rules.ruleSetToJava() + System.getProperty("line.separator");
	}
	public String getFinalMetricValueFunction() {
		return "return " + finalVal.toJava() +";"+ System.getProperty("line.separator");
	}
}
