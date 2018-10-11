package io.github.luzzu.qml.datatypes;

import java.util.ArrayList;
import io.github.luzzu.qml.datatypes.actions.Action;
import io.github.luzzu.qml.datatypes.conditions.Condition;

public class Rule {
	
	private String declerativeRule = "";
	private String resultVar;
	private ArrayList<Condition> conditions = new ArrayList<Condition>();
	private Action action;
	
	private StringBuilder conditionJavaStr = new StringBuilder();
	
	
	public void addCondition(Condition cond) {
		conditions.add(cond);
		conditionJavaStr.append(cond.conditionToJava());
	}
	public void addOperator(String operator) {
		switch (operator) {
		case "&":
			conditionJavaStr.append(" && ");
			break;
		case "|":
			conditionJavaStr.append(" || ");
			break;
		case "(":
			conditionJavaStr.append(" ( ");
			break;
		case ")":
			conditionJavaStr.append(" ) ");
			break;
		case "!":
			conditionJavaStr.append(" !");
			break;
		default:
			break;
		}
		//conditionJavaStr.append(str)
	}
	
	public String getDeclerativeRule() {
		return declerativeRule;
	}
	public void setDeclerativeRule(String declerativeRule) {
		this.declerativeRule = declerativeRule;
	}
	
	public ArrayList<Condition> getCondition() {
		return conditions;
	}
	public void setCondition(ArrayList<Condition> conditions) {
		this.conditions = conditions;
	}
	
	public String getResultVar() {
		return resultVar;
	}
	public void setResultVar(String resultVar) {
		this.resultVar = resultVar;
	}
	
	public Action getAction() {
		return action;
	}
	public void setAction(Action action) {
		this.action = action;
	}
	
	
	public String createVariables(){
		StringBuilder sb = new StringBuilder();
		sb.append(action.createVariable(resultVar) + System.getProperty("line.separator"));
		/*Iterator<Condition> condIterator = conditions.iterator();
		while (condIterator.hasNext()) {
			sb.append(condIterator.next().createVariable());	
		}*/
		return sb.toString();
	}
	public String ruleToJava() {
		String ruleString = "if (%%VALUE%%){ %%ACTION%%; }";
		ruleString = ruleString.replace("%%VALUE%%", conditionJavaStr.toString());
		return ruleString.replace("%%ACTION%%", action.actionToJava(resultVar));
	}
}
