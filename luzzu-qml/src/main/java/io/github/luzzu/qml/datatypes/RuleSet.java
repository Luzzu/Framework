package io.github.luzzu.qml.datatypes;


import java.util.Iterator;
import java.util.List;

public class RuleSet {
	private List<Rule> rules = null;//new ArrayList<Rule>();
	public void setRuleSet(List<Rule> _rules) {
		rules = _rules;
	}
	public void addRule(Rule r) {
		rules.add(r);
	}
	public List<Rule> getRuleSet() {
		return rules;
	}
	
	public String createVariables() {
		StringBuilder sb = new StringBuilder();
		Iterator<Rule> ruleIterator = rules.iterator();
		while (ruleIterator.hasNext()) {
			sb.append(ruleIterator.next().createVariables());
		}
		return sb.toString();
	}
	public String ruleSetToJava() {
		StringBuilder sb = new StringBuilder();
		Iterator<Rule> ruleIterator = rules.iterator();
		while (ruleIterator.hasNext()) {
			sb.append(ruleIterator.next().ruleToJava());
		}
		return sb.toString();
	}
}
