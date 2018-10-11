package io.github.luzzu.qml.datatypes.conditions.ImplementedConditions;

import org.apache.jena.graph.Node;

public class TypeOf implements ICustomCondition {

	@Override
	public boolean compute(Object... args) throws IllegalArgumentException {
		if(args.length != 3) {
			throw new IllegalArgumentException("Illegal Number of Arguments, Required 2, Found "+args.length);			
		}
		
		Node lhs = (Node)args[0];
		String op = (String)args[1];
		String rhs = (String)args[2];
		
		switch (op) {
		case "==":
		{
			if(lhs.isURI() && lhs.getURI().equals(rhs)) {
				return true;
			}
		}
			break;
		case "!=":
		{
			if(!lhs.isURI() || (lhs.isURI() && !lhs.getURI().equals(rhs))) {
				return true;
			}
		}
			break;
		default:
			throw new IllegalArgumentException("Illegal Operator expected is == or !=, found "+ op);
		}
	
		
		return false;
	}

}
