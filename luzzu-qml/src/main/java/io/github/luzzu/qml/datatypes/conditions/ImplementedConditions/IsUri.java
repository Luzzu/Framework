package io.github.luzzu.qml.datatypes.conditions.ImplementedConditions;

import org.apache.jena.graph.Node;

public class IsUri implements ICustomCondition{

	@Override
	public boolean compute(Object... args) throws IllegalArgumentException {
		if(args.length != 1) {
			throw new IllegalArgumentException("Illegal Number of Arguments, Required 1, Found "+args.length);
		}
		
		Node argNode = (Node) args[0];
		return argNode.isURI();
	}

}
