package io.github.luzzu.qml.datatypes.actions.ImplementedActions;

import java.util.HashSet;
import java.util.Set;

public class CountUnique implements IAction {

	Set<Object> set = new HashSet<Object>();


	@Override
	public void perform(Object... args) throws IllegalArgumentException {
		if(args.length != 1) {
			new IllegalArgumentException("Illegal Number of Arguments, Required 1, Found "+args.length);
		}
		if(!set.contains(args[0])) {
			set.add(args[0].toString());
		}
	}

	@Override
	public double finalValue() {
		return set.size();
	}

}
