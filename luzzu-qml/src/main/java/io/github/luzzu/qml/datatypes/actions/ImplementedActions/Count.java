package io.github.luzzu.qml.datatypes.actions.ImplementedActions;

public class Count implements IAction {

	int count = 0;
	@Override
	public void perform(Object... args) throws IllegalArgumentException {
		if(args.length != 0) {
			new IllegalArgumentException("Illegal Number of Arguments, Required 0, Found "+args.length);			
		}
		count++;
	}

	@Override
	public double finalValue() {
		return count;
	}

}
