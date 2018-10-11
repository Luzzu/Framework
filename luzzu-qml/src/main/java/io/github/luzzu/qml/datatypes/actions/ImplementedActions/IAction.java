package io.github.luzzu.qml.datatypes.actions.ImplementedActions;

public interface IAction {
	public void perform(Object ...args) throws IllegalArgumentException;
	public double finalValue();
}
