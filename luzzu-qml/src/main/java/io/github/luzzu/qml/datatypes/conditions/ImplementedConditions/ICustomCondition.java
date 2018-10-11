package io.github.luzzu.qml.datatypes.conditions.ImplementedConditions;

public interface ICustomCondition {
	public boolean compute(Object ...args) throws IllegalArgumentException;
}
