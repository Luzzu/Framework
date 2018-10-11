package io.github.luzzu.qml.datatypes.functions.implemented;


public class Normalise implements IFunction {

	@Override
	public double execute(double... args) throws IllegalArgumentException {
		if(args.length != 1)
		{
			throw new IllegalArgumentException("Normalise expects 1 arguments");
		}
		
		return 1.0 - args[0];
	}

}
