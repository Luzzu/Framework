package io.github.luzzu.qml.datatypes.functions.implemented;


public class Ratio implements IFunction {

	@Override
	public double execute(double... args) throws IllegalArgumentException {
		if(args.length != 2)
		{
			throw new IllegalArgumentException("Ratio expects 2 arguments");
		}
		
		return args[0]/args[1];
	}

}
