package io.github.luzzu.qml.datatypes.functions.implemented;


public class Add implements IFunction {

	@Override
	public double execute(double... args) throws IllegalArgumentException {
		if(args.length < 2)
		{
			throw new IllegalArgumentException("Add needs atleast two arguments");
		}
		
		double result = 0;
		for(int i=0; i<args.length;i++)
		{
			result += args[i];
		}
		return result;
	}

}
