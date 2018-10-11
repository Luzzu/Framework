package io.github.luzzu.qml.datatypes.functions;

public class FunctionParam {
	private FunctionParamType type;

	
	private String varName = "";
	private Function function = null;
	
	public void setParamType(FunctionParamType _type) {
		type = _type;
	}
	public FunctionParamType getParamType() {
		return type;
	}
	
	public void setVarName(String _varName) {
		varName = "action_var_"+_varName;
	}
	public String getVarName() {
		return varName;
	}
	
	public void setFunction(Function fun) {
		this.function = fun;
	}
	public Function getFunction() {
		return function;
	}
	
	public String toJava() {
		switch (type) {
		case FUNCTION:
			return function.toJava();
		case TOTALTRIPLES:
			return "totalTriplesAssessed";
		case VAR:
		default:
			return varName+".finalValue()";
		}
	}
}
