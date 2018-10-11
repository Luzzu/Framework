package io.github.luzzu.communications;

import java.util.concurrent.Callable;

import io.github.luzzu.io.IOProcessor;

public abstract class ExtendedCallable<V> implements Callable<V> {

	protected IOProcessor strmProc = null;
	
	public IOProcessor getIOProcessor(){
		if (this.strmProc == null) return null;
		else return this.strmProc;
	}

}
