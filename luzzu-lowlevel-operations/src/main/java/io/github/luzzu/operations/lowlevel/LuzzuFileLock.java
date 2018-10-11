package io.github.luzzu.operations.lowlevel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LuzzuFileLock {

	private static LuzzuFileLock instance = null;
	
	private static Map<String, ReentrantLock> locks = new ConcurrentHashMap<String, ReentrantLock>();
	
	private LuzzuFileLock(){};
	
	public static LuzzuFileLock getInstance(){
		if (instance == null) instance = new LuzzuFileLock();
		return instance;
	}
	
	public synchronized ReentrantLock getOrSetLockForFile(String filePath) {
		if (locks.containsKey(filePath)) {
			return locks.get(filePath);
		} else {
			ReentrantLock lock = new ReentrantLock(true);
			locks.putIfAbsent(filePath, lock);
			return lock;
		}
	}

}
