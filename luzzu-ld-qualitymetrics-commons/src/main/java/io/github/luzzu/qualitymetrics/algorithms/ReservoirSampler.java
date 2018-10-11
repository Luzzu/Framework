package io.github.luzzu.qualitymetrics.algorithms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * A collection of items randomly sampled from a much larger set, in accordance with the 
 * Reservoir Sampling algorithm (http://en.wikipedia.org/wiki/Reservoir_sampling). 
 * This data structure considers replacement.
 * 
 * Instances of this class are Thread Safe
 * @author Santiago Londono
 */
public class ReservoirSampler<T> implements Serializable {
	private static final long serialVersionUID = 171565809692064783L;

	// Size of the reservoir, maximum number of items that can be stored
	private int k;
	
	// Number of items that have been added, or attempted to be added to the reservoir
	private int i;
	
	// Kepts an index of the items in the reservoir, if required to do so, for O(1) retrieval of items by value
	private HashMap<T, Integer> mapIndex;

	// Random number generator
	private Random random;

	// Items contained in the reservoir
	private List<T> lstItems;

	/**
	 * Constructor
	 * @param k Size of the reservoir, that is, maximum number of items it can store
	 * @param indexItems Says whether items will be indexed by value, if so, a HashMap will be created to hold the indices
	 */
	public ReservoirSampler(int k, boolean indexItems) {
		this.k = k;
		this.lstItems = new ArrayList<T>(k);
		this.random = new Random();
		
		if(indexItems) {
			this.mapIndex = new HashMap<T, Integer>();
		}
	}
	
	/**
	 * Adds an item to the reservoir. If the current number of items is above k, the item might or might not be
	 * inserted with a probability that decreases with the total number of items in the reservoir 
	 * @param item Item to be added to the reservoir, assumed to come from a much larger set, called the source set
	 * @return true if the item was indeed added to the reservoir, false if it was randomly discarded
	 */
	public synchronized boolean add(T item) {
		// Keep track of how many items have been attempted to be added, as it determines the replacement probability
		this.i++;
		
		boolean wasAdded = false;
		int addedAtPos = this.lstItems.size();
		
		// The reservoir will be filled up to its max. capacity, then elements will be replaced randomly
		if(addedAtPos < this.k) {
			this.lstItems.add(item);
			wasAdded = true;
		} else {
			// Randomly generate the position where the item shall be inserted...
			addedAtPos = random.nextInt(this.i);
			
			// and replace the element currently at that position, if its within the reservoir
			if(addedAtPos < this.k) {
				// Remove from the index the item that will be replaced
				if(this.mapIndex != null) {
					this.mapIndex.remove(this.lstItems.get(addedAtPos));
				}
				
				this.lstItems.set(addedAtPos, item);
				wasAdded = true;
			}
		}
		
		// If the item was added, update the index
		if(this.mapIndex != null && wasAdded) {
			this.mapIndex.put(item, addedAtPos);
		}
		
		return wasAdded;
	}
	
	/**
	 * Searchs for the specified item in the reservoir, if found, returns it, otherwise returns null
	 * @param item Item to be searched for
	 * @return The item as stored in the reservoir, if it is found in it, null otherwise
	 */
	public synchronized T findItem(T item) {		
		// If the index is available, use it to find the item in O(1) time!
		if(this.mapIndex != null) {
			if(this.mapIndex.containsKey(item) && (this.lstItems.size() > this.mapIndex.get(item))) {
				return this.lstItems.get(this.mapIndex.get(item));
			}
		} else {
			for(T curItem : this.lstItems) {
				if(curItem.equals(item)) {
					return curItem;
				}
			}
		}
		return null;
	}
	
	/**
	 * Clears the reservoir and re-initializes its whole state
	 */
	public void reset() {
		this.i = 0;		
		this.random = new Random();
		
		synchronized(this) {
			this.lstItems.clear();
		}
	}
	
	/**
	 * Returns the list of items currently existing in the 
	 */
	public List<T> getItems() {
		return this.lstItems;
	}
	
	/**
	 * Returns the number of items currently contained in the reservoir
	 */
	public int size() {
		return this.lstItems.size();
	}
	
	/**
	 * Tells whether the reservoir is already full, that is, if the next added item will be randomly 
	 * accepted or discarded and when accepted, it will replace an existing item
	 * @return True if the reservoir is full, false otherwise
	 */
	public boolean isFull() {
		return (this.size() >= this.k);
	}

	/**
	 * Returns the size of the reservoir, that is, the maximum number of items it will hold
	 * @return Size of the reservoir
	 */
	public int getK() {
		return this.k;
	}
	
	/**
	 * Returns the total number of items that have been attempted to be added to the reservoir, 
	 * including those that were discarded as part of the random process and thus were not really added
	 */
	public int getTotalAddedItems() {
		return this.i;
	}

}
