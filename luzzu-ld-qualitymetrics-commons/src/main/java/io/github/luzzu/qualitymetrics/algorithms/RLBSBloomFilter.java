package io.github.luzzu.qualitymetrics.algorithms;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.BitSet;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * @author Santiago Londono
 * Implementation of the Randomized Load-balanced Biased Sampling Bloom Filter, as proposed by Bera et al. [bera12],
 * which is and adaptation of Bloom Filters to the data de-duplication problem.
 * - NOTICE: Instances of this class are NOT threadsafe
 */
public class RLBSBloomFilter {
	
	private static Logger logger = LoggerFactory.getLogger(RLBSBloomFilter.class);
	
	// Number of hashing functions and bit arrays composing the RLBS Bloom Filter
	private int k = 0;
	
	// Total memory to be used by the filter arrays in bits, the number of bits spanning 
	// each Bloom Filter will be set to m/k
	private int m = 0;
	// Number of elements of each bit array
	private int bitSetSize = 0;
		
	// Threshold False Positive Rate
	private double thresFPR = 0.0;
	
	// Arrays of functions used to implement the Bloom Filters required by the algorithm
	private BitSet[] arrBitSets = null;
	private HashFunction[] arrHashFunctions = null;
	
	/**
	 * Constructor. Initializes the RLBS Bloom Filter.
	 * @param k number of simple Bloom Filters composing this instance
	 */
	public RLBSBloomFilter(int k, int m, double thresFPR) {		
		// Initialize parameters and calculate derived ones
		this.thresFPR = thresFPR;
		this.m = m;
		this.k = k;
		this.bitSetSize = (int)(m/k);
		
		// Prepare the hash functions to map items to positions in the bit array
		this.arrHashFunctions = new HashFunction[k];
		this.arrBitSets = new BitSet[k];
		
		for(int i = 0; i < k; i++) {
			// Murmur3 hashing functions, having different seeds are independent from each other 
			this.arrHashFunctions[i] = Hashing.murmur3_128(i);
			// Each bit array implements a memory of m/k bit positions
			this.arrBitSets[i] = new BitSet(this.bitSetSize);
		}
		
		// Compute suggessted k, according to Bera et al. (pg. 24)
		double computedK = (Math.log(this.thresFPR) / Math.log(1 - (1/Math.E)));
		
		logger.info("RLBSBF initialized. Memory size (m): {}, Hash-functions: {}, Suggested Hash-functions: {}", this.m, this.k, computedK);
	}
	
	/***
	 * Estimates whether the item provided as parameter is already part of the RLBS Bloom Filter, that is,
	 * if the item is a duplicate of a previously seen item. If the item is found to be duplicate, returns true,
	 * otherwise false. False Positives and False Negatives can occur with a low probability 
	 * @param strItem item to be checked, in order to estimate if it was previously detected
	 * @return True if the item has been previously detected, false otherwise (with certain a false positive/negative rate)
	 */
	public boolean checkDuplicate(String strItem) {
		// Hash the item through the k hashing functions
		int[] arrItemHashings = new int[this.k];
		boolean isDuplicate = true;
		
		// Hash the item through the k hash functions and determine whether is duplicate
		for(int i = 0; i < this.k; i++) {
			// Generate the hashing index of the item for bit array #i
			arrItemHashings[i] = Hashing.consistentHash(this.arrHashFunctions[i].hashUnencodedChars(strItem), this.bitSetSize);
			// Check if the corresponding bit is set to 0, which is enough to regard the item as distinct
			isDuplicate = isDuplicate && this.arrBitSets[i].get(arrItemHashings[i]);
		}
		logger.debug("Item: {} encoded, is duplicate: {}. Positions: {}", strItem, isDuplicate, arrItemHashings);
		
		if(!isDuplicate) {
			Random randGen = new Random();
			BitSet bitArray = null;
			BigDecimal curProbReset = null;
			
			// The item was estimated to be distinct, randomly balance the load on each bit array
			for(int j = 0; j < this.k; j++) {
				// Determine how many bits are set in the array
				bitArray = this.arrBitSets[j];
				
				// Compute the probability of reseting a bit of the current bit array, as L(i)/s
				curProbReset = (new BigDecimal(bitArray.cardinality())).divide(BigDecimal.valueOf(this.bitSetSize), RoundingMode.HALF_EVEN);
				
				// Reset a random bit of the bit array, with probability curProbReset
				if(randGen.nextDouble() <= curProbReset.doubleValue()) {
					// Select a random bit position to be reset...
					int randPosToReset = randGen.nextInt(this.bitSetSize);
					bitArray.clear(randPosToReset);
					logger.debug("Load balancing BF{}. Resetting item at position {} will be reset (reset prob. {})", j, randPosToReset, curProbReset);
				} else {
					logger.debug("Load balancing BF{}. Randomly chose not to reset any position (reset prob. {})", j, curProbReset);
				}
				
				// Set the bit the item was mapped to by the hashing function
				bitArray.set(arrItemHashings[j]);
			}
		}
		
		return isDuplicate;
	}
	
	
	public int[] getSetBitLocations(String strItem) {
		// Hash the item through the k hashing functions
		int[] arrItemHashings = new int[this.k];
		
		for(int i = 0; i < this.k; i++) {
			arrItemHashings[i] = Hashing.consistentHash(this.arrHashFunctions[i].hashUnencodedChars(strItem), this.bitSetSize);
		}
		
		return arrItemHashings;
	}
}

