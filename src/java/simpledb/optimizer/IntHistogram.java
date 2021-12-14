package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	int buckets = -1;
	int min = -1;
	int max = -1;
	int inteval = 1;
	int CountNum[] = null;
	int totalTuple = 0;
	int lastBucketWidth = 0;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = buckets;
        this.totalTuple = 0;
        this.min = min;
        this.max = max;

        this.inteval = ((max - min + 1) / buckets);
        if (this.inteval == 0) {
            this.inteval = 1;
        } else if ((max - min + 1) % buckets != 0) {
            // increase bucket number to handle overflow
            this.buckets += 1;
        }
        this.lastBucketWidth = max - (min + (buckets - 1) * inteval) + 1;
        this.CountNum = new int[this.buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	CountNum[(v - this.min) / this.inteval]++;
        totalTuple += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    	if (op == Predicate.Op.EQUALS && (v < this.min || v > this.max)) return 0.0;
        if (op == Predicate.Op.NOT_EQUALS && (v < this.min || v > this.max)) return 1.0;
        if ((op == Predicate.Op.GREATER_THAN && v >= this.max) || (op == Predicate.Op.LESS_THAN && v <= this.min)) return 0.0;
        if ((op == Predicate.Op.GREATER_THAN_OR_EQ && v > this.max) || (op == Predicate.Op.LESS_THAN_OR_EQ && v < this.min)) return 0.0;
        if ((op == Predicate.Op.GREATER_THAN && v < this.min) || (op == Predicate.Op.LESS_THAN && v > this.max)) return 1.0;
        if ((op == Predicate.Op.GREATER_THAN_OR_EQ && v <= this.min) || (op == Predicate.Op.LESS_THAN_OR_EQ && v >= this.max)) return 1.0;

        // this.min <= v <= this.max
        int pos = (v - this.min) / this.inteval;
        int b_right = this.min + pos * this.inteval; // inclusive;
        double numQualifiedTup = 0.0;
        int curBucketWidth = this.inteval;
        if (pos == buckets - 1) {
            curBucketWidth = this.lastBucketWidth;
        }

        if (op == Predicate.Op.EQUALS) {
            numQualifiedTup = (1.0 / curBucketWidth) * CountNum[pos];
        } else if (op == Predicate.Op.NOT_EQUALS) {
            numQualifiedTup = totalTuple - (1.0 / curBucketWidth) * CountNum[pos];
        } else if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            for (int i = pos+1; i < CountNum.length; ++i) {
                numQualifiedTup += CountNum[i];
            }
            if (op == Predicate.Op.GREATER_THAN) {
                numQualifiedTup += ((double) (curBucketWidth - (v - b_right)) / curBucketWidth) * CountNum[pos];
            } else {
                numQualifiedTup += ((double) (curBucketWidth - (v - b_right) + 1) / curBucketWidth) * CountNum[pos];
            }
        } else {
            // less and lessequal
            for (int i = 0; i < pos; ++i) {
                numQualifiedTup += CountNum[i];
            }
            if (op == Predicate.Op.LESS_THAN ) {
                numQualifiedTup += ((double) (v - b_right) / curBucketWidth) * CountNum[pos];
            } else {
                numQualifiedTup += ((double) (v - b_right + 1) / curBucketWidth) * CountNum[pos];
            }
        }
        return numQualifiedTup / totalTuple;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
