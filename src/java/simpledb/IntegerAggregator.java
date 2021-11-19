package simpledb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private Integer aggregatedValue;
    private HashMap<String, Integer> aggregatedValues;
    private HashMap<String, Integer> mergedCount;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {

        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        aggregatedValues = new HashMap<>();
        mergedCount = new HashMap<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // for the passed aggregation column and aggregator, add tuple to an aggregated field
        // field is of type int
        // TODO: COUNT IS WRONG!! if we have tuples from multiple groups we will have an issue

        IntField intField = (IntField) tup.getField(afield);
        int currentInt = intField.getValue();
        String gbfieldString;
        if (gbfield == NO_GROUPING) {
            gbfieldString = "";
        } else {
            gbfieldString= tup.getField(gbfield).toString();
        }

        mergedCount.putIfAbsent(gbfieldString, 0);
        mergedCount.put(gbfieldString, mergedCount.get(gbfieldString) + 1);

        switch (what) {
            case MIN:
                if (aggregatedValues.get(gbfieldString) == null) {
                    aggregatedValues.put(gbfieldString, currentInt);
                } else {
                    aggregatedValues.put(gbfieldString, Math.min(currentInt, aggregatedValues.get(gbfieldString)));
                }
                break;
            case AVG:
                if (aggregatedValues.get(gbfieldString) == null) {
                    aggregatedValues.put(gbfieldString, currentInt);
                } else {
                    // pre incremented counter - we take into account the current merging value in that counter
                    // weight aggregatedValue by number of ints it represents
                    aggregatedValues.put(gbfieldString, ((mergedCount.get(gbfieldString) - 1) *
                            aggregatedValues.get(gbfieldString) + currentInt) / mergedCount.get(gbfieldString));
                }
                break;
            case SUM:
                if (aggregatedValues.get(gbfieldString) == null) {
                    aggregatedValues.put(gbfieldString, currentInt);
                } else {
                    aggregatedValues.put(gbfieldString, aggregatedValues.get(gbfieldString) + currentInt);
                }
                break;
            case MAX:
                if (aggregatedValues.get(gbfieldString) == null) {
                    aggregatedValues.put(gbfieldString, currentInt);
                } else {
                    aggregatedValues.put(gbfieldString, Math.max(currentInt, aggregatedValues.get(gbfieldString)));
                }
                break;
            case COUNT:
                aggregatedValues.put(gbfieldString, mergedCount.get(gbfieldString));
                break;
            case SC_AVG:
                // how do we structure this? see comment in Aggregator
                break;
            case SUM_COUNT:
                // how do we structure this? see comment in Aggregator
                break;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab3");

        // Return TupleIterator - need tuple desc and tuple iterable
        // need to build tuple desc and iterable since I just have strings and values
        // TODO add no gorup case?
        // TODO add support for the weird aggs?

        LinkedList<Tuple> tuples = new LinkedList<>();
        Type[] types;
        String[] fieldNames;

        if (gbfield == NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            fieldNames = new String[]{what.toString()};
        } else {
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            fieldNames = new String[]{"AGGREGATE_VALUE", what.toString()};
        }

        TupleDesc td = new TupleDesc(types, fieldNames);
        Tuple tuple;

        for (Map.Entry<String, Integer> entry : aggregatedValues.entrySet()) {
            tuple = new Tuple(td);
            String key = entry.getKey();
            Integer value = entry.getValue();

            switch (gbfieldtype) {
                case INT_TYPE:
                    if (gbfield == NO_GROUPING) {
                        tuple.setField(0, new IntField(value));
                    } else {
                        tuple.setField(0, new IntField(Integer.parseInt(key)));
                        tuple.setField(1, new IntField(value));
                    }
                    break;
                case STRING_TYPE:
                    if (gbfield == NO_GROUPING) {
                        tuple.setField(0, new IntField(value));
                    } else {
                        tuple.setField(0, new StringField(key, Type.STRING_LEN));
                        tuple.setField(1, new IntField(value));
                    }
                    break;
            }

            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }

}
