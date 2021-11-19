package simpledb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private Integer aggregatedValue;
    private HashMap<String, Integer> mergedCount;
    private HashMap<String, Integer> aggregatedValues;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        aggregatedValues = new HashMap<>();
        mergedCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

        String gbfieldString;
        if (gbfield == NO_GROUPING) {
            gbfieldString = "";
        } else {
            gbfieldString= tup.getField(gbfield).toString();
        }

        mergedCount.putIfAbsent(gbfieldString, 0);
        mergedCount.put(gbfieldString, mergedCount.get(gbfieldString) + 1);

        switch (what) {
            case COUNT:
                aggregatedValues.put(gbfieldString, mergedCount.get(gbfieldString));
                break;
        }

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
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
