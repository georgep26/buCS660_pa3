package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private String gFieldName;
    private String aFieldName;
    private DbIterator aggIterator;
    private TupleDesc newTd;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.aggIterator = null;

        TupleDesc fedTupleDesc = child.getTupleDesc();
        Type gbFieldType;
        this.aFieldName = fedTupleDesc.getFieldName(afield);
        if (gfield != -1) {
            this.gFieldName = fedTupleDesc.getFieldName(gfield);
            gbFieldType = fedTupleDesc.getFieldType(gfield);
        } else {
            this.gFieldName = null;
            gbFieldType = null;
        }

        Type aFieldType = fedTupleDesc.getFieldType(afield);
        if (aFieldType == Type.INT_TYPE) {
            if (gfield == -1) {
                this.aggregator = new IntegerAggregator(Aggregator.NO_GROUPING, null, afield, aop);
                newTd = new TupleDesc(new Type[]{aFieldType}, new String[]{aop.toString()});
            } else {
                this.aggregator = new IntegerAggregator(gfield, gbFieldType, afield, aop);
                newTd = new TupleDesc(new Type[]{gbFieldType, aFieldType}, new String[]{"AGG_Value", aop.toString()});
            }
        } else {
            if (gfield == -1) {
                this.aggregator = new StringAggregator(Aggregator.NO_GROUPING, null, afield, aop);
                newTd = new TupleDesc(new Type[]{aFieldType}, new String[]{aop.toString()});
            } else {
                this.aggregator = new StringAggregator(gfield, gbFieldType, afield, aop);
                newTd = new TupleDesc(new Type[]{gbFieldType, aFieldType}, new String[]{"AGG_Value", aop.toString()});
            }
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return (this.gfield == -1) ? Aggregator.NO_GROUPING : this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    return gFieldName;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    return aFieldName;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        child.open();

        Tuple childTuple;
        if (aggIterator == null) {
            // merge all child tuples
            while(child.hasNext()) {
                childTuple = child.next();
                aggregator.mergeTupleIntoGroup(childTuple);
            }
            aggIterator = aggregator.iterator();
        }
        aggIterator.open();

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggIterator.hasNext()) {
            return aggIterator.next();
        }
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    return newTd;
    }

    public void close() {
        super.close();
        child.close();
        if (aggIterator != null) {
            aggIterator.close();
        }
    }

    @Override
    public DbIterator[] getChildren() {
	    return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }
    
}
