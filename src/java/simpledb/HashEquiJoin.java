package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private HashMap<Object, ArrayList<Tuple>> map;
    private Iterator<Tuple> tupleIt;
    private Tuple t1;
    private Tuple t2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        tupleIt = null;
        t1 = null;
        t2 = null;
        map = new HashMap<Object, ArrayList<Tuple>>();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
	return child1.getTupleDesc().getFieldName(p.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (child1 == null || child2 == null) {
            throw new NoSuchElementException("Child is null!");
        }
        child1.open();
        child2.open();
        super.open();
        createMap();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        super.close();
        this.t1=null;
        this.t2=null;
        this.listIt=null;
        this.map.clear();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (tupleIt != null && tupleIt.hasNext()) {
            return combineTuples();
        }


        while (child2.hasNext()) {
            t2 = child2.next();
            ArrayList<Tuple> match = map.get(t2.getField(p.getField2()));
            if (match != null) {
                tupleIt = match.iterator();
                return combineTuples();                
            }
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

    private void createMap() throws DbException, TransactionAbortedException {
        ArrayList<Tuple> currentList;
        // Fill map with field 1 values 
        map.clear();
        while (child1.hasNext()) {
            t1 = child1.next();
            if (map.containsKey(t1.getField(p.getField1()))) {
                currentList = map.get(t1.getField(p.getField1()));
                currentList.add(t1);
            } else {
                ArrayList<Tuple> list = new ArrayList<Tuple>();
                list.add(t1);
                map.put(t1.getField(p.getField1()), list);
            }
            
        }
        
    }

    private Tuple combineTuples(){
        t1 = tupleIt.next();
        int t1Len = t1.getTupleDesc().numFields();
        int t2Len = t2.getTupleDesc().numFields();
        Tuple newTuple = new Tuple(getTupleDesc());
        for (int i=0; i < t1Len; i++) {
            newTuple.setField(i, t1.getField(i));
        }
        for (int j=0; j < t2Len; j++) {
            newTuple.setField(t1Len+j, t2.getField(j));
        }
        return newTuple;
    }
    
}
