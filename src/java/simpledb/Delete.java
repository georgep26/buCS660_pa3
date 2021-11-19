package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private boolean open;
    private boolean deleted;
    private TupleDesc returnTd;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        this.open = false;
        this.deleted = false;
        returnTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Deleted tuple count"});
    }

    public TupleDesc getTupleDesc() {
        return returnTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        open = true;
    }

    public void close() {
        super.close();
        child.close();
        open = false;
        deleted = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!open) {
            throw new DbException("iterator has not been opened");
        }

        if (deleted) {
            return null;
        }

        int deletedCount = 0;
        while(child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, child.next());
                deletedCount++;
            } catch (IOException e) {
                e.printStackTrace();
                throw new DbException("Error while deleting tuple from table");
            }
        }

        deleted = true;

        Tuple countTuple = new Tuple(returnTd);
        countTuple.setField(0, new IntField(deletedCount));

        return countTuple;
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
