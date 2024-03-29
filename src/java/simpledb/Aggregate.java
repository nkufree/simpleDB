package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private  OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    
    private Aggregator aggregator;
    private OpIterator it;
    private TupleDesc td;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    /**
    构造函数。
    实现提示：根据afield的类型，您可能需要构造一个{@link IntegerAggregator}或{@link StringAggregator}来帮助您实现readNext()方法。
    @param child
        提供元组给我们的OpIterator。
    @param afield
        我们正在计算聚合的列。
    @param gfield
        我们正在分组结果的列，如果没有分组，则为-1。
    @param aop
        聚合操作符。
    **/
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	
    	
    	Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);
    	
        if(this.child.getTupleDesc().getFieldType(this.afield) == (Type.STRING_TYPE)){
            this.aggregator = new StringAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }else{
            this.aggregator = new IntegerAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }
        this.it = this.aggregator.iterator();
        // create tupleDesc for agg
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        // group field
        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.afield));
        names.add(this.child.getTupleDesc().getFieldName(this.afield));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	return td.getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
    	if(this.gfield == -1)
            return this.td.getFieldName(0);
        else
            return this.td.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	this.child.open();
        while (this.child.hasNext())
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        this.it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    /**
    返回下一个元组。如果有一个分组字段，则第一个字段是我们正在分组的字段，第二个字段是计算聚合的结果。
    如果没有分组字段，则结果元组应包含一个表示聚合结果的字段。如果没有更多元组，则应返回null。
    */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	while (this.it.hasNext())
            return this.it.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	this.child.rewind();
        this.it.rewind();

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
    /**
    返回此Aggregate的TupleDesc。如果没有分组字段，则它将具有一个字段-聚合列。
    如果有一个分组字段，则第一个字段将是分组字段，第二个字段将是聚合值列。
    聚合列的名称应具有说明性。
    例如：“aggName(aop) (child_td.getFieldName(afield))”，其中aop和afield是构造函数中给定的，child_td是子迭代器的TupleDesc。
    */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return td;
    }

    public void close() {
	// some code goes here
    	super.close();
    	child.close();
    	it.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
    	 this.child = children[0];
         List<Type> types = new ArrayList<>();
         List<String> names = new ArrayList<>();
         Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);
         // group field
         if (gfieldtype != null) {
             types.add(gfieldtype);
             names.add(this.child.getTupleDesc().getFieldName(this.gfield));
         }
         types.add(this.child.getTupleDesc().getFieldType(this.afield));
         names.add(this.child.getTupleDesc().getFieldName(this.afield));
         if (aop.equals(Aggregator.Op.SUM_COUNT)) {
             types.add(Type.INT_TYPE);
             names.add("COUNT");
         }
         assert (types.size() == names.size());
         this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }
    
}
