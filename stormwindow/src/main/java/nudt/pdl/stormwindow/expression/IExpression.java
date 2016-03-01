package nudt.pdl.stormwindow.expression;


import java.io.Serializable;

import backtype.storm.tuple.Tuple;



/**
 * 
 * <表达式求值接口，对事件进行求值，返回结果>
 * 
 */
public interface IExpression extends Serializable
{
    /**
     * <单流事件表达式求值>
     * @param theEvent 事件
     * @return 结果
     */
    Object evaluate(Tuple tuple);
    
    /**
     * <多流事件表达式求值>
     * @param eventsPerStream 多流事件
     * @return 结果
     */
    Object evaluate(Tuple[] eventsPerStream);
    

}
