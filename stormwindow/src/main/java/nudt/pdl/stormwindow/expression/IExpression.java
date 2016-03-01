package nudt.pdl.stormwindow.expression;


import java.io.Serializable;

import nudt.pdl.stormwindow.event.IEvent;

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
    Object evaluate(IEvent theEvent);
    
    /**
     * <多流事件表达式求值>
     * @param eventsPerStream 多流事件
     * @return 结果
     */
    Object evaluate(IEvent[] eventsPerStream);
    
    /**
     * <返回表达式返回类型>
     * <功能详细描述>
     * @return 类型
     */
    Class< ? > getType();
}
