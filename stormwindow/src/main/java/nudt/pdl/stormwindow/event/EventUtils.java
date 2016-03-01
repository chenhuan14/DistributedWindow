package nudt.pdl.stormwindow.event;

import nudt.pdl.stormwindow.common.MultiKey;
import nudt.pdl.stormwindow.expression.IExpression;

/**
 * 
 * 事件工具类
 * <功能详细描述>
 * 
 */
public class EventUtils
{
    /**
     * 根据事件及设定属性名称，获得属性值相关MultiKey
     * <功能详细描述>
     * @param event 事件
     * @param propertyNames 设定属性名称
     * @return 属性值相关MultiKey
     */
    public static MultiKey getMultiKey(IEvent event, String[] propertyNames)
    {
        Object[] keys = getProperties(event, propertyNames);
        return new MultiKey(keys);
    }
    
    /**
     * 根据事件及设定属性名称，获得属性值
     * <功能详细描述>
     * @param event 事件
     * @param propertyNames 设定属性名称
     * @return 属性值
     */
    public static Object[] getProperties(IEvent event, String[] propertyNames)
    {
        Object[] properties = new Object[propertyNames.length];
        for (int i = 0; i < propertyNames.length; i++)
        {
            properties[i] = event.getValue(propertyNames[i]);
        }
        return properties;
    }
    
    /**
     * 根据事件及设定表达式，获得值相关MultiKey
     * <功能详细描述>
     * @param event 事件
     * @param exp 设定表达式
     * @return 值相关MultiKey
     */
    public static MultiKey getMultiKey(IEvent event, IExpression[] exp)
    {
        Object[] keys = getExpressionValues(event, exp);
        return new MultiKey(keys);
    }
    
    /**
     * 根据事件及设定表达式，获得值
     * <功能详细描述>
     * @param event 事件
     * @param exp 设定表达式
     * @return 值
     */
    public static Object[] getExpressionValues(IEvent event, IExpression[] exp)
    {
        Object[] expvalue = new Object[exp.length];
        
        for (int i = 0; i < exp.length; i++)
        {
            expvalue[i] = exp[i].evaluate(event);
        }
        return expvalue;
    }
}
