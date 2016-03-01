package nudt.pdl.stormwindow.operator;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.exception.StreamingException;



/**
 * 流处理基础算子接口
 * 这个算子里面的接口，都是运行时接口
 *
 */
public interface IOperator extends Serializable
{
    /**
     * 运行时的初始化接口
     *
     * @param emitters 事件发射器
     * @throws StreamingException 流处理异常
     */
    void initialize()
        throws StreamingException;
    
    /**
     * 运行时的执行接口
     *
     * @param streamName 流名称
     * @param event 事件
     * @throws StreamingException 流处理异常
     */
    void execute(String streamName, Tuple event)
        throws StreamingException;
    
    /**
     * 运行时的销毁接口
     *
     * @throws StreamingException 流处理异常
     */
    void destroy()
        throws StreamingException;
}
