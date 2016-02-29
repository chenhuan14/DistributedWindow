package nudt.pdl.stormwindow.operator;

import java.util.List;
import java.util.Map;

import nudt.pdl.stormwindow.event.IEventType;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.exception.StreamingRuntimeException;
import nudt.pdl.stormwindow.storm.IEmitter;


/**
 * 基础的流处理算子实现类
 * 只实现了基本的并发设置，参数设置等方法
 * <p/>
 * Streaming内部实现都依赖于此类
 * 外部Storm相关不感知此类
 *
 */
public abstract class AbsOperator implements IRichOperator
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 9152942480213961636L;
    

    
    private int parallelNumber;
    
    private String operatorId;
    
    
    private Map<String, IEmitter> emitters;
    
    /**
     * <默认构造函数>
     */
    public AbsOperator()
    {
        
    }
    

    public final void initialize(Map<String, IEmitter> emitterMap)
        throws StreamingException
    {
        
    }

    /**
     * 初始化
     *
     * @throws StreamingException 初始化异常
     */
    public abstract void initialize()
        throws StreamingException;



    /**
     * 设置输入流名称
     *
     * @param streamNames 输入流名称
     * @throws StreamingException 算子处理异常
     */
    public abstract void setInputStream(List<String> streamNames)
        throws StreamingException;
    
    /**
     * 设置输出流名称
     *
     * @param streamName 输出流名称
     * @throws StreamingException 算子处理异常
     */
    public abstract void setOutputStream(String streamName)
        throws StreamingException;
    

    

    public String getOperatorId()
    {
        return this.operatorId;
    }
    
    /**
     * 设置算子id
     *
     * @param id 算子id
     */
    public void setOperatorId(String id)
    {
        operatorId = id;
    }

    public int getParallelNumber()
    {
        return parallelNumber;
    }
    
    /**
     * 设置算子并发度
     *
     * @param number 并发度
     */
    public void setParallelNumber(int number)
    {
        this.parallelNumber = number;
    }
    
   
  
    /**
     * 通过流名称获取emitter
     *
     * @return emitter
     */
    public Map<String, IEmitter> getEmitterMap()
    {
       return emitters;
    }
    
    /**
     * 通过流名称获取emitter
     *
     * @param streamName 流名称
     * @return emitter
     */
    public IEmitter getEmitter(String streamName)
    {
        if (emitters.containsKey(streamName))
        {
            return emitters.get(streamName);
        }
        throw new StreamingRuntimeException("can not get emitter by stream name " + streamName);
    }
    
    /**
     * 通过流名称获取emitter
     *
     * @return emitter
     */
    public IEmitter getEmitter()
    {
        if (emitters.containsKey(getOutputStream()))
        {
            return emitters.get(getOutputStream());
        }
        throw new StreamingRuntimeException("can not get emitter by stream name " + this.getOutputStream());
    }
    
}