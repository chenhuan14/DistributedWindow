package nudt.pdl.stormwindow.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer.ConditionObject;

import org.apache.commons.lang.StringUtils;

import nudt.pdl.stormwindow.event.IEventType;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.exception.StreamingRuntimeException;
import nudt.pdl.stormwindow.storm.IEmitter;
import nudt.pdl.stormwindow.util.Constant;


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
    
	private IEventType outputSchema;
	private String outputStream;
    
    private List<String> inputStreams;
    private Map<String, IEventType> inputSchemas;
    
    /**
     * <默认构造函数>
     */
    public AbsOperator()
    {
    	inputStreams = new ArrayList<String>();
		inputSchemas = new HashMap<>();
		outputStream = Constant.DEFAULT_OUTPUT_STREAM;
		outputSchema = Constant.DEFAULT_OUTPUT_SCHMA;
    }
    

    public final void initialize(Map<String, IEmitter> emitterMap)
        throws StreamingException
    {
    	this.addInputStream(Constant.DEFAULT_INPUT_STREAM);
		this.addInputSchema(Constant.DEFAULT_INPUT_STREAM, Constant.DEFAULT_INPUT_SCHMA);
    	
		this.emitters = emitterMap;
   
        initialize();
    }
    
    @Override
	public List<String> getInputStream() {
		// TODO Auto-generated method stub
		return this.inputStreams;
	}

	@Override
	public String getOutputStream() {
		// TODO Auto-generated method stub
		return this.getOutputStream();
	}

	@Override
	public Map<String, IEventType> getInputSchema() {
		// TODO Auto-generated method stub
		return inputSchemas;
	}

	@Override
	public IEventType getOutputSchema() {
		// TODO Auto-generated method stub
		return outputSchema;
	}
	
	
	public void setInputStream(List<String> streamNames) throws StreamingException {
		this.inputStreams = streamNames;
		
	}

	
	public void setOutputStream(String streamName) throws StreamingException {
		this.outputStream = streamName;
		
	}

	
	public void setInputSchema(Map<String, IEventType> schemas) throws StreamingException {
		this.inputSchemas = schemas;
	}

	
	public void setOutputSchema(IEventType schema) throws StreamingException {
		this.outputSchema = schema;
		
	}


    /**
     * 初始化
     *
     * @throws StreamingException 初始化异常
     */
    public abstract void initialize()
        throws StreamingException;


    /**
     * 添加输入流
     * @param streamName 输入流名称
     */
    public void addInputStream(String streamName)
    {
        if (!StringUtils.isEmpty(streamName))
        {
            if (!inputStreams.contains(streamName))
            {
                inputStreams.add(streamName);
            }
        }
    }
    
    /**
     * 添加输入流schema
     * @param streamName 流名称
     * @param schema 输入流schema
     */
    public void addInputSchema(String streamName, IEventType schema)
    {
        if (schema != null)
        {
            inputSchemas.put(streamName, schema);
        }
    }
	

    

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