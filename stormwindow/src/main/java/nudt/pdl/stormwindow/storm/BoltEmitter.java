package nudt.pdl.stormwindow.storm;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.exception.StreamingRuntimeException;

public class BoltEmitter implements IEmitter, Serializable{

	private static final long serialVersionUID = -4271344576924088638L;

	private static final Logger LOG = LoggerFactory.getLogger(BoltEmitter.class);
    
    private String streamName = null;
    
    private OutputCollector outputCollector;
	    
	

    /**
     * <默认构造函数>
     *
     * @param collector spout数据收集器
     * @param name 流名称
     * @param isAck 是否包含acker
     */
    public BoltEmitter(OutputCollector collector, String name)
    {
        if (collector == null)
        {
            LOG.error("Failed to create event emitter, storm collector is null.");
            throw new StreamingRuntimeException("Failed to create event emitter, storm collector is null.");
        }
        
        this.outputCollector = collector;
        this.streamName = name;
 
    }
    
	@Override
	public void emit(Object[] datas) throws StreamingException {
		// TODO Auto-generated method stub
        if (streamName == null)
        {
            outputCollector.emit(new Values(datas));
        }
        else
        {
            outputCollector.emit(streamName, new Values(datas));
        }
	}

	@Override
	public void emit(Object data) throws StreamingException {
		emit(new Object[]{data});
	}

}
