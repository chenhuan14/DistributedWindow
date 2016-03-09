package nudt.pdl.stormwindow.storm;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.operator.AbsWindowedOperator;
import nudt.pdl.stormwindow.util.Constant;
import nudt.pdl.stormwindow.window.creator.WindowInfo;

/**
 * 一个输入流一个输出流的抽象类
 * @author Administrator
 *
 */
public abstract class WindowedStormBolt extends AbsWindowedOperator implements IRichBolt{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1645582908889146825L;

	private static final Logger LOG = LoggerFactory.getLogger(WindowedStormBolt.class);
	
	private OutputCollector collector;
	
	public WindowedStormBolt()
	{
		super();
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
        try
        {
        	WindowInfo info = getWindowInfo();
        	if(info.isSpiltEqual())
        	{
        		int subWindowNum = context.getComponentTasks(context.getThisComponentId()).size();
        		if(info.getKeepLength() % subWindowNum != 0)
            	{
            		Log.error("window with keeplength: " + info.getKeepLength() +" cannot spilt to " + subWindowNum + " subwindows") ;
            		throw new StreamingException("window with keeplength: " + info.getKeepLength() +" cannot spilt to " + subWindowNum + " subwindows");
            	}
            	info.setKeepLength(info.getKeepLength() / subWindowNum);	
        	}
            initialize();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to initialize function stream.");
            throw new RuntimeException("failed to initialize output stream", e);
        }
		
	}
	
	public void sendToNextBolt(String streamID, Values value)
	{
		collector.emit(streamID,value);
	}
	
	public void sendToNextBolt(Values value)
	{
		collector.emit(value);
	}

	@Override
	public void execute(Tuple input) {
		LOG.debug("start to execute storm bolt");
        if (input == null)
        {
            LOG.error("Input tuple is null.");
            throw new RuntimeException("Input tuple is null.");
        }
        
        String sourceStreamName = input.getSourceStreamId();
        
        if (StringUtils.isEmpty(sourceStreamName))
        {
            LOG.error("sourceStreamName is null.");
            throw new RuntimeException("sourceStreamName is nul");
        }
        
        try
        {
            List<String> inStreams = getInputStream();
 
            for (String streamName : inStreams)
            {
    	
                if (streamName != Constant.DEFAULT_INPUT_STREAM && !sourceStreamName.equals(streamName))
                {
                    continue;
                }
               
                execute(streamName, input);
            }
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to execute tuple.");
            throw new RuntimeException("Failed to execute tuple.", e);
        }
		
        collector.ack(input);
	}

	@Override
    public void cleanup()
    {
        try
        {
           destroy();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to destroy function stream.");
            throw new RuntimeException("Failed to destroy function stream", e);
        }
    }
    

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	

	
}
