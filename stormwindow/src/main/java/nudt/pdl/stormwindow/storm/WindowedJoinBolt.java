package nudt.pdl.stormwindow.storm;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.operator.AbsWindowedJoinOperator;
import nudt.pdl.stormwindow.util.Constant;

public abstract class WindowedJoinBolt extends AbsWindowedJoinOperator implements IRichBolt {

	private static final long serialVersionUID = -2326023250963245052L;
	private static final Logger LOG = LoggerFactory.getLogger(WindowedStormBolt.class);
	private OutputCollector collector;
	
	public WindowedJoinBolt() {
		// TODO Auto-generated constructor stub
		super();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		this.collector = collector;
        try
        {
            initialize();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to initialize function stream.");
            throw new RuntimeException("failed to initialize output stream", e);
        }	
	}

	
	

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
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
	public void cleanup() {
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

	protected void sendToNextBolt(String streamID, Values value)
	{
		if(streamID.equals(Constant.DEFAULT_OUTPUT_STREAM))
			collector.emit(value);
		else
			collector.emit(streamID,value);
	}
	

	protected void sendToNextBolt(Values value)
	{
			collector.emit(value);
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
