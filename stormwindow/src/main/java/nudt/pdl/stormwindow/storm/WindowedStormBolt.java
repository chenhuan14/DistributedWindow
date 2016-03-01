package nudt.pdl.stormwindow.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Container;
import nudt.pdl.stormwindow.event.IEventType;
import nudt.pdl.stormwindow.event.TupleEvent;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.operator.AbsWindowedOperator;
import nudt.pdl.stormwindow.operator.IRichOperator;
import nudt.pdl.stormwindow.util.Constant;

public abstract class WindowedStormBolt extends AbsWindowedOperator implements IRichBolt{
	
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
		
		Map<String,IEmitter> emitters = createEmitters(collector);
        try
        {
            initialize(emitters);
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to initialize function stream.");
            throw new RuntimeException("failed to initialize output stream", e);
        }
		
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
            if(inStreams == null)
            {
                inStreams = new ArrayList<>();
                inStreams.add(Constant.DEFAULT_INPUT_STREAM);
            }
            
            for (String streamName : inStreams)
            {
            	IEventType type = (IEventType) getInputSchema().get(streamName);
            	if(type == null)
            		type = Constant.DEFAULT_INPUT_SCHMA;
            	
                if (streamName != Constant.DEFAULT_INPUT_STREAM && !sourceStreamName.equals(streamName))
                {
                    continue;
                }
               
                TupleEvent event = TupleTransform.tupeToEvent(input, type);
                execute(streamName, event);
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
    

	/*
	 *只实现只有一个输出流的操作 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 IEventType schema = getOutputSchema();
         if (schema == null)
         {
             schema = Constant.DEFAULT_OUTPUT_SCHMA;
         }
         
         if (!StringUtils.isEmpty(getOutputStream()) && !getOutputStream().equals(Constant.DEFAULT_OUTPUT_STREAM))
         {
             declarer.declareStream(getOutputStream(), new Fields(schema.getAllAttributeNames()));
         }
         else
         {
             declarer.declare(new Fields(schema.getAllAttributeNames()));
         }
	}

	/**
	 * 目前只实现了只有一个输出流的操作
	 * @param collector
	 * @return
	 */
	private Map<String, IEmitter> createEmitters(OutputCollector collector)
    {
        Map<String, IEmitter> emitters = Maps.newHashMap();
        BoltEmitter emitter = new BoltEmitter(collector, getOutputStream());
        emitters.put(getOutputStream(), emitter);
        return emitters;
    }

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	

	
}
