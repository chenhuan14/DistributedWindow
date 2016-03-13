package nudt.pdl.stormwindow.datasplit;

import java.util.List;
import java.util.Map;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class RoundRobinSpilt implements IRichBolt{
	

	private String outputComponentID;
	private OutputCollector collector;
	private List<Integer> tasks;


	
	private int index ;
	
	public RoundRobinSpilt(String outputComponentID) 
	{

		this.outputComponentID = outputComponentID;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		tasks = context.getComponentTasks(outputComponentID);
		
		index = 0;
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
	
		collector.emitDirect(tasks.get(index), input.getValues());
		index = (index + 1) % tasks.size();
		
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rawdata"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
