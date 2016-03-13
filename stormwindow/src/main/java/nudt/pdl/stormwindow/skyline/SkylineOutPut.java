package nudt.pdl.stormwindow.skyline;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SkylineOutPut implements IRichBolt{
	
	private OutputCollector collector;
	private int count;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
		int count = input.getIntegerByField("skylinecount");
		List<SkyTuple> skyline =(LinkedList<SkyTuple>) input.getValueByField("skylinearray");
		
		System.out.println("**************************new skyline******************************");
		System.out.println("skyline count = "+count );
		for(SkyTuple tuple : skyline)
		{
			System.out.println(tuple);
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
