package nudt.pdl.stormwindow.operator.topK;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TopOneMerge implements IBasicBolt{

	private Map<Integer,Tuple> topOnes;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		topOnes = new HashMap<>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		int sourceTaskId = input.getSourceTask();
		topOnes.put(sourceTaskId,input);
		Tuple topOne = getTopOne();
		if(topOne != null)
		{
			System.out.println("topOne: "+ topOne.getStringByField("word") + " = " + topOne.getIntegerByField("count"));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	private Tuple getTopOne()
	{
		int max  = 0;
		Tuple temp = null;
		for(Tuple t: topOnes.values())
		{
			int count  = t.getIntegerByField("count");
			if(count > max)
			{
				max = count;
				temp = t;
			}
		}
		return temp;
	}

}
