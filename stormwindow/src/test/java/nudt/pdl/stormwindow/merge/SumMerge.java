package nudt.pdl.stormwindow.merge;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/*
 * 上游局部Sum都达到时才计算
 */
public class SumMerge implements IRichBolt
{
	TopologyContext context;
	int numberInput;
	List<Integer> sumTasks;
	OutputCollector collector;
	
	Map<Integer,Boolean> freshed;
	Map<Integer,LinkedList<Long>> buffer;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.context = context;
		this.sumTasks = context.getComponentTasks("sum");
		this.numberInput = sumTasks.size();
		this.collector = collector;
		freshed = new HashMap<>();
		buffer = new HashMap<>();
		for(int id : sumTasks)
		{
			freshed.put(id, false);
			buffer.put(id, new LinkedList<Long>());
		}
		
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		int taskNum = input.getSourceTask();
		if(buffer.get(taskNum).size()==0)
		{
			buffer.get(taskNum).add(input.getLong(0));
			freshed.put(taskNum, true);
		}
		else
			buffer.get(taskNum).add(input.getLong(0));
		
		if(!freshed.values().contains(false))
		{
			long totolSum = 0;
			for(LinkedList<Long> list : buffer.values())
			{
				totolSum += list.removeFirst();
			}
			
			collector.emit(new Values(totolSum));
			
			for(int id : sumTasks)
			{
				freshed.put(id, false);
			}
		}
		
	}
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("totolSum"));
		
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
}