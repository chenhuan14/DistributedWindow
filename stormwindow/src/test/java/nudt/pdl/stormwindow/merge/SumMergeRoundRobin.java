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
public class SumMergeRoundRobin implements IRichBolt
{
	TopologyContext context;
	int numberInput;
	List<Integer> sumTasks;
	OutputCollector collector;

	int index;
	Map<Integer,LinkedList<Long>> buffer;
	Map<Integer, Long> localSum;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.context = context;
		this.sumTasks = context.getComponentTasks("sum");
		this.numberInput = sumTasks.size();
		this.collector = collector;
		this.index = 0;
		
		localSum = new HashMap<>();
		
		buffer = new HashMap<>();
		for(int id : sumTasks)
		{
			buffer.put(id, new LinkedList<Long>());
			localSum.put(id, (long)0);
		}
		
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		int taskNum = input.getSourceTask();
		buffer.get(taskNum).add(input.getLong(0));
		
		int targetTask = sumTasks.get(index);
		
		if(buffer.get(targetTask).size() != 0)
		{
			localSum.put(targetTask, buffer.get(targetTask).removeFirst());
			index = (index + 1) % numberInput;
			collector.emit(new Values(getMergeSum()));
		}
	}
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	private long getMergeSum()
	{
		long totolSum = 0;
		for(Long localsum: localSum.values())
		{
			totolSum += localsum;
		}
		return totolSum;
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