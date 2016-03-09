package nudt.pdl.stormwindow.triggerpolicy;

import java.util.List;
import java.util.Map;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.util.Constant;
/*
 *  数据分发到子窗口，当触发条件达到时或已达到2N时，发送Trigger信号
 *  Fileds("wIndex","word","count",Constant.TRIGGER_FLAG)
 */
public class DataSplitAndTriggerBolt implements IRichBolt{

	private OutputCollector collector;
	
	private final long windowLength;
	private final int triggerInterval;
	
	private long wIndex;
	private int triggerCount;
	
	List<Integer> tasks;
	int tasksNum;
	
	private HashFunction h1 = Hashing.murmur3_128(13);
	
	
	public DataSplitAndTriggerBolt(long windowLength , int triggerInterval) {
		this.windowLength = windowLength;
		this.triggerInterval = triggerInterval;
		wIndex = 0;
		triggerCount = 0;
	}
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		tasks = context.getComponentTasks("topOne");
		tasksNum = tasks.size();
	}

	@Override
	public void execute(Tuple input) {
		String key = input.getStringByField("word");
		int count = input.getIntegerByField("count");
		int targeTask = (int) (Math.abs(h1.hashBytes(key.getBytes()).asLong()) % tasksNum);
		
		collector.emitDirect(tasks.get(targeTask), new Values(wIndex,key,count,false));
		wIndex++;
		triggerCount++;
		
		if(triggerCount == triggerInterval)
		{
			sendTriggerTuple();
			triggerCount = 0;
		}
		
		if(wIndex == 2*windowLength)
		{
			sendTriggerTuple();
			wIndex = 0;
		}
		
	}
	
	
	private void sendTriggerTuple()
	{
		for(int target : tasks)
		{
			collector.emitDirect(target, new Values(wIndex,"",0,true));
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(Constant.INDEX_IN_WINDOW,"word","count",Constant.TRIGGER_FLAG));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
