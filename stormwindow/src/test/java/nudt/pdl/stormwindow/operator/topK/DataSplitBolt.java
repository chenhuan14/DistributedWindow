package nudt.pdl.stormwindow.operator.topK;

import java.util.ArrayDeque;
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

public class DataSplitBolt implements IRichBolt{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1500175366665590099L;
	private long windowLength;
	private ArrayDeque<Integer> windowIds;
	private HashFunction h1 = Hashing.murmur3_128(13);
	
	TopologyContext context;
	OutputCollector collector;
	List<Integer> topOneTask;
	int tasksNum;
	
	public DataSplitBolt(long windowLength) {
		this.windowLength  = windowLength;
		this.windowIds = new ArrayDeque<>();
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.context = context;
		this.collector = collector;	
		topOneTask = context.getComponentTasks("topOne");
		tasksNum = topOneTask.size();
	}

	/**
	 * （1）根据Hash得到目标Task
	 * （2）向特定子窗口发送过期信号
	 * （3）加入索引窗口
	 * （4）发送新数据
	 */
	@Override
	public void execute(Tuple input) {
		String key = input.getStringByField("word");
		int targeTask = (int) (Math.abs(h1.hashBytes(key.getBytes()).asLong()) % tasksNum);
		
		if(windowIds.size() == windowLength )
		{
			int evcitionTask = windowIds.removeFirst();
			collector.emitDirect(topOneTask.get(evcitionTask), new Values("",0,true));
			System.out.println("------------------tevcition from :" + evcitionTask);
		}
		windowIds.add(targeTask);
		System.out.println("add to :" + targeTask + "\tvalue : " + key );
		
		collector.emitDirect(topOneTask.get(targeTask), new Values(key,input.getInteger(1),false));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count","evicitionFlag"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
