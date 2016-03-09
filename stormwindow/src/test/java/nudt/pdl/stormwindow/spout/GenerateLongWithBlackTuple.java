package nudt.pdl.stormwindow.spout;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 处理情况二：采用发送空元组方式
 * 
 * @author Administrator
 *
 */
public class GenerateLongWithBlackTuple extends BaseRichSpout{
	  
	private static final long serialVersionUID = 1L;
	private TopologyContext context;
    private	SpoutOutputCollector _collector;
	static long i = 0 ;
	private List<Integer> numSumTasks;
	private int index;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.context = context;
		_collector = collector;
		index = 0;
		numSumTasks = context.getComponentTasks("sum");
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(100);
		
		
		_collector.emitDirect(numSumTasks.get(index),new Values(i++));
		System.out.println("emit to task: " + index + "\tvalue = " + (i-1));
		for(int i = 0 ; i< numSumTasks.size() ; i++)
		{
			
			if(i == index )
				continue;
			_collector.emitDirect(numSumTasks.get(i), new Values((long)0));
		}
		index = (index + 1) % numSumTasks.size();
	
//		_collector.emit(new Values(i++));	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("long"));
	}
}
