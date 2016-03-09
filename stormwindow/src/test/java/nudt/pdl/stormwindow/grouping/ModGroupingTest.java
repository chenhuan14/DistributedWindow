package nudt.pdl.stormwindow.grouping;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ModGroupingTest {
	public static class TestUidSpout extends BaseRichSpout{
		
		SpoutOutputCollector collector;
		private Random random;

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
			random = new Random(System.currentTimeMillis());
		}

		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			collector.emit(new Values(Math.abs(random.nextInt(100000))));
				
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("uid"));
			
		}
		
	}
}
