package nudt.pdl.stormwindow.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import nudt.pdl.stormwindow.operator.topK.DataSplitBolt;
import nudt.pdl.stormwindow.operator.topK.RandomWordSpout;
import nudt.pdl.stormwindow.operator.topK.TopOneMerge;
import nudt.pdl.stormwindow.operator.topK.WordTopOne;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.triggerpolicy.DataSplitAndTriggerBolt;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;

public class TriggerTopOneTopology {
	public static final long windowLength = 20;
	public static final int triggerInterval = 2;
	
	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RandomWordSpout(), 1);
		
		builder.setBolt("dataSplit", new DataSplitAndTriggerBolt(windowLength, triggerInterval)).shuffleGrouping("spout");
		
		WindowedStormBolt topOneBolt = new WordTopOne();	
		
		// triggerSliding窗口，不需要等分。
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.TriggerSliding, windowLength,false);
		topOneBolt.setWindowInfo(info); 
		

		builder.setBolt("topOne", topOneBolt,2).directGrouping("dataSplit");
		
		builder.setBolt("merge", new TopOneMerge()).shuffleGrouping("topOne");
	
		Config conf = new Config();
		conf.setDebug(false);  
		
		LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("topOne", conf, builder.createTopology());
	}
}
