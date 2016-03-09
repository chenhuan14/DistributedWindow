package nudt.pdl.stormwindow.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import nudt.pdl.stormwindow.operator.topK.DataSplitBolt;
import nudt.pdl.stormwindow.operator.topK.RandomWordSpout;
import nudt.pdl.stormwindow.operator.topK.TopOneMerge;
import nudt.pdl.stormwindow.operator.topK.WordTopOne;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;
import nudt.pdl.stormwindow.window.creator.WindowEviction;
import nudt.pdl.stormwindow.window.creator.WindowInfo;
import nudt.pdl.stormwindow.window.creator.WindowType;

public class TopOneTopology {
	
	public static void main(String[] args)
	{
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RandomWordSpout(), 1);
		
		
		WindowedStormBolt topOneBolt = new WordTopOne();	
		// eventSliding 窗口，不需要等分。
		WindowInfo info = new WindowInfo(WindowType.LengthtBased, WindowEviction.EventSliding, 100,false);
		topOneBolt.setWindowInfo(info);
		
		builder.setBolt("dataSplit", new DataSplitBolt(100)).shuffleGrouping("spout");
		
		builder.setBolt("topOne", topOneBolt,5).directGrouping("dataSplit");
		
		builder.setBolt("merge", new TopOneMerge()).shuffleGrouping("topOne");
	
		Config conf = new Config();
		conf.setDebug(false);  
		
		LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("topOne", conf, builder.createTopology());
	}
}
