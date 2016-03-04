package nudt.pdl.stormwindow.operator.join;

import java.util.Set;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.common.MultiKey;
import nudt.pdl.stormwindow.storm.WindowedJoinBolt;

public class InnerJoinCount extends WindowedJoinBolt{
	

	private static final long serialVersionUID = 8486693248364114308L;
	long matchCount = 0;
	
	public InnerJoinCount() {
		// TODO Auto-generated constructor stub
		super();
	}
	
	@Override
	protected void processJoinResult(Set<MultiKey> newEvents, Set<MultiKey> oldEvents) {
		// TODO Auto-generated method stub
		matchCount = matchCount + newEvents.size() - oldEvents.size();
		for(MultiKey event : newEvents )
		{
			System.out.println("new Match ：" + event.get(0) + event.get(1));
		}
		
		for(MultiKey event : oldEvents )
		{
			System.out.println("eviciton ：" + event.get(0) + event.get(1));
		}
		
		sendToNextBolt( new Values(matchCount));
		
	}

	@Override
	protected boolean isMatched(Tuple lEvent, Tuple rEvnt) {
		
		int ldata = lEvent.getIntegerByField("type");
		int rdata = rEvnt.getIntegerByField("type");
		
		return  ldata == rdata;
	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("matchcount"));
		
	}

	
	

}
