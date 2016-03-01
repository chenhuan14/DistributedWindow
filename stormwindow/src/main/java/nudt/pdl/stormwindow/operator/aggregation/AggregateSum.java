package nudt.pdl.stormwindow.operator.aggregation;



import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;

public class AggregateSum extends WindowedStormBolt{

	long sum = 0;


	public void process(Tuple[] newData, Tuple[] oldData) {
		processNewData(newData);
		processOldData(oldData);
		sendToNextBolt("sumResult",new Values(sum));
	}
	
	private void processNewData(Tuple[] newData)
	{
		if(newData != null)
		{
			for(int i = 0 ;i < newData.length; i++)
			{
				sum += (long)newData[i].getValue(0);
				
			}
		}
		
	}
	
	private void processOldData(Tuple[] oldData)
	{
		if(oldData != null)
		{
			for(int i = 0 ;i < oldData.length; i++)
			{
				sum -= (long)oldData[i].getValue(0);	
			}
		}
	}
	
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("sumResult", new Fields("result"));
	}
}
