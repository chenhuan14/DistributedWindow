package nudt.pdl.stormwindow.operator.aggregation;


import nudt.pdl.stormwindow.event.IEvent;

import nudt.pdl.stormwindow.exception.StreamingException;

import nudt.pdl.stormwindow.storm.IEmitter;
import nudt.pdl.stormwindow.storm.WindowedStormBolt;

public class AggregateSum extends WindowedStormBolt{

	long sum = 0;


	public void process(IEvent[] newData, IEvent[] oldData) {
		processNewData(newData);
		processOldData(oldData);
		sendResult();
	}
	
	private void processNewData(IEvent[] newData)
	{
		if(newData != null)
		{
			for(int i = 0 ;i < newData.length; i++)
			{
				sum += (long)newData[i].getValue(0);
				
			}
		}
		
	}
	
	private void processOldData(IEvent[] oldData)
	{
		if(oldData != null)
		{
			for(int i = 0 ;i < oldData.length; i++)
			{
				sum += (long)oldData[i].getValue(0);
				
			}
		}
	}
	
	private void sendResult() 
	{
		
		IEmitter emitter = getEmitter();
		
		try {
			emitter.emit(sum);
		} catch (StreamingException e) {
			
			e.printStackTrace();
		}
		
	}
}
