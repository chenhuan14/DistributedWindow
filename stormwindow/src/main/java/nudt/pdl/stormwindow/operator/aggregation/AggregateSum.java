package nudt.pdl.stormwindow.operator.aggregation;

import java.util.List;
import java.util.Map;

import nudt.pdl.stormwindow.event.IEvent;
import nudt.pdl.stormwindow.event.IEventType;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.operator.AbsWindowedOperator;
import nudt.pdl.stormwindow.storm.IEmitter;

public class AggregateSum extends AbsWindowedOperator{

	long sum = 0;

	@Override
	public void process(IEvent[] newData, IEvent[] oldData) {
		processNewData(newData);
		processOldData(oldData);
		sendResult();
	}
	
	private void processNewData(IEvent[] newData)
	{
		for(IEvent event : newData)
		{
			sum += (long)event.getValue("long");
			
		}
	}
	
	private void processOldData(IEvent[] oldData)
	{
		for(IEvent event : oldData)
		{
			sum -= (long)event.getValue("long");
			
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
