package nudt.pdl.stormwindow.operator.aggregation;

import java.util.List;
import java.util.Map;

import nudt.pdl.stormwindow.event.IEvent;
import nudt.pdl.stormwindow.event.IEventType;
import nudt.pdl.stormwindow.operator.AbsWindowedOperator;

public class AggregateSum extends AbsWindowedOperator{

	long sum = 0;
	
	
	
	public  AggregateSum(String inputStream, String OutputStream) {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public List<String> getInputStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOutputStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, IEventType> getInputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IEventType getOutputSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void process(IEvent[] newData, IEvent[] oldData) {
		// TODO Auto-generated method stub
		
	}

}
