package nudt.pdl.stormwindow.group;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class ModStreamGrouping implements CustomStreamGrouping{
	
	private WorkerTopologyContext context;
	private List<Integer> targetTasks;
	
	
	

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		this.context = context;
		this.targetTasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
		
		
		return null;
	}

}
