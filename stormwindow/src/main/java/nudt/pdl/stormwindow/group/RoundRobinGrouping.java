package nudt.pdl.stormwindow.group;

import java.util.Arrays;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class RoundRobinGrouping implements CustomStreamGrouping{
	private WorkerTopologyContext context;
	private List<Integer> targetTasks;
	private  int targetNumber;
	private int index = 0;
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		this.context = context;
		this.targetTasks = targetTasks;
		targetNumber = targetTasks.size();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
				
		Integer target = targetTasks.get(index);
		index = (index + 1) % targetNumber;
		
		return Arrays.asList(target);
		
	}

}
