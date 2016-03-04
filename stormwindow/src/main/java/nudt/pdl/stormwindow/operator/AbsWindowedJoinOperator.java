package nudt.pdl.stormwindow.operator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.common.MultiKey;
import nudt.pdl.stormwindow.common.Pair;

import nudt.pdl.stormwindow.exception.StreamingException;

import nudt.pdl.stormwindow.process.join.IEventCollection;
import nudt.pdl.stormwindow.process.join.SimpleEventCollection;

import nudt.pdl.stormwindow.util.Constant;
import nudt.pdl.stormwindow.view.FirstLevelStream;
import nudt.pdl.stormwindow.view.JoinProcessView;
import nudt.pdl.stormwindow.window.IWindow;
import nudt.pdl.stormwindow.window.creator.WindowCreator;
import nudt.pdl.stormwindow.window.creator.WindowInfo;

/**
 * Join操作抽象类，两个输入流，一个输出流
 * @author Administrator
 *
 */
public abstract class AbsWindowedJoinOperator extends AbsOperator implements IProcessor{

	private static final long serialVersionUID = 6398008597839672406L;
	private final int streamNum = 2;
	private WindowInfo leftWindowInfo;
	private WindowInfo rightWindowInfo;
	
	private FirstLevelStream leftFirstStream ;
	private FirstLevelStream rightFirstStream ;
	
	private IWindow leftWindow;
	private IWindow rightWindow;
	
	private String leftStreamName;
	private String rightStreamName;
	
	private String outputStreamName;
	
	private IEventCollection leftEventCollection;
	private IEventCollection rightEventCollection;
	
    private LinkedHashSet<MultiKey> oldResults = new LinkedHashSet<MultiKey>();
    
    private LinkedHashSet<MultiKey> newResults = new LinkedHashSet<MultiKey>();
	

	public AbsWindowedJoinOperator()
	{
		super();
		leftWindowInfo = new WindowInfo();
		rightWindowInfo = new WindowInfo();
		
		leftFirstStream = new FirstLevelStream();
		rightFirstStream = new FirstLevelStream();
	
		leftStreamName = Constant.DEFAULT_JOININPUT_LEFT_STREAM;
		rightStreamName = Constant.DEFAULT_JOININPUT_RIGHT_STREAM;
		
		leftEventCollection = new SimpleEventCollection(leftStreamName);
		rightEventCollection = new SimpleEventCollection(rightStreamName);
		
		leftStreamName = Constant.DEFAULT_JOININPUT_LEFT_STREAM;
		rightStreamName = Constant.DEFAULT_JOININPUT_RIGHT_STREAM;
		outputStreamName = Constant.DEFAULT_OUTPUT_STREAM;
		
	}
	
	
	
	
	public void setLeftWindow(WindowInfo leftWindowInfo)
	{
		this.leftWindowInfo = leftWindowInfo;
	}
	
	public void setRightWindow(WindowInfo rightWindowInfo)
	{
		this.rightWindowInfo = rightWindowInfo;
	}
	
	public void setLeftStreamName(String name)
	{
		this.leftStreamName = name;
	}
	
	public void setRightStreamName(String name)
	{
		this.rightStreamName = name;
	}
	
	

	/* (non-Javadoc)
	 * @see nudt.pdl.stormwindow.operator.AbsOperator#initialize()
	 */
	@Override
	public void initialize() throws StreamingException {
		JoinProcessView processView = new JoinProcessView();
		processView.setProcessor(this);
		
		leftWindow = WindowCreator.createInstance(leftWindowInfo);
		rightWindow = WindowCreator.createInstance(rightWindowInfo);
		
		if(null == leftFirstStream || null == rightFirstStream)
		{
			throw new StreamingException("no such window type");
		}
		else
		{
			leftWindow.addView(processView);
			rightWindow.addView(processView);
			
			leftFirstStream.addView(leftWindow);
			rightFirstStream.addView(rightWindow);
			
			leftFirstStream.start();
			rightFirstStream.start();
		}
	}
	
	@Override
	public void execute(String streamName, Tuple event) throws StreamingException {
		if(streamName.equals(leftStreamName))
			leftFirstStream.add(event);
		else if(streamName.equals(rightStreamName))
			rightFirstStream.add(event);
		else
		{
			throw new StreamingException("no such inputStream：" + streamName);
		}
	}

	
	
	
	/* (non-Javadoc)
	 * @see nudt.pdl.stormwindow.operator.IRichOperator#getInputStream()
	 */
	@Override
	public List<String> getInputStream() {
		// TODO Auto-generated method stub
		ArrayList<String> inputStreams = new ArrayList<>();
		inputStreams.add(leftStreamName);
		inputStreams.add(rightStreamName);
		return inputStreams;
	}




	/* (non-Javadoc)
	 * @see nudt.pdl.stormwindow.operator.IRichOperator#getOutputStream()
	 */
	@Override
	public String getOutputStream() {
		// TODO Auto-generated method stub
		return this.outputStreamName;
	}




	@Override
	public void destroy() throws StreamingException {
		// TODO Auto-generated method stub
		leftFirstStream.stop();
		rightFirstStream.stop();	
	}
	
	

	/* (non-Javadoc)
	 * @see nudt.pdl.stormwindow.operator.IProcessor#process(nudt.pdl.stormwindow.event.Tuple[], nudt.pdl.stormwindow.event.Tuple[])
	 */
	@Override
	public void process(Tuple[] newData, Tuple[] oldData) {
		Tuple event = getCurrentEvent(newData, oldData);
		if(null == event)
			return;

			
		int streamIndex = getStreamIndex(event);
		
	    //将更新数据按数据流组织
        Tuple[][] newDataPerStream = new Tuple[streamNum][];
        Tuple[][] oldDataPerStream = new Tuple[streamNum][];
        
        newDataPerStream[streamIndex] = newData;
        oldDataPerStream[streamIndex] = oldData;
        
        synchronized(this){
        	maintainData(newDataPerStream, oldDataPerStream);
        }
        
     
        //对事件JOIN
        Pair<Set<MultiKey>, Set<MultiKey>> composedEvents = join(newDataPerStream, oldDataPerStream);
        
        //处理join结果
        processJoinResult(composedEvents.getFirst(), composedEvents.getSecond());
      
       
	}
	
	
	public IEventCollection getLeftCollection()
	{
		return leftEventCollection;
	}
	
	public IEventCollection getRightCollection()
	{
		return rightEventCollection;
	}
	
	public void maintainData(Tuple[][] newDataPerStream, Tuple[][] oldDataPerStream)
	{
        if (newDataPerStream.length != 2 || oldDataPerStream.length != 2)
        {
            
            throw new RuntimeException("This updated streams numbers is not for Bi Stream Join.");
        }
        //默认0下标为左流，1下标为右流
        leftEventCollection.addRemove(newDataPerStream[0], oldDataPerStream[0]);
        rightEventCollection.addRemove(newDataPerStream[1], oldDataPerStream[1]);
    }
	
	
	 private Pair<Set<MultiKey>, Set<MultiKey>> join(Tuple[][] newDataPerStream, Tuple[][] oldDataPerStream)
	    {
	        if (null == newDataPerStream && null == oldDataPerStream)
	        {
	            throw new RuntimeException("Mission impossible.");
	        }
	        
	        if ((null != newDataPerStream && newDataPerStream.length != streamNum)
	            || (null != oldDataPerStream && oldDataPerStream.length != streamNum))
	        {
	     
	            throw new RuntimeException("This updated streams numbers do NOT match with Bi Stream Join.");
	        }
	        
	        newResults.clear();
	        oldResults.clear();
	        
	 
            if (null != oldDataPerStream)
            {
                for (int streamIndex = 0; streamIndex < oldDataPerStream.length; streamIndex++)
                {
                    compose(oldDataPerStream[streamIndex], streamIndex, oldResults);
                }
            }
	        
	        
	        if (null != newDataPerStream)
	        {
	            for (int streamIndex = 0; streamIndex < newDataPerStream.length; streamIndex++)
	            {
	                compose(newDataPerStream[streamIndex], streamIndex, newResults);
	            }
	        }
	        //保证结果中任一个都 非NULL
	        return new Pair<Set<MultiKey>, Set<MultiKey>>(newResults, oldResults);
	    }
	
	    /**
	     * 将更新数据与窗口中的对方流数据,根据KEY（条件）进行匹配
	     * 根据是否允许Join 空流，由具体的Composer进行操作
	     * @param events 更新数据
	     * @param streamIndex 更新数据流的ID
	     * @param result 组合结果
	     */
	    private void compose(Tuple[] events, int streamIndex, Set<MultiKey> result)
	    {
	        if (events == null || events.length == 0)
	        {
	            return;
	        }
	        
	        ArrayDeque<Tuple[]> joinTemp = new ArrayDeque<Tuple[]>();
	        for (Tuple theEvent : events)
	        {
	            perEventCompose(theEvent, streamIndex, joinTemp);
	            
	            // 如果joinTemp结果无事件，则没有任何结果
	            for (Tuple[] row : joinTemp)
	            {
	                result.add(new MultiKey(row));
	            }
	            joinTemp.clear();
	        }
	    }
	    
	    /**
	     * 根据条件（有索引数据）进行JOIN，无匹配事件，不输出
	     * <功能详细描述>
	     * @param lookupEvent 待匹配事件
	     * @param index 待匹配事件所在流ID
	     * @param result 匹配的双流事件
	     */
	    protected void perEventCompose(Tuple lookupEvent, int index, Collection<Tuple[]> result)
	    {
	        if (null == lookupEvent)
	        {
	            return;
	        }
	        
	        Set<Tuple> joinedEvents = getMatchEvents(lookupEvent, index);
	        
	        if (joinedEvents == null)
	        {
	            return;
	        }
	        
	        // Create result row for each found event
	        for (Tuple joinedEvent : joinedEvents)
	        {
	            Tuple[] events = new Tuple[streamNum];
	            events[index] = lookupEvent;
	            events[streamNum - index - 1] = joinedEvent;
	            result.add(events);
	        }
	    }
	    
    /**
     * 获得对方流中匹配事件
     * <功能详细描述>
     * @param lookupEvent 事件待匹配事件
     * @param index 待匹配事件所在的流ID
     * @return 对方流中匹配事件
     */
    private  Set<Tuple> getMatchEvents(Tuple lookupEvent, int index)
    {
    	Set<Tuple> joinedEvents = new LinkedHashSet<>();
    	IEventCollection collection = (index == 0) ?   rightEventCollection : leftEventCollection;
    	Set<Tuple> datas = collection.getAllEvents();
    	if(null == datas || datas.size() ==0)
    		return null;
    	else
    	{
    		for(Tuple event : datas)
    		{
    			if(isMatched(lookupEvent, event))
    				joinedEvents.add(event);
    		}
    	}
    	return joinedEvents;
    	
    }
	    
    protected abstract void processJoinResult(Set<MultiKey> newEvents, 
	            Set<MultiKey> oldEvents);
 
	    
	protected abstract boolean isMatched(Tuple lEvent, Tuple rEvnt);;
    /**
     * <从新事件和旧事件中获取事件，如果新事件不为空，则从新事件获取，否则从旧事件中获取>
     * @param newData 新事件
     * @param oldData 旧事件
     * @return 事件
     */
    private Tuple getCurrentEvent(Tuple[] newData, Tuple[] oldData)
    {
        if (null == newData && null == oldData)
        {
            return null;
        }
        
        Tuple event = null;
        
        if (null != newData)
        {
            event = newData[0];
            if (null != event)
            {
                return event;
            }
        }
        return oldData[0];
    }

    
    /**
     * <返回事件对应类型在Join操作中索引>
     * @param event 事件
     * @return Join索引
     */
    private int getStreamIndex(Tuple event)
    {
        String name = event.getSourceStreamId();
        if(name.equals(leftStreamName))
        		return 0;
        else if (name.equals(rightStreamName))
        	return 1;
        
        throw new RuntimeException("Wrong stream name.");
    }
    


    
}
