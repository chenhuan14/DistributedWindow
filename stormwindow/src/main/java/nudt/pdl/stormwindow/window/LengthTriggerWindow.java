package nudt.pdl.stormwindow.window;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.util.Constant;
import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.IRenew;
import nudt.pdl.stormwindow.view.IView;
import nudt.pdl.stormwindow.view.ViewImpl;
/*
 * 通过保存各个元组在整体窗口的下标，当收到新元组时通过比较新旧下标来判断是否超过窗口范围。
 * 新元组可能是新数据也可能是上端发送的心跳
 */
public class LengthTriggerWindow extends ViewImpl implements IWindow, IRenew{
	 /**
	 * 
	 */
	private static final long serialVersionUID = -3143865236497249525L;

	/**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(LengthTriggerWindow.class);
    
	/**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * 窗口中的事件.Tuple(long index, Values) index为数据在窗口的下标，范围为[1,2N]，N为窗口大小
     */
    private ArrayDeque<Tuple> events = new ArrayDeque<Tuple>();
    
    private final long keepLength;
    
    public LengthTriggerWindow(long keepLength) {
		// TODO Auto-generated constructor stub
    	this.keepLength = keepLength;
	}
   
	@Override
	public void update(Tuple[] newData, Tuple[] oldData) {
		// TODO Auto-generated method stub
		
		List<Tuple> expireData = new ArrayList<>();
		List<Tuple> newDataWithoutTrigger = new ArrayList<>();
		//TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        //将新事件加入到窗口中
        if (null == newData || 0 == newData.length)
        {
            return;
        }
        
        for (Tuple event : newData)
        {
        	if(!event.contains(Constant.INDEX_IN_WINDOW))
        	{
        		LOG.error("data cannot contain index field!");
        		throw new RuntimeException("data cannot contain index field!");
        	}
        	long newIndex = event.getLongByField(Constant.INDEX_IN_WINDOW);
        	
        	
        	if(events.size() == 0)
        	{
        		//非trigger信号，加入窗口
            	if(!event.getBooleanByField(Constant.TRIGGER_FLAG))
            	{
            		events.add(event);
            		newDataWithoutTrigger.add(event);
            	}
            	continue;
        	}
        	
        
        	Tuple oldest = events.getFirst();        	
        	long oldIndex = oldest.getLongByField(Constant.INDEX_IN_WINDOW);
        	
        	//当新元组下标大于旧元组时，可以通过淘汰满足（New.index - old.index >= window.size）的所有旧元素
        	//当新元组下标小于旧元组时，可以通过淘汰满足(New.index +window.size>=old.index）
        	if(newIndex > oldIndex)
        	{
        		while(newIndex - oldIndex >= keepLength)
        		{
        			Tuple temp = events.removeFirst();
        			expireData.add(temp);
        			
        			if(events.size() == 0)
                		break;	
        			oldest = events.getFirst();
        			oldIndex = oldest.getLongByField(Constant.INDEX_IN_WINDOW);
        		}
        	}
        	else if(oldIndex > newIndex)
        	{
        		while(newIndex + keepLength >= oldIndex)
        		{
        			Tuple temp = events.removeFirst();
        			expireData.add(temp);
        			
        			if(events.size() == 0)
                		break;	
        			oldest = events.getFirst();
        			oldIndex = oldest.getLongByField(Constant.INDEX_IN_WINDOW);
        		}
        	}
        	else
        	{
        		throw new RuntimeException("index error");
        	}
        	
        	if(!event.getBooleanByField(Constant.TRIGGER_FLAG))
        	{
        		events.add(event);
        		newDataWithoutTrigger.add(event);
        	}
        }
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            dataCollection.update(newDataWithoutTrigger.toArray(new Tuple[newDataWithoutTrigger.size()]), 
            		expireData.toArray(new Tuple[expireData.size()]));
        }
        
        if (this.hasViews())
        {
            updateChild(newDataWithoutTrigger.toArray(new Tuple[newDataWithoutTrigger.size()]),
            		expireData.toArray(new Tuple[expireData.size()]));
        }
	}

	@Override
	public IView renewView() {
        LengthTriggerWindow renewWindow = new LengthTriggerWindow(keepLength);
        
        IDataCollection datacollection = getDataCollection();
        if (datacollection != null)
        {
            IDataCollection renewCollection = datacollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        return renewWindow;
	}

	@Override
	public void setDataCollection(IDataCollection dataCollection) {
		// TODO Auto-generated method stub
		this.dataCollection = dataCollection;
		
	}
	
    /**
     * 返回窗口事件缓存集
     * @return 窗口事件缓存集
     */
    public IDataCollection getDataCollection()
    {
        return dataCollection;
    }

}
