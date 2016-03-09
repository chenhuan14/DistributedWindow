package nudt.pdl.stormwindow.window;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.IRenew;
import nudt.pdl.stormwindow.view.IView;
import nudt.pdl.stormwindow.view.ViewImpl;

public class EventBasedSlidingWindow extends ViewImpl implements IWindow, IRenew{
	 /**
	 * 
	 */
	private static final long serialVersionUID = -3143865236497249525L;

	/**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(EventBasedSlidingWindow.class);
    
	/**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * 窗口中的事件
     */
    private ArrayDeque<Tuple> events = new ArrayDeque<Tuple>();
    
    
	
	@Override
	public void update(Tuple[] newData, Tuple[] oldData) {
		// TODO Auto-generated method stub
		
		List<Tuple> expireData = new ArrayList<>();
		//TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        //将新事件加入到窗口中
        if (null == newData || 0 == newData.length)
        {
            LOG.error("Input Length Batch Window newData is Null!");
            return;
        }
        
        for (Tuple event : newData)
        {
        	if(!event.getBooleanByField("evicitionFlag"))
        		events.add(event);
        	else
        		expireData.add(events.removeFirst());
        }
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            dataCollection.update(newData, expireData.toArray(new Tuple[expireData.size()]));
        }
        
        if (this.hasViews())
        {
            updateChild(newData, expireData.toArray(new Tuple[expireData.size()]));
        }
       
	}

	@Override
	public IView renewView() {
        EventBasedSlidingWindow renewWindow = new EventBasedSlidingWindow();
        
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
