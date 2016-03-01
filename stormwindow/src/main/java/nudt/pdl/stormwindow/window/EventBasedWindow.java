package nudt.pdl.stormwindow.window;



import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.IRenew;
import nudt.pdl.stormwindow.view.IView;
import nudt.pdl.stormwindow.view.ViewImpl;

/**
 * 根据事件来划分窗口，以特定的某个事件为分界来划分窗口
 */
public class EventBasedWindow extends ViewImpl implements IBatch, IWindow, IRenew
{
    
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -6696709815769384593L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(EventBasedWindow.class);
    
    /**
     * 窗口中上个批次中事件  
     */
    private ArrayDeque<Tuple> lastBatch = null;
    
    /**
     * 窗口中当前批次中事件  
     */
    private ArrayDeque<Tuple> curBatch = new ArrayDeque<Tuple>();
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Tuple[] newData, Tuple[] oldData)
    {
        if (null == newData || 0 == newData.length)
        {
            LOG.error("Input Length Batch Window newData is Null!");
            return;
        }
        
        //将事件加入有效数据中
        for (Tuple newEvent : newData)
        {
            //如果是标记事件，则将本批次的事件你发送出去
            if (newEvent.contains("flagEvent"))
            {
                sendBatchData();
            }
            else
            {
                curBatch.add(newEvent);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setDataCollection(IDataCollection dataCollection)
    {
        if (dataCollection == null)
        {
            LOG.error("Invalid dataCollection.");
            throw new IllegalArgumentException("Invalid dataCollection");
        }
        
        this.dataCollection = dataCollection;
    }
    
    /**
     * 返回窗口事件缓存集
     * @return 窗口事件缓存集
     */
    public IDataCollection getIDataCollection()
    {
        return dataCollection;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBatchData()
    {
        if (this.hasViews())
        {
        	Tuple[] newData = null;
        	Tuple[] oldData = null;
            if (!curBatch.isEmpty())
            {
                newData = curBatch.toArray(new Tuple[curBatch.size()]);
            }
            if ((lastBatch != null) && (!lastBatch.isEmpty()))
            {
                oldData = lastBatch.toArray(new Tuple[lastBatch.size()]);
            }
            
            if ((newData != null) || (oldData != null))
            {
                IDataCollection datacollection = getIDataCollection();
                if (datacollection != null)
                {
                    datacollection.update(newData, oldData);
                }
                
                updateChild(newData, oldData);
                
                LOG.debug("The batchdata has been send to childView.");
            }
        }
        
        lastBatch = curBatch;
        curBatch = new ArrayDeque<Tuple>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        EventBasedWindow renewWindow = new EventBasedWindow();
        
        IDataCollection datacollection = getIDataCollection();
        if (datacollection != null)
        {
            IDataCollection renewCollection = datacollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        return renewWindow;
    }
    
}
