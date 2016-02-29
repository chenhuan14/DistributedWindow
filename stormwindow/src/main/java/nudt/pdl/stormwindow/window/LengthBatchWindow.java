package nudt.pdl.stormwindow.window;



import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nudt.pdl.stormwindow.event.IEvent;
import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.IView;


/**
 * <长度跳动窗口实现类>
 * 
 */
public class LengthBatchWindow extends LengthBasedWindow implements IBatch
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -9202732897486801898L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(LengthBatchWindow.class);
    
    /**
     * 窗口中上个批次中事件  
     */
    private ArrayDeque<IEvent> lastBatch = null;
    
    /**
     * 窗口中当前批次中事件  
     */
    private ArrayDeque<IEvent> curBatch = new ArrayDeque<IEvent>();
    
    /**
     * <默认构造函数>
     *@param keepLength 窗口保持长度
     */
    public LengthBatchWindow(long keepLength)
    {
        super(keepLength);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        /**
         * TODO 
         * 未考虑窗口叠加时，上一个窗口传递的过期数据的处理，同时认为当前newData为一条。
         * 后续考虑窗口叠加时，newData可以为多条。
         */
        if (null == newData || 0 == newData.length)
        {
            LOG.error("Input Length Batch Window newData is Null!");
            return;
        }
        
        //将事件插入有效数据中
        for (IEvent newEvent : newData)
        {
            curBatch.add(newEvent);
        }
        //LOG.debug("The newData has been added to LengthBatchWindow");
        
        //判断是否队列已满
        if (curBatch.size() < getKeepLength())
        {
            //LOG.debug("LengthBatchWindow current Batch Size is :{}", curBatch.size());
            return;
        }
        
        //如果队列已满，则发送数据到子视图
        sendBatchData();
    }
    
    /**
     * <当队列已满时，发送数据到子视图>
     * <当队列已满时，发送数据到子视图。处理完成后，将新数据和旧数据重新赋值。 >
     */
    public void sendBatchData()
    {
        if (this.hasViews())
        {
            IEvent[] newData = null;
            IEvent[] oldData = null;
            if (!curBatch.isEmpty())
            {
                newData = curBatch.toArray(new IEvent[curBatch.size()]);
            }
            if ((lastBatch != null) && (!lastBatch.isEmpty()))
            {
                oldData = lastBatch.toArray(new IEvent[lastBatch.size()]);
            }
            
            if ((newData != null) || (oldData != null))
            {
                IDataCollection dataCollection = getDataCollection();
                if (dataCollection != null)
                {
                    dataCollection.update(newData, oldData);
                }
                
                updateChild(newData, oldData);
                
                LOG.debug("The batchdata has been send to childView.");
            }
        }
        
        lastBatch = curBatch;
        curBatch = new ArrayDeque<IEvent>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        LengthBatchWindow renewWindow = new LengthBatchWindow(getKeepLength());
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            IDataCollection renewCollection = dataCollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        
        return renewWindow;
    }
}
