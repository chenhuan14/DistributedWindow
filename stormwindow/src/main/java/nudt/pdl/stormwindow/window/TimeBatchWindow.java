package nudt.pdl.stormwindow.window;



import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.timer.TimeService;
import nudt.pdl.stormwindow.view.IDataCollection;



/**
 * <时间跳动窗口实现类>
 * 
 */
public class TimeBatchWindow extends TimeBasedWindow implements IBatch
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -3916243765038851148L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeBatchWindow.class);
    
    /**
     * 窗口中上个批次中事件  
     */
    private ArrayDeque<Tuple> lastBatch = null; //上一个Batch中事件   
    
    /**
     * 窗口中当前批次中事件 
     */
    private ArrayDeque<Tuple> curBatch = new ArrayDeque<Tuple>(); //当前Batch中事件 
    
    /**
     * <默认构造函数>
     *@param keepTime 窗口保持时间
     */
    public TimeBatchWindow(long keepTime)
    {
        super(keepTime);
        TimeService timeservice = new TimeService(keepTime, this);
        setTimeservice(timeservice);
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Tuple[] newData, Tuple[] oldData)
    {
        //TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        
        if ((null == newData) || (newData.length == 0))
        {
            LOG.error("Input Time Batch Window newData is Null!");
            return;
        }
        
        try
        {
            lock();
            for (Tuple event : newData)
            {
                curBatch.add(event);
                //LOG.debug("The newData has been added to TimeBatchWindow,current Batch size: {}", curBatch.size());
            }
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void timerCallBack(long currentTime)
    {
        //TODO 加锁操作对性能的影响，有没有更好的方法？
        try
        {
            lock();
            sendBatchData();
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
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
                IDataCollection dataCollection = getDataCollection();
                if (dataCollection != null)
                {
                    dataCollection.update(newData, oldData);
                }
                
                updateChild(newData, oldData);
                LOG.debug("The batchdata has been send to childView, current batch size: {}, old batch size :{}.",
                    newData != null ? curBatch.size() : 0,
                    oldData != null ? lastBatch.size() : 0);
            }
        }
        
        lastBatch = curBatch;
        curBatch = new ArrayDeque<Tuple>();
    }
}
