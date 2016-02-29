package nudt.pdl.stormwindow.window;



import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nudt.pdl.stormwindow.event.IEvent;
import nudt.pdl.stormwindow.view.IDataCollection;
import nudt.pdl.stormwindow.view.IView;



/**
 * <长度滑动窗口实现类>
 * 
 */
public class LengthSlideWindow extends LengthBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 5709468267900902359L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(LengthSlideWindow.class);
    
    /**
     * 窗口事件集合，先入先出。
     */
    private final ArrayDeque<IEvent> events = new ArrayDeque<IEvent>();
    
    /**
     * <默认构造函数>
     *@param keepLength 窗口保持长度
     */
    public LengthSlideWindow(long keepLength)
    {
        super(keepLength);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        //TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        //将新事件加入到窗口中
        if (null == newData || 0 == newData.length)
        {
            LOG.error("Input Length Batch Window newData is Null!");
            return;
        }
        
        for (IEvent event : newData)
        {
            events.add(event);
        }
        //LOG.debug("The newData has been added to LengthSlideWindow,current Size is :{}", events.size());
        
        /**
         * 判断窗口中事件是否大于窗口保持事件长度，
         * 将多余的事件标记为过期时间，移出窗口。
         */
        int expireCount = (int) (events.size() - getKeepLength());
        IEvent[] expireData = null;
        if (expireCount > 0)
        {
            expireData = new IEvent[expireCount];
            for (int i = 0; i < expireCount; i++)
            {
                expireData[i] = events.removeFirst();
            }
        }
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            dataCollection.update(newData, expireData);
        }
        
        if (this.hasViews())
        {
            updateChild(newData, expireData);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        LengthSlideWindow renewWindow = new LengthSlideWindow(getKeepLength());
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            IDataCollection renewCollection = dataCollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        
        return renewWindow;
    }
}
