package nudt.pdl.stormwindow.window;



import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.exception.StreamingRuntimeException;



/**
 * 保存事件内部结构，记录事件过期时间，方便定时器回调时判断哪些事件过期。
 * 
 */
@SuppressWarnings("rawtypes")
public class TimeSlideEventList implements Iterable, Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 4403904415494890627L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeSlideEventList.class);
    
    /**
     * 容器错误信息
     */
    private static final String CONTAINER_ERROR_MSG = "TimeEventPair Container Type is Error";
    
    /**
     * 
     * <内部类，{时间，事件容器}对>
     */
    static class TimeEventPair implements Serializable
    {
        /**
         * 序列化id
         */
        private static final long serialVersionUID = -902895049822670441L;
        
        /**
         * 时间戳
         */
        private long timestamp;
        
        /**
         * 事件容器
         */
        private Object container;
        
        TimeEventPair(long timestamp, Object container)
        {
            this.timestamp = timestamp;
            this.container = container;
        }
    }
    
    private ArrayDeque<TimeEventPair> datas = null;
    
    private Map<Tuple, TimeEventPair> reverseIndex = null;
    
    /**
     * 默认构造函数
     *
     */
    public TimeSlideEventList()
    {
        this.datas = new ArrayDeque<TimeEventPair>();
    }
    
    /**
     * 默认构造函数
     *@param isSupportRemove 是否支持根据事件删除
     */
    public TimeSlideEventList(boolean isSupportRemove)
    {
        this.datas = new ArrayDeque<TimeEventPair>();
        if (isSupportRemove)
        {
            reverseIndex = new HashMap<Tuple, TimeEventPair>();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<TimeEventPair> iterator()
    {
        if (null == datas)
        {
            return null;
        }
        
        return datas.iterator();
    }
    
    /**
     * 加入事件
     * @param timestamp 时间
     * @param theEvent 事件
     */
    public void add(final long timestamp, final Tuple theEvent)
    {
        //为空
        if (datas.isEmpty())
        {
            TimeEventPair pair = new TimeEventPair(timestamp, theEvent);
            datas.addLast(pair);
            
            if (reverseIndex != null)
            {
                reverseIndex.put(theEvent, pair);
            }
            
            return;
        }
        
        /*
         * 有数据，判断队列最后的时间是否等于当前插入事件时间。
         * 如果等于则更新队列中该时间的事件容器内容，将新事件加入容器中。
         */
        TimeEventPair last = datas.getLast();
        if (last.timestamp == timestamp)
        {
            if (last.container instanceof Tuple)
            {
                List<Tuple> list = new ArrayList<Tuple>();
                list.add((Tuple)last.container);
                list.add(theEvent);
                last.container = list;
            }
            else if (last.container instanceof List)
            {
                @SuppressWarnings("unchecked")
                List<Tuple> list = (List<Tuple>)last.container;
                list.add(theEvent);
            }
            else
            {
                LOG.error(CONTAINER_ERROR_MSG);
                throw new RuntimeException(CONTAINER_ERROR_MSG);
            }
            
            if (reverseIndex != null)
            {
                reverseIndex.put(theEvent, last);
            }
            
            return;
        }
        
        //如果不等于，则当前对为全新元素插入队列中
        TimeEventPair pair = new TimeEventPair(timestamp, theEvent);
        if (reverseIndex != null)
        {
            reverseIndex.put(theEvent, pair);
        }
        datas.addLast(pair);
    }
    
    /**
     * 删除事件
     * @param theEvent 待删除事件
     */
    @SuppressWarnings("unchecked")
    public final void remove(final Tuple theEvent)
    {
        if (reverseIndex == null)
        {
            String str = "Time window does not accept event removal";
            LOG.error(str);
            throw new StreamingRuntimeException(str);
        }
        TimeEventPair pair = reverseIndex.get(theEvent);
        
        if (pair == null)
        {
            return;
        }
        
        if (pair.container != null && pair.container.equals(theEvent))
        {
            datas.remove(pair);
        }
        else if (pair.container != null)
        {
            List<Tuple> list = (List<Tuple>)pair.container;
            list.remove(theEvent);
        }
        else
        {
            LOG.error(CONTAINER_ERROR_MSG);
            throw new RuntimeException(CONTAINER_ERROR_MSG);
        }
        
        reverseIndex.remove(theEvent);
    }
    
    /**
     * 根据时间获取过期事件
     * @param currentTime 时间
     * @return 过期事件
     */
    @SuppressWarnings("unchecked")
    public Tuple[] getOldData(final long currentTime)
    {
        //为空
        if (datas.isEmpty())
        {
            return null;
        }
        
        //事件最老过期时间大于当前时间
        TimeEventPair pair = datas.getFirst();
        if (pair.timestamp >= currentTime)
        {
            return null;
        }
        
        //事件过期事件小于当前时间，则加入到OldData数组中
        List<Tuple> oldData = new ArrayList<Tuple>();
        while (pair.timestamp < currentTime)
        {
            if (pair.container instanceof Tuple)
            {
                oldData.add((Tuple)pair.container);
            }
            else if (pair.container instanceof List)
            {
                oldData.addAll((List<Tuple>)pair.container);
            }
            else
            {
                LOG.error(CONTAINER_ERROR_MSG);
                throw new RuntimeException(CONTAINER_ERROR_MSG);
            }
            
            datas.removeFirst();
            
            if (datas.isEmpty())
            {
                break;
            }
            
            pair = datas.getFirst();
        }
        
        if (!oldData.isEmpty())
        {
            if (reverseIndex != null)
            {
                for (Tuple theEvent : oldData)
                {
                    reverseIndex.remove(theEvent);
                }
            }
            
            return oldData.toArray(new Tuple[oldData.size()]);
        }
        
        return null;
    }
    
}
