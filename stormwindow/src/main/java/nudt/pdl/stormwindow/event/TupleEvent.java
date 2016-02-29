package nudt.pdl.stormwindow.event;



import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nudt.pdl.stormwindow.exception.StreamingRuntimeException;


/**
 * 
 * 内部事件定义
 * <功能详细描述>
 * 
 */
public class TupleEvent implements IEvent
{
    private static final Logger LOG = LoggerFactory.getLogger(TupleEvent.class);
    
    private static final long serialVersionUID = -1036661342270395558L;
    
    private IEventType eventType;
    
    private Object[] tuple;
    
    private String appName;
    
    private String streamName;
    
    /**
     * 特殊事件标识，该标识用于窗口的分割，根据事件分割
     */
    private boolean isFlagEvent = false;
    
    /**
     * <默认构造函数>
     *@param name 流名称
     *@param eTypeName 事件类型名称
     *@param values 事件值
     *@param eventTypeManager 事件类型管理器
     */
    public TupleEvent(String name, String eTypeName, Map<String, Object> values, EventTypeMng eventTypeManager)
    {
        if (StringUtils.isEmpty(name))
        {
            LOG.error("Stream name is null.");
            throw new RuntimeException("Stream name is null.");
        }
        
        TupleEventType type = (TupleEventType)eventTypeManager.getEventType(eTypeName);
        if (type == null)
        {
            LOG.error("Schema is undefined, name:{}.", eTypeName);
            throw new RuntimeException("Schema is undefined, name:" + eTypeName);
        }
        
        if (values == null)
        {
            LOG.error("Event values is null.");
            throw new RuntimeException("Event values is null.");
        }
        
        init(name, type, values);
    }
    
    /**
     * <默认构造函数>
     *@param streamName 流名称
     *@param eventType 事件类型
     *@param values 事件值
     */
    public TupleEvent(String streamName, IEventType eventType, Map<String, Object> values)
    {
        if (StringUtils.isEmpty(streamName))
        {
            LOG.error("Stream name is null.");
            throw new RuntimeException("Stream name is null.");
        }
        
        if (eventType == null)
        {
            LOG.error("Event type is null.");
            throw new RuntimeException("Event type is null.");
        }
        
        if (values == null)
        {
            LOG.error("Event values is null.");
            throw new RuntimeException("Event values is null.");
        }
        
        init(streamName, eventType, values);
    }
    
    /**
     * <默认构造函数>
     *@param streamName 流名称
     *@param eventType 事件类型
     *@param values 事件值
     *@throws StreamSerDeException 
     */
    public TupleEvent(String streamName, IEventType eventType, Object[] values)
    {
        if (StringUtils.isEmpty(streamName))
        {
            LOG.error("Stream name is null.");
            throw new RuntimeException("Stream name is null.");
        }
        this.streamName = streamName;
        
        if (eventType == null)
        {
            LOG.error("Event type is null.");
            throw new RuntimeException("Event type is null.");
        }
        
        if (values == null)
        {
            LOG.error("Event values is null.");
            throw new RuntimeException("Event values is null.");
        }
        
        String[] att = eventType.getAllAttributeNames();
        
        if (values.length != att.length)
        {
            LOG.error("The tuple size and it's EventType is not match. EventType:{}.", eventType);
            throw new RuntimeException("The tuple size and it's EventType is not match. EventType:" + eventType);
        }
        
        this.tuple = new Object[att.length];
        this.eventType = eventType;
        for (int i = 0; i < att.length; i++)
        {
            tuple[i] = values[i];
        }
        
    }
    
    /**
     * <默认构造函数>
     */
    public TupleEvent()
    {
    }
    
    private void init(String name, IEventType eType, Map<String, Object> values)
    {
        this.streamName = name;
        this.tuple = new Object[values.size()];
        this.eventType = eType;
        String[] att = eType.getAllAttributeNames();
        
        if (values.size() != att.length)
        {
            LOG.error("The tuple size and it's EventType is not match. EventType:{}.", eType.getEventTypeName());
            throw new RuntimeException("The tuple size and it's EventType is not match. EventType:"
                + eType.getEventTypeName());
        }
        for (int i = 0; i < att.length; i++)
        {
            tuple[i] = values.get(att[i]);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue(String propertyName)
    {
        int index = eventType.getAttributeIndex(propertyName);
        if (index == -1)
        {
            LOG.error("Can not find index by property {}.", propertyName);
            throw new StreamingRuntimeException("Can not find index by property " + propertyName);
        }
        return tuple[index];
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int getIndexByPropertyName(String propertyName)
    {
        int index = eventType.getAttributeIndex(propertyName);
        if (index == -1)
        {
            LOG.error("Can not find index by property {}.", propertyName);
            throw new StreamingRuntimeException("Can not find index by property " + propertyName);
        }
        return index;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IEventType getEventType()
    {
        return eventType;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue(int index)
    {
        if (index < 0 || index >= tuple.length)
        {
            LOG.error("Invalid index:{}", index);
            throw new RuntimeException("Invalid index:" + index);
        }
        return tuple[index];
    }
    
    public void setAppName(String app)
    {
        this.appName = app;
    }
    
    public String getAppName()
    {
        return this.appName;
    }
    
    @Override
    public Object[] getAllValues()
    {
        return tuple;
    }
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
    
    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuilder event = new StringBuilder();
        event.append("appName=").append(appName);
        event.append(", streamName=").append(streamName);
        String[] attnames = this.eventType.getAllAttributeNames();
        Object[] values = this.getAllValues();
        
        if (values != null)
        {
            for (int i = 0; i < attnames.length; i++)
            {
                event.append(", ")
                    .append(attnames[i])
                    .append("=")
                    .append(values[i] == null ? "" : values[i].toString());
            }
        }
        
        return event.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setFlagEvent()
    {
        this.isFlagEvent = true;
    }
    
    @Override
    public boolean isFlagEvent()
    {
        return isFlagEvent;
    }
    
}
