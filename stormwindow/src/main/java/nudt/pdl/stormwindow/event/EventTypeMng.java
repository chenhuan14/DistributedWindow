package nudt.pdl.stormwindow.event;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 事件类型管理
 * <功能详细描述>
 * 
 */
public class EventTypeMng implements Serializable
{
    private static final long serialVersionUID = -2112989625254976096L;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(EventTypeMng.class);
    
    /*
     * MAP<数据类型名称, 具体数据类型>
     */
    private Map<String, IEventType> schemas;
    
    /**
     * <默认构造函数>
     *
     */
    public EventTypeMng()
    {
        this.schemas = new HashMap<String, IEventType>();
    }
    
    /**
     * 增加事件类型
     * <功能详细描述>
     * @param schema schema
     * @return
     */
    public void addEventType(IEventType schema)
    {
        if (null == schema)
        {
            throw new RuntimeException("The event type is null.");
        }
        
        if (StringUtils.isEmpty(schema.getEventTypeName()))
        {
            throw new RuntimeException("The event type name is empty.");
        }
        
        LOGGER.info("AddEventType enter, the eventtypeName is:{}.", schema.getEventTypeName());
        
        /**
         * 事件类型名称不得重复
         */
        if (schemas.containsKey(schema.getEventTypeName()))
        {
            throw new RuntimeException("The event type name: " + schema.getEventTypeName() + " is already exist.");
        }
        
        schemas.put(schema.getEventTypeName(), schema);
    }
    
    /**
     * 根据事件类型名称获得具体事件类型
     * <功能详细描述>
     * @param name 事件类型名称
     * @return 事件类型
     */
    public IEventType getEventType(String name)
    {
        if (StringUtils.isEmpty(name))
        {
            LOGGER.error("The schema name is null.");
            return null;
        }
        
        if (!schemas.containsKey(name))
        {
            LOGGER.error("The schema name {} is not exist.", name);
            return null;
        }
        
        return schemas.get(name);
    }
    
    /**
     * 清除所有事件类型
     * <功能详细描述>
     */
    public void clear()
    {
        this.schemas.clear();
    }
    
}
