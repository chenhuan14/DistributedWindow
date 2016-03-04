package nudt.pdl.stormwindow.process.join;



import java.io.Serializable;
import java.util.Set;

import backtype.storm.tuple.Tuple;




/**
 * 
 * 事件容器
 * <功能详细描述>
 * 
 */
public interface IEventCollection extends Serializable
{
    /**
     * Add and remove events from collection.
     * <p>
     *     It is up to the implement to decide whether to add first and then remove,
     *     or whether to remove and then add.
     * </p>
     * <p>
     *     It is important to note that a given event can be in both the
     *     removed and the added events. This means that unique indexes probably need to remove first
     *     and then add. Most other non-unique indexes will add first and then remove
     *     since the an event can be both in the add and the remove stream.
     * </p>
     * @param newData to add
     * @param oldData to remove
     */
    void addRemove(Tuple[] newData, Tuple[] oldData);
    
    /**
     * Add events to collection.
     * @param events to add
     */
    public void add(Tuple[] events);
    
    /**
     * Remove events from collection.
     * @param events to remove
     */
    public void remove(Tuple[] events);
    
    /**
     * Returns true if the index is empty, or false if not
     * @return true for empty index
     */
    public boolean isEmpty();
    
    /**
     * Clear out index.
     */
    public void clear();
    
    /**
     * 获得保存事件的流名称
     * <功能详细描述>
     * @return 流名称 
     */
    public String getStreamName();
    

    
    public Set<Tuple> getAllEvents();
}
