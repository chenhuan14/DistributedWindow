package nudt.pdl.stormwindow.view;


import java.io.Serializable;

import backtype.storm.tuple.Tuple;



/**
 * <窗口事件集合>
 * <窗口通过接口将新事件和旧事件发送过来，集合中增加新事件，删除旧事件。>
 * 
 */
public interface IDataCollection extends Serializable
{
    /**
     * <接受窗口传递的新事件和旧事件>
     * <接受窗口传递的新事件和旧事件， 集合中增加新事件，删除旧事件。>
     * @param newData 新事件
     * @param oldData 旧事件
     */
    public void update(Tuple[] newData, Tuple[] oldData);
    
    /**
     * <根据已有缓存集创建新缓存集>
     * <根据已有缓存集创建新缓存集>
     * @return 新缓存集
     */
    public IDataCollection renew();
}
