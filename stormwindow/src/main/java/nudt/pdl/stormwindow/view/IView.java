package nudt.pdl.stormwindow.view;

import backtype.storm.tuple.Tuple;

/**
 * 
 * <IView接口提供方法，将流或者视图中的新数据和过期数据发送给子视图。>
 * 
 */
public interface IView extends IViewable
{
    /**
     * <返回父视图>
     * @return 父视图
     */
    public IViewable getParent();
    
    /**
     * <设置父视图>
     * @param parent 父视图
     */
    public void setParent(IViewable parent);
    
    /**
     * <将新数据和过期数据发送给子视图>
     * @param newData 新数据
     * @param oldData 过期数据
     */
    void update(Tuple[] newData, Tuple[] oldData);
}
