package nudt.pdl.stormwindow.operator;

import nudt.pdl.stormwindow.event.IEvent;

public interface IProcessor {
    /**
     * <数据处理>
     * @param newData 新事件
     * @param oldData 旧事件
     */
    public void process(IEvent[] newData, IEvent[] oldData);
}
