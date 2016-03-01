package nudt.pdl.stormwindow.operator;

import backtype.storm.tuple.Tuple;

public interface IProcessor {
    /**
     * <数据处理>
     * @param newData 新事件
     * @param oldData 旧事件
     */
    public void process(Tuple[] newData, Tuple[] oldData);
}
