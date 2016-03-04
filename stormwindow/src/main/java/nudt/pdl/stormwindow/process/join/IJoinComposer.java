package nudt.pdl.stormwindow.process.join;



import java.io.Serializable;
import java.util.Set;

import backtype.storm.tuple.Tuple;
import nudt.pdl.stormwindow.common.MultiKey;
import nudt.pdl.stormwindow.common.Pair;




/**
 * 
 * 数据流关联
 * <功能详细描述>
 * 
 */
public interface IJoinComposer extends Serializable
{
    /**
     * 保存有效窗口数据
     * <功能详细描述>
     * @param newDataPerStream 每个流的新事件
     * @param oldDataPerStream 每个流的旧事件
     */
    public void maintainData(Tuple[][] newDataPerStream, Tuple[][] oldDataPerStream);
    
    /**
     * 针对输入新旧事件，进行窗口有效事件的JOIN操作
     * <功能详细描述>
     * @param newDataPerStream 每个流的新事件
     * @param oldDataPerStream 每个流的旧事件
     * @return JOIN后的结果
     */
    public Pair<Set<MultiKey>, Set<MultiKey>> join(Tuple[][] newDataPerStream, Tuple[][] oldDataPerStream);
    
    /**
     * 获得JOIN的流数量
     * <功能详细描述>
     * @return JOIN的流数量
     */
    public int getStreamsSize();
}
