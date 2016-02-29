package nudt.pdl.stormwindow.operator;


import java.util.List;
import java.util.Map;

import nudt.pdl.stormwindow.event.IEventType;



/**
 * 流处理算子基本接口
 * 所有的流处理相关的算子实现，都来源于这个算子
 * 移除所有Set接口，仅仅保留get接口，以保证属性在运行时不会被改变
 * 所有的外部Storm实现，均依赖于这个接口
 * 
 */
public interface IRichOperator extends IOperator{
    /**
     * 获取算子id
     * @return 算子id
     */
    String getOperatorId();
    
    /**
     * 获取算子并发度
     * @return 算子的并发度
     */
    int getParallelNumber();
    
    /**
     * 获取输入流名称
     * @return 输入流名称
     */
    List<String> getInputStream();
    
    /**
     * 获取输出流名称
     * @return 输出流名称
     */
    String getOutputStream();
    

}
