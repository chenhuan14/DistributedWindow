package nudt.pdl.stormwindow.storm;

import nudt.pdl.stormwindow.operator.IRichOperator;

/**
 * 流处理算子适配接口
 * 依靠这个接口，将流处理的算子注入到具体的Storm算 Bolt中
 * 
 */
public interface StreamAdapter
{
    /**
     * 设置流处理算子
     * @param operator 流处理算子
     */
    void setOperator(IRichOperator operator);
}
