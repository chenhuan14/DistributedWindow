package nudt.pdl.stormwindow.storm;

import nudt.pdl.stormwindow.exception.StreamingException;

public interface IEmitter
{
    /**
     * emit 发送数据
     * @param datas 数据数组，整条数据里面所有的列为一个数组
     * @throws StreamingException 流处理异常
     */
    void emit(Object[] datas)
        throws StreamingException;
    
    void emit(Object data)
    	throws StreamingException;
}
