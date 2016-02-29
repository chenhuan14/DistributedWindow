package nudt.pdl.stormwindow.output;



/**
 * Storm输出类
 */
import java.io.Serializable;

import nudt.pdl.stormwindow.common.Pair;
import nudt.pdl.stormwindow.event.IEvent;
import nudt.pdl.stormwindow.exception.StreamingException;
import nudt.pdl.stormwindow.storm.IEmitter;


/**
 * 在Storm平台上将处理后的数据发送给下一个bolt
 */
public class OutputStorm implements IOutput, Serializable
{
    
    /**
     * Automatically generate a serial version ID
     */
    private static final long serialVersionUID = -5877349723893025482L;
    
    private IEmitter emitter;
    
    /**
     * 输出类型
     */
    private  OutputType outType = OutputType.I;
    
    /**
     * 构造函数
     * 按照指定类型进行输出
     */
    public OutputStorm(OutputType outputType)
    {
        this.outType = outputType;
    }

    /**
     * 构造函数
     * 默认输出I流
     */
    public OutputStorm()
    {
        this.outType = OutputType.I;
    }


    /** 
     * {@inheritDoc} 
     */
    @SuppressWarnings("unchecked")
    public void output(Object object)
    {
        if (null == object)
        {
            return;
        }
        
        Pair<IEvent[], IEvent[]> inputPair = (Pair<IEvent[], IEvent[]>)object;

        try
        { 
            switch (outType)
            {
                case I:
                    emitIStream(inputPair);
                    break;
                case R:
                    emitRStream(inputPair);
                    break;
                case IR:
                    emitIStream(inputPair);
                    emitRStream(inputPair);
                    break;
                default:
                    break;
            }
        }
        catch (StreamingException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void emitIStream(Pair<IEvent[], IEvent[]> inputPair) throws StreamingException
    {
        IEvent[] iEvents = inputPair.getFirst();
        if (iEvents == null)
        {
            return;
        }
        
        for (int i = 0; i < iEvents.length; i++)
        {
            IEvent tupleEvent = iEvents[i];
            if (tupleEvent != null)
            {
                emitter.emit(tupleEvent.getAllValues());
            }
        }
    }
    
    private void emitRStream(Pair<IEvent[], IEvent[]> inputPair) throws StreamingException
    {
        IEvent[] rEvents = inputPair.getSecond();
        if (rEvents == null)
        {
            return;
        }
        
        for (int i = 0; i < rEvents.length; i++)
        {
            IEvent tupleEvent = rEvents[i];
            if (tupleEvent != null)
            {
                emitter.emit(tupleEvent.getAllValues());
            }
        }
    }
    
    public void setEmit(IEmitter emit)
    {
        this.emitter = emit;
    }
    
}
