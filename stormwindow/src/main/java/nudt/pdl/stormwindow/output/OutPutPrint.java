package nudt.pdl.stormwindow.output;

import nudt.pdl.stormwindow.common.Pair;
import nudt.pdl.stormwindow.event.Attribute;
import nudt.pdl.stormwindow.event.IEvent;
import nudt.pdl.stormwindow.event.TupleEventType;

/**
 * 输出测试类，打印到屏幕
 */
public class OutPutPrint implements IOutput
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 8512176298059174174L;
    
    /**
     * 执行输出操作
     * @param object 需要输出的对象
     */
    @SuppressWarnings("unchecked")
    @Override
    public void output(Object object)
    {
        if (null == object)
        {
            return;
        }
        
        Pair<IEvent[], IEvent[]> list = (Pair<IEvent[], IEvent[]>)object;
        
        if (null == list.getFirst())
        {
            return;
        }
        
        for (int i = 0; i < list.getFirst().length; i++)
        {
            
            IEvent tupleEvent = list.getFirst()[i];
            
            TupleEventType type = (TupleEventType)tupleEvent.getEventType();
            Attribute[] att = type.getAllAttributes();
            for (int j = 0; j < att.length; j++)
            {
                System.out.print(att[j].getAttName() + "=" + tupleEvent.getValue(att[j].getAttName()) + "    ");
            }
            System.out.println();
        }
    }
    
}
