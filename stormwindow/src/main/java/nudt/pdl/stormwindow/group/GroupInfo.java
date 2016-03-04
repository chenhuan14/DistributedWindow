package nudt.pdl.stormwindow.group;



import java.io.Serializable;
import java.util.List;

/**
 * 
 * 数据分发相关信息
 * <功能详细描述>
 * 
 */
public class GroupInfo implements Serializable
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 4066114313620144701L;
    
    private String streamName;
    
    private DistributeType ditributeType;
    
    private List<String> fields;
    
    //    private CustomStreamGrouping grouping;
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
    
    public DistributeType getDitributeType()
    {
        return ditributeType;
    }
    
    public void setDitributeType(DistributeType ditributeType)
    {
        this.ditributeType = ditributeType;
    }
    
    public List<String> getFields()
    {
        return fields;
    }
    
    public void setFields(List<String> fields)
    {
        this.fields = fields;
    }
    
    /**
     * 尚不考虑用户自定义分发方式
     */
    /*
    public CustomStreamGrouping getGrouping()
    {
        return grouping;
    }

    public void setGrouping(CustomStreamGrouping grouping)
    {
        this.grouping = grouping;
    }
    */
    
}
