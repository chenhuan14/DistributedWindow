package nudt.pdl.stormwindow.event;



import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * 数据流schema中的信息：属性数据类型：属性名称
 * <功能详细描述>
 * 
 */
public class Attribute implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 8426637664102777131L;
    
    /**
     * 属性数据类型
     */
    private Class< ? > attDataType;
    
    /**
     * 属性名称
     */
    private String attName;
    
    /**
     * <默认构造函数>:不允许有空的属性名称
     *@param dt 属性数据类型
     *@param name 属性名称
     */
    public Attribute(Class< ? > dt, String name)
    {
        if (StringUtils.isEmpty(name))
        {
            throw new RuntimeException("The attribute name is empty.");
        }
        if (null == dt)
        {
            throw new RuntimeException("The Class of attribute is null. attribute name=" + name);
        }
        this.attDataType = dt;
        this.attName = name;
    }
    
    /**
     * 获得属性数据类型
     * <功能详细描述>
     * @return 属性数据类型
     */
    public Class< ? > getAttDataType()
    {
        return attDataType;
    }
    
    /**
     * 获得属性名称
     * <功能详细描述>
     * @return 属性名称
     */
    public String getAttName()
    {
        return attName;
    }
    
    /**
     * 设置属性数据类型
     * <功能详细描述>
     * @param attDataType 属性数据类型
     */
    protected void setAttDataType(Class< ? > attDataType)
    {
        this.attDataType = attDataType;
    }
    
    protected void setAttName(String attName)
    {
        this.attName = attName;
    }
    
}
