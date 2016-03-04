package nudt.pdl.stormwindow.group;


/**
 * 
 * 分布式环境中事件分发方式
 * <功能详细描述>
 * 
 */
public enum DistributeType
{
    /**
     * 随机分发
     */
    SHUFFLE("shuffle"),
    /**
     * 汇聚分发，多个源分发事件到一个目标
     */
    GLOBAL("global"),
    /**
     * 按字段分发
     */
    FIELDS("fields"),
    /**
     * 本地分发
     */
    LOCALORSHUFFLE("localorshuffle"),
    /**
     * 发给所有后续节点
     */
    ALL("all"),
    /**
     * 指定分发的目标节点
     */
    DIRECT("direct"),
    /**
     * 用户自定义
     */
    CUSTOM("custom"),
    /**
     * 无，与SHUFFLE类似
     */
    NONE("none");
    
    /**
     * 描述
     */
    private String desc;
    
    /**
     * <默认构造函数>
     * @param desc 描述
     */
    private DistributeType(String desc)
    {
        this.desc = desc;
    }
    
    public String getDesc()
    {
        return desc;
    }
    
    public void setDesc(String desc)
    {
        this.desc = desc;
    }
    
}
