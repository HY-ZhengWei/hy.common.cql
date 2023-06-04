package org.hy.common.xcql;





/**
 * XCQL异常处理机制。
 * 
 * 异常发生时，先回滚、关闭数据库连接后，此类中的方法才会被调用。
 * 
 * 常规情况下，XCQL是Java代码是执行的，自然就有Java原生的异常处理机制Exception。
 *           所以在常规情况下，是不需此类出马的。
 * 
 * 应用场景1：XCQL触发器中，每个触发器均能独立处理异常的能力。
 * 应用场景2：XCQL组中，对重点XCQL异常信息的业务层面的特殊处理。常规情况下XCQL组自身也是有异常处理机制的。
 * 应用场景3：对所有XCQL异常信息的统一处理，如记录在日志文件中。
 *          具体实现方法是：在构建XCQL实例前，定义一个xid = XCQL.$XCQLErrors 的XJava对象，并放在XJava对象池中。
 *
 * @author      ZhengWei(HY)
 * @createDate  2023-06-03
 * @version     v1.0
 */
public interface XCQLError
{
    
    /**
     * 异常处理
     * 
     * @author      ZhengWei(HY)
     * @createDate  2019-02-18
     * @version     v1.0
     *
     * @param i_Error   异常信息
     */
    public void errorLog(XCQLErrorInfo i_Error);
    
}
