package cn.edu.tsinghua.iginx.transaction.participant;

import cn.edu.tsinghua.iginx.transaction.exception.ParticipantCommitException;
import cn.edu.tsinghua.iginx.transaction.exception.ParticipantConflictException;
import cn.edu.tsinghua.iginx.transaction.exception.ParticipantPrepareException;

public interface Participant {

    /**
     * 对参与者执行 prepare 操作
     * @throws ParticipantPrepareException 发生未预期的错误
     * @throws ParticipantConflictException 与其他事务发生冲突
     */
    void prepare() throws ParticipantPrepareException, ParticipantConflictException;

    /**
     * 对参与者执行 commit 操作
     * @throws ParticipantCommitException 发生未预期的错误
     */
    void commit() throws ParticipantCommitException;

    /**
     * 获取参与者的类型
     * @return 参与者类型
     */
    ParticipantType getType();

    /**
     * 序列化成可解析的字节数组
     * @return 字节数组，表示参与者的类型
     */
    byte[] encode();

}
