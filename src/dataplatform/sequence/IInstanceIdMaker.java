package dataplatform.sequence;

/**
 * id生成器
 * @author	fuhuiyuan
 */
public interface IInstanceIdMaker {
	
	/**
	 * 生成新的id
	 * @return	id
	 */
	int nextInstanceId();

}
