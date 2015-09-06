package dataplatform.cache.redis;

/**
 * Redis运行时异常
 * @author 	fuhuiyuan
 */
public class RedisException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public RedisException(Throwable cause) {
		super(cause);
	}

}
