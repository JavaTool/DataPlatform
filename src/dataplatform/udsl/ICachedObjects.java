package dataplatform.udsl;

public interface ICachedObjects {
	
	<T> String makeKey(T entity);
	
	<T> String makeField(T entity);

}
