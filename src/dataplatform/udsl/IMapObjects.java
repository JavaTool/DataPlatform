package dataplatform.udsl;

public interface IMapObjects {
	
	<T> String getKey(T entity);
	
	<T> Object getValue(T entity);

}
