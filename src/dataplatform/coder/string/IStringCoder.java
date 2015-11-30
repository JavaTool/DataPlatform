package dataplatform.coder.string;

public interface IStringCoder<E> {
	
	E parse(String str);
	
	String toString(E e);

}
