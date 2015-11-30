package dataplatform.coder.string;

public abstract class SimpleStringCoder<E> implements IStringCoder<E> {

	@Override
	public String toString(E e) {
		return e.toString();
	}

}
