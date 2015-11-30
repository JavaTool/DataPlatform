package dataplatform.util;

public final class ClassUtil {
	
	private ClassUtil() {}
	
	public static <T> boolean isNumberArray(T[] array) {
		if (array instanceof Byte[] || array.getClass().equals(byte[].class)) {
			return true;
		} else if (array instanceof Short[] || array.getClass().equals(short[].class)) {
			return true;
		} else if (array instanceof Integer[] || array.getClass().equals(int[].class)) {
			return true;
		} else if (array instanceof Long[] || array.getClass().equals(long[].class)) {
			return true;
		} else if (array instanceof Double[] || array.getClass().equals(double[].class)) {
			return true;
		} else if (array instanceof Float[] || array.getClass().equals(float[].class)) {
			return true;
		}
		return false;
	}

}
