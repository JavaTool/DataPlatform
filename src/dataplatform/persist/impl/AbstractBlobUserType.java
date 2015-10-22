package dataplatform.persist.impl;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.HibernateException;
import org.hibernate.type.StandardBasicTypes;

import dataplatform.persist.BlobUserType;

public abstract class AbstractBlobUserType implements BlobUserType {
	
	protected static final Log log = LogFactory.getLog(AbstractBlobUserType.class);
	
	/**
	 * 数据库中保存的数据类型
	 */
	private static final int[] SQL_TYPES = {StandardBasicTypes.BINARY.sqlType()};

	@Override
	public Object assemble(Serializable arg0, Object arg1) throws HibernateException {
		return arg0;
	}

	@Override
	public Object nullSafeGet(ResultSet resultSet, String[] names, Object owner) throws HibernateException, SQLException {
		byte[] bytes = resultSet.getBytes(names[0]);
		try {
			return makeObject(bytes, owner);
		} catch (Exception e) {
			throw new HibernateException(e);
		}
	}

	@Override
	public void nullSafeSet(PreparedStatement statement, Object value, int index) throws HibernateException, SQLException {
		byte[] bytes = makeBytes(value);
		if (bytes != null) {
			statement.setBytes(index, bytes);
		} else {
			statement.setNull(index, StandardBasicTypes.BINARY.sqlType());
		}
	}

	@Override
	public int compare(Object o1, Object o2) {
		return 0;
	}

	@Override
	public boolean equals(Object arg0, Object arg1) throws HibernateException {
		if (arg0 == arg1)
			return true;
		if (arg0 == null || arg1 == null)
			return false;
		return arg0.equals(arg1);
	}

	@Override
	public Serializable disassemble(Object arg0) throws HibernateException {
		return (Serializable) arg0;
	}

	@Override
	public int hashCode(Object arg0) throws HibernateException {
		return arg0.hashCode();
	}

	@Override
	public boolean isMutable() {
		return true;
	}

	@Override
	public Object replace(Object arg0, Object arg1, Object arg2) throws HibernateException {
		return arg1;
	}

	@Override
	public int[] sqlTypes() {
		return SQL_TYPES;
	}

}
