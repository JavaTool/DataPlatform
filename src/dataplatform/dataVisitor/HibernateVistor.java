package dataplatform.dataVisitor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;

import com.google.common.collect.Maps;

import dataplatform.persist.DataAccessException;

public final class HibernateVistor implements IDataVisitor {
	
	private Configuration conf;
	
	private SessionFactory sessionFactory;
	
	private Map<String, EntityPersister> entityPersisters;
	
	private Map<String, Lock> locks;
	
	public HibernateVistor(Configuration conf) {
		this.conf = conf;
		initHibernate();
		initLocks();
	}
	
	@SuppressWarnings("rawtypes")
	private void initHibernate() {
		if (conf == null) {
			conf = new Configuration();
			conf.configure();
		}
		sessionFactory = conf.buildSessionFactory();
		entityPersisters = Maps.newHashMap();
		Iterator ite = sessionFactory.getAllClassMetadata().entrySet().iterator(); 
		while (ite.hasNext()) {
			Map.Entry entry = (Map.Entry) ite.next();
			String entityName = (String) entry.getKey();
			EntityPersister entityPersister = (EntityPersister) entry.getValue();
			entityPersisters.put(entityName, entityPersister);
		}
	}
	
	private void initLocks() {
		locks = Maps.newHashMap();
		Iterator<String> ite = entityPersisters.keySet().iterator();
		while (ite.hasNext()) {
			String entityName = ite.next();
			locks.put(entityName, new ReentrantLock());
		}
	}
	
	private Lock getLock(String name) {
		return locks.get(name);
	}
	
	private Session getSession() {
		return sessionFactory.getCurrentSession();
	}
	
	private void checkCreateObject(Object o) {
		if (o instanceof HibernateProxy) {
			throw new IllegalArgumentException();
		}
	}
	
	private String makeHql(String entityName, Map<String, Object> conditions) {
		StringBuilder builder = new StringBuilder("from ");
		builder.append(entityName);
		if (conditions != null) {
			
		}
		return builder.toString();
	}
	
	private <T> String getEntityName(Class<T> clz, VisitorType visitorType) {
		return clz.getName();
	}
	
	private Query createQuery(String entityName, Map<String, Object> conditions) {
		Query query = getSession().createQuery(makeHql(entityName, conditions));
		if (conditions != null) {
			conditions.forEach((key, value) -> {query.setParameter(key, value);});
		}
		return query;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		String entityName = getEntityName(clz, visitorType);
		Lock lock = getLock(entityName);
		lock.lock();
		try {
			Session session = getSession();
			Transaction tx = session.beginTransaction();
			try {
				Query query = createQuery(entityName, conditions);
				Object entity = query.uniqueResult();
				session.clear();
				tx.commit();
				return (T) entity;
			} catch (Exception e) {
				tx.rollback();
				throw new DataAccessException(e);
			}
		} catch(DataAccessException e) {
			throw e;
		} catch(Exception e) {
			throw new DataAccessException(e); 
		} finally {
			lock.unlock();
		}
	}

	@Override
	public <T> void save(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		checkCreateObject(entity);
		String entityName = getEntityName(entity.getClass(), visitorType);
		Lock lock = getLock(entityName);
		lock.lock();
		try {
			Session session = getSession();
			Transaction tx = session.beginTransaction();
			try {
				session.update(entity);
				tx.commit();
			} catch (Exception e) {
				tx.rollback();
				throw new DataAccessException(e);
			}
		} catch(DataAccessException e) {
			throw e;
		} catch(Exception e) {
			throw new DataAccessException(e);
		} finally{
			lock.unlock();
		}
	}

	@Override
	public <T> void delete(T entity, VisitorType visitorType, Map<String, Object> conditions) {
		String entityName = getEntityName(entity.getClass(), visitorType);
		Session session = getSession();
		Transaction tx = session.beginTransaction();
		try {
			Query query = createQuery(entityName, conditions);
			query.executeUpdate();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			throw new DataAccessException(e);
		}
	}

	@Override
	public <T> List<T> getList(Class<T> clz, VisitorType visitorType, Map<String, Object> conditions) {
		String entityName = getEntityName(clz, visitorType);
		Lock lock = getLock(entityName);
		lock.lock();
		try{
			Session session = getSession();
			Transaction tx = session.beginTransaction();
			try {
				Query query = createQuery(entityName, conditions);
				@SuppressWarnings("unchecked")
				List<T> list = query.list();
				session.clear();
				tx.commit();
				return list;
			} catch (Exception e) {
				tx.rollback();
				throw new DataAccessException(e);
			}
		} catch(DataAccessException e) {
			throw e;
		} catch(Exception e) {
			throw new DataAccessException(e); 
		} finally {
			lock.unlock();
		}
	}

	@Override
	public <T> void save(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		for (T en : entity) {
			if (en != null) {
				save(en, visitorType, conditions);
			}
		}
	}

	@Override
	public <T> void delete(T[] entity, VisitorType visitorType, Map<String, Object> conditions) {
		for (T en : entity) {
			if (en != null) {
				delete(en, visitorType, conditions);
			}
		}
	}

}
