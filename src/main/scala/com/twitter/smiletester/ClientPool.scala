package com.twitter.smiletester

import org.apache.commons.pool.PoolableObjectFactory
import net.lag.smile.MemcacheClient
import net.lag.configgy.ConfigMap
import org.apache.commons.pool.impl.GenericObjectPool

class ClientPool(maxActive: Int, config: ConfigMap) {
  private val clientOrPool = new GenericObjectPool(new PoolableMemcacheFactory(config), maxActive)

  def get(key: String): Option[String] = {
    val client = clientOrPool.borrowObject().asInstanceOf[MemcacheClient[String]]
    val result = client.get(key)
    clientOrPool.returnObject(client)
    result
  }

  def set(key: String, value: String) {
    val client = clientOrPool.borrowObject().asInstanceOf[MemcacheClient[String]]
    val result = client.set(key, value, 0, 7200)
    clientOrPool.returnObject(client)
    result
  }
}

class PoolableMemcacheFactory(val config:ConfigMap) extends PoolableObjectFactory {
  override def makeObject() = MemcacheClient.create(config)

  // noops for now
  override def activateObject(obj: Object) {}
  override def destroyObject(obj: Object) {}
  override def passivateObject(obj: Object) {}
  override def validateObject(obj: Object) = true
}
