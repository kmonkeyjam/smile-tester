package com.twitter.smiletester

import org.apache.commons.pool.PoolableObjectFactory
import net.lag.configgy.ConfigMap
import org.apache.commons.pool.impl.GenericObjectPool
import net.lag.smile._
import collection.jcl.ArrayList
import com.meetup.memcached.{MemcachedClient, SockIOPool}
import collection.mutable.ListBuffer
import java.lang.Integer
import com.twitter.ostrich.Stats

trait MemcacheWrapper {
  def get(key: String): Option[Array[Byte]]
  def set(key: String, value: Array[Byte], flags:Int, expire: Int)
  def delete(key: String)
}

object MemcachePool {
  def apply(config: ConfigMap): MemcachePool = {
    val maxActive = try {
      config.configMap("pool")("maxActive").toInt
    } catch {
      case _ => 1
    }
    new MemcachePool(maxActive, config)
  }

  object MemcacheClient {
    def apply(config: ConfigMap): MemcacheClient[Array[Byte]] = {
      val pool = ServerPool.fromConfig(config)
      val locator = NodeLocator.byName(config("distribution", "default")) match {
        case (hashName, factory) => factory(KeyHasher.byName(config("hash", hashName)))
      }
      val client = new MemcacheClient(locator, NoopCodec)
      client.setPool(pool)
      client.namespace = config.getString("namespace")
      client
    }
  }
  object NoopCodec extends MemcacheCodec[Array[Byte]] {
    def encode(value: Array[Byte]): Array[Byte] = value
    def decode(data: Array[Byte]): Array[Byte] = data
  }
}

class MemcachePool(maxActive: Int, config: ConfigMap) {
  private val client: MemcacheWrapper =
    config.getString("client_type", "smile_pool") match {
      case "smile_pool" => new PoolWrapper(maxActive, config)
      case "smile_conn_pool" => new ClientBalancer(config)
      case "non_smile" => {
        NonSmileMemcacheClient(config)
        new NonSmileMemcacheClient(config)
      }
    }

  def get(key: String): Option[Array[Byte]] = client.get(key)
  def set(key: String, value: Array[Byte], flags:Int, expire: Int) = client.set(key, value, flags, expire)
  def delete(key: String) = client.delete(key)
  override def toString() = "MemcachePool(%s): %s".format(maxActive, client)
}

class PoolWrapper(maxActive: Int, config: ConfigMap) extends Object with MemcacheWrapper {
  config.setInt("num_connection_in_pool", 1)

  val pool = new GenericObjectPool(new PoolableMemcacheFactory(config), maxActive)
  pool.setMaxIdle(maxActive)

  def get(key: String): Option[Array[Byte]] = {
    execute { _.get(key) }
  }

  def set(key: String, value: Array[Byte], flags:Int, expire: Int) {
    execute { _.set(key, value, flags, expire) }
  }

  def delete(key: String) {
    execute { _.delete(key) }
  }

  def execute[T](f: MemcacheClient[Array[Byte]] => T): T = {
    val memcache = pool.borrowObject().asInstanceOf[MemcacheClient[Array[Byte]]]
    val result = f(memcache)
    pool.returnObject(memcache)
    result
  }
}

class PoolableMemcacheFactory(val config:ConfigMap) extends PoolableObjectFactory {
  override def makeObject() = MemcachePool.MemcacheClient(config)

  // noops for now
  override def activateObject(obj: Object) {}
  override def destroyObject(obj: Object) {}
  override def passivateObject(obj: Object) {}
  override def validateObject(obj: Object) = true
}

class ClientBalancer(config: ConfigMap) extends Object with MemcacheWrapper{
  val numClients = config.getInt("num_clients_to_balance", 10)
  val clientPool = new ArrayList[MemcacheClient[Array[Byte]]]
  val random = new Random

  for (i <- 1 to numClients) {
    clientPool += MemcachePool.MemcacheClient(config)
  }

  def get(key: String): Option[Array[Byte]] = {
    getClient.get(key)
  }

  def set(key: String, value: Array[Byte], flags:Int, expire: Int) {
    getClient.set(key, value, flags, expire)
  }

  def delete(key: String) {
    getClient.delete(key)
  }

  def getClient: MemcacheClient[Array[Byte]] = {
    clientPool(random.nextInt(numClients))
  }
}

object NonSmileMemcacheClient {
  val client = new MemcachedClient();

  def apply(config: ConfigMap) {
    val servers = config.getList("servers")
    val weights = new ListBuffer[java.lang.Integer]
    for (i <- 1 to servers.size) weights += 1

    val pool = SockIOPool.getInstance();
    pool.setServers( servers.toArray );
    pool.setWeights( weights.toArray[java.lang.Integer] );

    pool.setInitConn( 5 );
    pool.setMinConn( 5 );
    pool.setMaxConn( config.getInt("other_memcache_max_conn", 250) );
    pool.setMaxIdle( 1000 * 60 * 60 * 6 );

    pool.setMaintSleep( 30 );

    pool.setNagle( false );
    pool.setSocketTO( 3000 );
    pool.setSocketConnectTO( 0 );

    pool.initialize();
  }
}

class NonSmileMemcacheClient(config: ConfigMap) extends Object with MemcacheWrapper {
  def get(key: String): Option[Array[Byte]] = {
    getClient.get(key) match {
      case value: Object => {
        Some(value.asInstanceOf[String].getBytes)
      }
      case null => None
    }
  }

  def set(key: String, value: Array[Byte], flags:Int, expire: Int) {
    getClient.set(key, new String(value))
  }

  def delete(key: String) {
    getClient.delete(key)
  }

  def getClient: MemcachedClient = {
    NonSmileMemcacheClient.client
  }
}