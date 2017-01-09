package com.sung.zk.ui.server.zookeeper.zk;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.zkclient.exception.ZkInterruptedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public interface IZkClient {

	int DEFAULT_CONNECTION_TIMEOUT = 10000;

	int DEFAULT_SESSION_TIMEOUT = 30000;

	/**
	 * @Title: close
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:42:58
	 * @Description: Close the client.
	 * @param：
	 * @return：void返回类型
	 * @throws
	 */
	void close();

	/**
	 * @Title: connect
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:43:24
	 * @Description: (这里用一句话描述这个方法的作用)
	 * @param：@param timeout 超时时间
	 * @param：@param watcher
	 * @return：void返回类型
	 * @throws
	 */
	void connect(final long timeout, Watcher watcher);

	/**
	 * @Title: countChildren
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:44:26
	 * @Description: 计算给定节点名称 子节点的个数
	 * @param：@param path
	 * @param：@return
	 * @return：int返回类型
	 * @throws
	 */
	int countChildren(String path);

	/**
	 * @Title: create
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:44:56
	 * @Description: 创建一个节点
	 * @param：@param path
	 * @param：@param data
	 * @param：@param mode
	 * @param：@return
	 * @return：String返回类型
	 * @throws
	 */
	String create(final String path, byte[] data, final CreateMode createMode);

	/**
	 * @Title: createEphemeral
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:45:35
	 * @Description: 创建一个 CreateMode EPHEMERAL 节点 empty data
	 * @param：@param path
	 * @return：void返回类型
	 * @throws
	 */
	void createEphemeral(final String path);

	/**
	 * @Title: createEphemeral
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:47:40
	 * @Description: 创建一个 CreateMode EPHEMERAL 节点
	 * @param：@param path
	 * @param：@param data
	 * @return：void返回类型
	 * @throws
	 */
	void createEphemeral(final String path, final byte[] data);

	/**
	 * @Title: createEphemeralSequential
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:48:29
	 * @Description: Create CreateMode is EPHEMERAL_SEQUENTIAL node
	 * @param：@param path
	 * @param：@param data
	 * @param：@return
	 * @return：String返回类型
	 * @throws
	 */
	String createEphemeralSequential(final String path, final byte[] data);

	/**
	 * @Title: createPersistent
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:49:22
	 * @Description: Createing CreateMode is PERSISTENT node
	 * @param：@param path
	 * @return：void返回类型
	 * @throws
	 */
	void createPersistent(String path);

	/**
	 * @Title: createPersistent
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:50:04
	 * @Description: Create CreateMode is persistent node with empty data (null)
	 * @param：@param path
	 * @param：@param createParents
	 * @return：void返回类型
	 * @throws
	 */
	void createPersistent(String path, boolean createParents);

	/**
	 * @Title: createPersistent
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:50:38
	 * @Description: Create a persistent node.
	 * @param：@param path
	 * @param：@param data
	 * @return：void返回类型
	 * @throws
	 */
	void createPersistent(String path, byte[] data);

	/**
	 * @Title: createPersistentSequential
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:51:13
	 * @Description: Create CreateMode is persistent, sequental node.
	 * @param：@param path
	 * @param：@param data
	 * @param：@return
	 * @return：String返回类型
	 * @throws
	 */
	String createPersistentSequential(String path, byte[] data);

	/**
	 * @Title: delete
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:51:47
	 * @Description: delete a node
	 * @param：@param path
	 * @param：@return
	 * @return：boolean返回类型
	 * @throws
	 */
	boolean delete(final String path);

	/**
	 * @Title: deleteRecursive
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:52:10
	 * @Description: delete a node with all children（含子节点）
	 * @param：@param path
	 * @param：@return
	 * @return：boolean返回类型
	 * @throws
	 */
	boolean deleteRecursive(String path);

	/**
	 * @Title: exists
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:52:50
	 * @Description: 判断节点
	 * @param：@param path
	 * @param：@return
	 * @return：boolean返回类型
	 * @throws
	 */
	boolean exists(final String path);

	/**
	 * @Title: getChildren
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:53:22
	 * @Description: 获取给定节点的子节点
	 * @param：@param path
	 * @param：@return
	 * @return：List<String>返回类型
	 * @throws
	 */
	List<String> getChildren(String path);

	/**
	 * @Title: getCreationTime
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:53:52
	 * @Description: get the node creation time
	 * @param：@param path
	 * @param：@return
	 * @return：long返回类型
	 * @throws
	 */
	long getCreationTime(String path);

	/**
	 * @Title: numberOfListeners
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:54:39
	 * @Description: 获取此 ZK connected all 连接数
	 * @param：@return
	 * @return：int返回类型
	 * @throws
	 */
	int numberOfListeners();

	/**
	 * @Title: readData
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:55:31
	 * @Description:读取数据
	 * @param：@param path
	 * @param：@return
	 * @return：byte[]返回类型
	 * @throws
	 */
	byte[] readData(String path);

	/**
	 * @Title: readData
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:56:19
	 * @Description: read the data for the node
	 * @param：@param path
	 * @param：@param returnNullIfPathNotExists
	 * @param：@return
	 * @return：byte[]返回类型
	 * @throws
	 */
	byte[] readData(String path, boolean returnNullIfPathNotExists);

	/**
	 * @Title: readData
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:56:41
	 * @Description: read the data and stat for the node
	 * @param：@param path
	 * @param：@param stat
	 * @param：@return
	 * @return：byte[]返回类型
	 * @throws
	 */
	byte[] readData(String path, Stat stat);

	/**
	 * @Title: subscribeChildChanges
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:58:13
	 * @Description: subscribe the changing for children
	 * @param：@param path
	 * @param：@param listener
	 * @param：@return
	 * @return：List<String>返回类型
	 * @throws
	 */
	List<String> subscribeChildChanges(String path, IZkChildListener listener);

	/**
	 * @Title: subscribeDataChanges
	 * @author：孙刚
	 * @date：2014年9月2日 下午1:59:24
	 * @Description: subscribe the data changing for the node
	 * @param：@param path
	 * @param：@param listener
	 * @return：void返回类型
	 * @throws
	 */
	void subscribeDataChanges(String path, IZkDataListener listener);

	/**
	 * @Title: subscribeStateChanges
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:00:29
	 * @Description: subscribe the connection state
	 * @param：@param listener
	 * @return：void返回类型
	 * @throws
	 */
	void subscribeStateChanges(IZkStateListener listener);

	/**
	 * @Title: unsubscribeAll
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:00:57
	 * @Description: unsubscribe the child listener
	 * @param：
	 * @return：void返回类型
	 * @throws
	 */
	void unsubscribeAll();

	/**
	 * @Title: unsubscribeChildChanges
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:01:49
	 * @Description: unsubscribe the child listener
	 * @param：@param path
	 * @param：@param childListener
	 * @return：void返回类型
	 * @throws
	 */
	void unsubscribeChildChanges(String path, IZkChildListener childListener);

	/**
	 * @Title: unsubscribeDataChanges
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:02:15
	 * @Description: unsubscribe the connection state
	 * @param：@param path
	 * @param：@param dataListener
	 * @return：void返回类型
	 * @throws
	 */
	void unsubscribeDataChanges(String path, IZkDataListener dataListener);

	/**
	 * @Title: unsubscribeStateChanges
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:02:37
	 * @Description: unsubscribe the connection state
	 * @param：@param stateListener
	 * @return：void返回类型
	 * @throws
	 */
	void unsubscribeStateChanges(IZkStateListener stateListener);

	/**
	 * @Title: cas
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:03:34 Updates data of an existing znode. The current
	 *                 content of the znode is passed to the {@link DataUpdater}
	 *                 that is passed into this method, which returns the new
	 *                 content. The new content is only written back to
	 *                 ZooKeeper if nobody has modified the given znode in
	 *                 between. If a concurrent change has been detected the new
	 *                 data of the znode is passed to the updater once again
	 *                 until the new contents can be successfully written back
	 *                 to ZooKeeper.
	 * @param：@param path
	 * @param：@param updater
	 * @return：void返回类型
	 * @throws
	 */
	void cas(String path, DataUpdater updater);

	/**
	 * @Title: waitForKeeperState
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:04:15
	 * @Description: wait some time for the state
	 * @param：@param keeperState
	 * @param：@param time
	 * @param：@param timeUnit
	 * @param：@return
	 * @return：boolean返回类型
	 * @throws
	 */
	boolean waitForKeeperState(KeeperState keeperState, long time,
							   TimeUnit timeUnit);

	/**
	 * @Title: waitUntilConnected
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:06:15
	 * @Description: wait for the connected state.
	 * @param：@return
	 * @param：@throws ZkInterruptedException
	 * @return：boolean返回类型
	 * @throws
	 */
	boolean waitUntilConnected() throws ZkInterruptedException;

	/**
	 * @Title: waitUntilConnected
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:06:53
	 * @Description: wait for the connected state
	 * @param：@param time
	 * @param：@param timeUnit
	 * @param：@return
	 * @return：boolean返回类型
	 * @throws
	 */
	boolean waitUntilConnected(long time, TimeUnit timeUnit);

	/**
	 * @Title: waitUntilExists
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:07:11
	 * @Description: wait some unit until the node exists
	 * @param：@param path
	 * @param：@param timeUnit
	 * @param：@param time
	 * @param：@return
	 * @return：boolean返回类型
	 * @throws
	 */
	boolean waitUntilExists(String path, TimeUnit timeUnit, long time);

	/**
	 * @Title: writeData
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:07:32
	 * @Description: write the data for the node
	 * @param：@param path
	 * @param：@param data
	 * @param：@return
	 * @return：Stat返回类型
	 * @throws
	 */
	Stat writeData(String path, byte[] data);

	/**
	 * @Title: writeData
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:07:55
	 * @Description: write the data for the node
	 * @param：@param path
	 * @param：@param data
	 * @param：@param expectedVersion
	 * @param：@return
	 * @return：Stat返回类型
	 * @throws
	 */
	Stat writeData(String path, byte[] data, int expectedVersion);

	/**
	 * @Title: multi
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:08:24
	 * @Description: multi operation for zookeeper 3.4.x
	 * @param：@param ops
	 * @param：@return
	 * @return：List<?>返回类型
	 * @throws
	 */
	List<?> multi(Iterable<?> ops);

	/**
	 * @Title: getZooKeeper
	 * @author：孙刚
	 * @date：2014年9月2日 下午2:08:39
	 * @Description: (这里用一句话描述这个方法的作用)
	 * @param：@return
	 * @return：ZooKeeper返回类型
	 * @throws
	 */
	ZooKeeper getZooKeeper();

	// check the connecting state of zookeeper client
	boolean isConnected();

	interface DataUpdater {
		/**
		 * Updates the current data of a znode.
		 * 
		 * @param currentData
		 *            The current contents.
		 * @return the new data that should be written back to ZooKeeper.
		 */
		public byte[] update(byte[] currentData);

	}
}
