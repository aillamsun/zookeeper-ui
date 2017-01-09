package com.sung.zk.ui.server.zookeeper.zk;

import com.github.zkclient.exception.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

public class ZkClient implements Watcher, IZkClient {

	private Logger LogUtils = LoggerFactory.getLogger(ZkClient.class);

	protected ZkConnection _connection;
	
	private final Map<String, Set<IZkChildListener>> _childListener = new ConcurrentHashMap<String, Set<IZkChildListener>>();

	private final Map<String, Set<IZkDataListener>> _dataListener = new ConcurrentHashMap<String, Set<IZkDataListener>>();

	private final Set<IZkStateListener> _stateListener = new CopyOnWriteArraySet<IZkStateListener>();

	private volatile KeeperState _currentState;

	private final ZkLock _zkEventLock = new ZkLock();

	private volatile boolean _shutdownTriggered;

	private ZkEventThread _eventThread;

	private Thread _zookeeperEventThread;

	// Create a client with default connection timeout and default session
	// timeout
	public ZkClient(String connectString) {
		this(connectString, DEFAULT_CONNECTION_TIMEOUT);
	}

	// Create a client
	public ZkClient(String connectString, int connectionTimeout) {
		this(connectString, DEFAULT_SESSION_TIMEOUT, connectionTimeout);
	}

	public ZkClient(String connectString, int sessionTimeout,
			int connectionTimeout) {
		this(new ZkConnection(connectString, sessionTimeout), connectionTimeout);
	}

	public ZkClient(ZkConnection zkConnection, int connectionTimeout) {
		_connection = zkConnection;
		connect(connectionTimeout, this);
	}

	public List<String> subscribeChildChanges(String path,
			IZkChildListener listener) {
		synchronized (_childListener) {
			Set<IZkChildListener> listeners = _childListener.get(path);
			if (listeners == null) {
				listeners = new CopyOnWriteArraySet<IZkChildListener>();
				_childListener.put(path, listeners);
			}
			listeners.add(listener);
		}
		return watchForChilds(path);
	}

	public void unsubscribeChildChanges(String path,
			IZkChildListener childListener) {
		synchronized (_childListener) {
			final Set<IZkChildListener> listeners = _childListener.get(path);
			if (listeners != null) {
				listeners.remove(childListener);
			}
		}
	}

	public void subscribeDataChanges(String path, IZkDataListener listener) {
		Set<IZkDataListener> listeners;
		synchronized (_dataListener) {
			listeners = _dataListener.get(path);
			if (listeners == null) {
				listeners = new CopyOnWriteArraySet<IZkDataListener>();
				_dataListener.put(path, listeners);
			}
			listeners.add(listener);
		}
		watchForData(path);
		LogUtils.debug("Subscribed data changes for " + path);
	}

	public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
		synchronized (_dataListener) {
			final Set<IZkDataListener> listeners = _dataListener.get(path);
			if (listeners != null) {
				listeners.remove(dataListener);
			}
			if (listeners == null || listeners.isEmpty()) {
				_dataListener.remove(path);
			}
		}
	}

	public void subscribeStateChanges(final IZkStateListener listener) {
		synchronized (_stateListener) {
			_stateListener.add(listener);
		}
	}

	public void unsubscribeStateChanges(IZkStateListener stateListener) {
		synchronized (_stateListener) {
			_stateListener.remove(stateListener);
		}
	}

	public void unsubscribeAll() {
		synchronized (_childListener) {
			_childListener.clear();
		}
		synchronized (_dataListener) {
			_dataListener.clear();
		}
		synchronized (_stateListener) {
			_stateListener.clear();
		}
	}

	public void createPersistent(String path) {
		createPersistent(path, false);
	}

	public void createPersistent(String path, boolean createParents) {
		try {
			create(path, null, CreateMode.PERSISTENT);
		} catch (ZkNodeExistsException e) {
			if (!createParents) {
				throw e;
			}
		} catch (ZkNoNodeException e) {
			if (!createParents) {
				throw e;
			}
			String parentDir = path.substring(0, path.lastIndexOf('/'));
			createPersistent(parentDir, createParents);
			createPersistent(path, createParents);
		}
	}

	public void createPersistent(String path, byte[] data) {
		create(path, data, CreateMode.PERSISTENT);
	}

	public String createPersistentSequential(String path, byte[] data) {
		return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
	}

	public void createEphemeral(final String path) {
		create(path, null, CreateMode.EPHEMERAL);
	}

	public String create(final String path, byte[] data, final CreateMode mode) {
		if (path == null) {
			throw new NullPointerException("path must not be null.");
		}
		final byte[] bytes = data;

		return retryUntilConnected(new Callable<String>() {

			@Override
			public String call() throws Exception {
				return _connection.create(path, bytes, mode);
			}
		});
	}

	public void createEphemeral(final String path, final byte[] data) {
		create(path, data, CreateMode.EPHEMERAL);
	}

	public String createEphemeralSequential(final String path, final byte[] data) {
		return create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	@Override
	public void process(WatchedEvent event) {
		LogUtils.info("Received event: " + event);
		_zookeeperEventThread = Thread.currentThread();
		boolean stateChanged = event.getPath() == null;
		boolean znodeChanged = event.getPath() != null;

		boolean dataChanged = event.getType() == EventType.NodeDataChanged
				|| event.getType() == EventType.NodeDeleted
				|| event.getType() == EventType.NodeCreated || //
				event.getType() == EventType.NodeChildrenChanged;
		getEventLock().lock();

		try {
			// node was created
			if (getShutdownTrigger()) {
				LogUtils.info("ignoring event '{" + event.getType() + " | "
						+ event.getPath() + "}' since shutdown triggered");
				return;
			}
			if (stateChanged) {
				processStateChanged(event);
			}
			if (dataChanged) {
				processDataOrChildChange(event);
			}
		} finally {
			if (stateChanged) {
				getEventLock().getStateChangedCondition().signalAll();

				// If the session expired we have to signal all conditions,
				// because watches might have been removed and
				// there is no guarantee that those
				// conditions will be signaled at all after an Expired event
				if (event.getState() == KeeperState.Expired) {
					getEventLock().getZNodeEventCondition().signalAll();
					getEventLock().getDataChangedCondition().signalAll();
					// We also have to notify all listeners that something might
					// have changed
					fireAllEvents();
				}
			}
			if (znodeChanged) {
				getEventLock().getZNodeEventCondition().signalAll();
			}
			if (dataChanged) {
				getEventLock().getDataChangedCondition().signalAll();
			}
			getEventLock().unlock();
			LogUtils.info("Leaving process event");
		}
	}

	private void fireAllEvents() {
		for (Entry<String, Set<IZkChildListener>> entry : _childListener
				.entrySet()) {
			fireChildChangedEvents(entry.getKey(), entry.getValue());
		}
		for (Entry<String, Set<IZkDataListener>> entry : _dataListener
				.entrySet()) {
			fireDataChangedEvents(entry.getKey(), entry.getValue());
		}
	}

	public List<String> getChildren(String path) {
		return getChildren(path, hasListeners(path));
	}

	protected List<String> getChildren(final String path, final boolean watch) {
		try {
			return retryUntilConnected(new Callable<List<String>>() {

				@Override
				public List<String> call() throws Exception {
					return _connection.getChildren(path, watch);
				}
			});
		} catch (ZkNoNodeException e) {
			return null;
		}
	}

	public int countChildren(String path) {
		try {
			Stat stat = new Stat();
			this.readData(path, stat);
			return stat.getNumChildren();
			// return getChildren(path).size();
		} catch (ZkNoNodeException e) {
			return -1;
		}
	}

	protected boolean exists(final String path, final boolean watch) {
		return retryUntilConnected(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return _connection.exists(path, watch);
			}
		});
	}

	public boolean exists(final String path) {
		return exists(path, hasListeners(path));
	}

	private void processStateChanged(WatchedEvent event) {
		LogUtils.info("zookeeper state changed (" + event.getState() + ")");
		setCurrentState(event.getState());
		if (getShutdownTrigger()) {
			return;
		}
		try {
			fireStateChangedEvent(event.getState());

			if (event.getState() == KeeperState.Expired) {
				reconnect();
				fireNewSessionEvents();
			}
		} catch (final Exception e) {
			throw new RuntimeException("Exception while restarting zk client",
					e);
		}
	}

	private void fireNewSessionEvents() {
		for (final IZkStateListener stateListener : _stateListener) {
			_eventThread.send(new ZkEventThread.ZkEvent("New session event sent to "
					+ stateListener) {

				@Override
				public void run() throws Exception {
					stateListener.handleNewSession();
				}
			});
		}
	}

	private void fireStateChangedEvent(final KeeperState state) {
		for (final IZkStateListener stateListener : _stateListener) {
			_eventThread.send(new ZkEventThread.ZkEvent("State changed to " + state
					+ " sent to " + stateListener) {

				public void run() throws Exception {
					stateListener.handleStateChanged(state);
				}
			});
		}
	}

	private boolean hasListeners(String path) {

		Set<IZkDataListener> dataListeners = _dataListener.get(path);
		if (dataListeners != null && dataListeners.size() > 0) {
			return true;
		}
		Set<IZkChildListener> childListeners = _childListener.get(path);
		if (childListeners != null && childListeners.size() > 0) {
			return true;
		}
		return false;
	}

	public boolean deleteRecursive(String path) {
		List<String> children;
		try {
			children = getChildren(path, false);
		} catch (ZkNoNodeException e) {
			return true;
		}

		for (String subPath : children) {
			if (!deleteRecursive(path + "/" + subPath)) {
				return false;
			}
		}

		return delete(path);
	}

	private void processDataOrChildChange(WatchedEvent event) {
		final String path = event.getPath();

		if (event.getType() == EventType.NodeChildrenChanged
				|| event.getType() == EventType.NodeCreated
				|| event.getType() == EventType.NodeDeleted) {
			
			Set<IZkChildListener> childListeners = _childListener.get(path);
			
			if (childListeners != null && !childListeners.isEmpty()) {
				fireChildChangedEvents(path, childListeners);
			}
		}

		if (event.getType() == EventType.NodeDataChanged
				|| event.getType() == EventType.NodeDeleted
				|| event.getType() == EventType.NodeCreated) {
			Set<IZkDataListener> listeners = _dataListener.get(path);
			if (listeners != null && !listeners.isEmpty()) {
				fireDataChangedEvents(event.getPath(), listeners);
			}
		}
	}

	private void fireDataChangedEvents(final String path,
			Set<IZkDataListener> listeners) {
		for (final IZkDataListener listener : listeners) {
			_eventThread.send(new ZkEventThread.ZkEvent("Data of " + path
					+ " changed sent to " + listener) {

				@Override
				public void run() throws Exception {
					// reinstall watch
					exists(path, true);
					try {
						byte[] data = readData(path, null, true);
						listener.handleDataChange(path, data);
					} catch (ZkNoNodeException e) {
						listener.handleDataDeleted(path);
					}
				}
			});
		}
	}

	private void fireChildChangedEvents(final String path,
			Set<IZkChildListener> childListeners) {
		try {
			// reinstall the watch
			for (final IZkChildListener listener : childListeners) {
				_eventThread.send(new ZkEventThread.ZkEvent("Children of " + path
						+ " changed sent to " + listener) {

					@Override
					public void run() throws Exception {
						try {
							// if the node doesn't exist we should listen for
							// the root node to reappear
							exists(path);
							List<String> children = getChildren(path);
							listener.handleChildChange(path, children);
						} catch (ZkNoNodeException e) {
							listener.handleChildChange(path, null);
						}
					}
				});
			}
		} catch (Exception e) {
			LogUtils.error(
					"Failed to fire child changed event. Unable to getChildren.  ",
					e);
		}
	}

	public boolean waitUntilExists(String path, TimeUnit timeUnit, long time)
			throws ZkInterruptedException {
		Date timeout = new Date(System.currentTimeMillis()
				+ timeUnit.toMillis(time));
		LogUtils.info("Waiting until znode '" + path
				+ "' becomes available.");
		if (exists(path)) {
			return true;
		}
		acquireEventLock();
		try {
			while (!exists(path, true)) {
				boolean gotSignal = getEventLock().getZNodeEventCondition()
						.awaitUntil(timeout);
				if (!gotSignal) {
					return false;
				}
			}
			return true;
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
	}

	public boolean waitUntilConnected() throws ZkInterruptedException {
		return waitUntilConnected(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
	}

	public boolean waitUntilConnected(long time, TimeUnit timeUnit)
			throws ZkInterruptedException {
		return waitForKeeperState(KeeperState.SyncConnected, time, timeUnit);
	}

	public boolean waitForKeeperState(KeeperState keeperState, long time,
			TimeUnit timeUnit) throws ZkInterruptedException {
		if (_zookeeperEventThread != null
				&& Thread.currentThread() == _zookeeperEventThread) {
			throw new IllegalArgumentException(
					"Must not be done in the zookeeper event thread.");
		}

		Date timeout = new Date(System.currentTimeMillis()
				+ timeUnit.toMillis(time));

		LogUtils.info("Waiting for keeper state " + keeperState);
		acquireEventLock();
		try {
			boolean stillWaiting = true;
			while (_currentState != keeperState) {
				if (!stillWaiting) {
					return false;
				}
				stillWaiting = getEventLock().getStateChangedCondition()
						.awaitUntil(timeout);
			}
			LogUtils.info("State is " + _currentState);
			return true;
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
	}

	private void acquireEventLock() {
		try {
			getEventLock().lockInterruptibly();
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		}
	}

	/**
	 * @param callable
	 *            the callable object
	 * @return result of Callable
	 */
	public <E> E retryUntilConnected(Callable<E> callable) {
		if (_zookeeperEventThread != null
				&& Thread.currentThread() == _zookeeperEventThread) {
			throw new IllegalArgumentException(
					"Must not be done in the zookeeper event thread.");
		}
		while (true) {
			try {
				return callable.call();
			} catch (ConnectionLossException e) {
				Thread.yield();
				waitUntilConnected();
			} catch (SessionExpiredException e) {
				Thread.yield();
				waitUntilConnected();
			} catch (KeeperException e) {
				throw ZkException.create(e);
			} catch (InterruptedException e) {
				throw new ZkInterruptedException(e);
			} catch (Exception e) {
				throw ZkClientUtils.convertToRuntimeException(e);
			}
		}
	}

	public void setCurrentState(KeeperState currentState) {
		getEventLock().lock();
		try {
			_currentState = currentState;
		} finally {
			getEventLock().unlock();
		}
	}

	/**
	 * Returns a mutex all zookeeper events are synchronized aginst. So in case
	 * you need to do something without getting any zookeeper event interruption
	 * synchronize against this mutex. Also all threads waiting on this mutex
	 * object will be notified on an event.
	 * 
	 * @return the mutex.
	 */
	public ZkLock getEventLock() {
		return _zkEventLock;
	}

	public boolean delete(final String path) {
		try {
			retryUntilConnected(new Callable<byte[]>() {

				@Override
				public byte[] call() throws Exception {
					_connection.delete(path);
					return null;
				}
			});

			return true;
		} catch (ZkNoNodeException e) {
			return false;
		}
	}

	public byte[] readData(String path) {
		return readData(path, false);
	}

	public byte[] readData(String path, boolean returnNullIfPathNotExists) {
		byte[] data = null;
		try {
			data = readData(path, null);
		} catch (ZkNoNodeException e) {
			if (!returnNullIfPathNotExists) {
				throw e;
			}
		}
		return data;
	}

	public byte[] readData(String path, Stat stat) {
		return readData(path, stat, hasListeners(path));
	}

	protected byte[] readData(final String path, final Stat stat,
			final boolean watch) {
		byte[] data = retryUntilConnected(new Callable<byte[]>() {

			@Override
			public byte[] call() throws Exception {
				return _connection.readData(path, stat, watch);
			}
		});
		return data;
	}

	public Stat writeData(String path, byte[] object) {
		return writeData(path, object, -1);
	}

	public void cas(String path, DataUpdater updater) {
		Stat stat = new Stat();
		boolean retry;
		do {
			retry = false;
			try {
				byte[] oldData = readData(path, stat);
				byte[] newData = updater.update(oldData);
				writeData(path, newData, stat.getVersion());
			} catch (ZkBadVersionException e) {
				retry = true;
			}
		} while (retry);
	}

	public Stat writeData(final String path, final byte[] data,
			final int expectedVersion) {
		return retryUntilConnected(new Callable<Stat>() {

			@Override
			public Stat call() throws Exception {
				return _connection.writeData(path, data, expectedVersion);
			}
		});
	}

	public void watchForData(final String path) {
		retryUntilConnected(new Callable<Object>() {

			@Override
			public Object call() throws Exception {
				_connection.exists(path, true);
				return null;
			}
		});
	}

	public List<String> watchForChilds(final String path) {
		if (_zookeeperEventThread != null
				&& Thread.currentThread() == _zookeeperEventThread) {
			throw new IllegalArgumentException(
					"Must not be done in the zookeeper event thread.");
		}
		return retryUntilConnected(new Callable<List<String>>() {

			@Override
			public List<String> call() throws Exception {
				exists(path, true);
				try {
					return getChildren(path, true);
				} catch (ZkNoNodeException e) {
				}
				return null;
			}
		});
	}

	public synchronized void connect(final long maxMsToWaitUntilConnected,
			Watcher watcher) {
		if (_eventThread != null) {
			return;
		}
		boolean started = false;
		try {
			getEventLock().lockInterruptibly();
			setShutdownTrigger(false);
			_eventThread = new ZkEventThread(_connection.getServers());
			_eventThread.start();
			_connection.connect(watcher);
			LogUtils.info("Awaiting connection to Zookeeper server: "
					+ maxMsToWaitUntilConnected);
			if (!waitUntilConnected(maxMsToWaitUntilConnected,
					TimeUnit.MILLISECONDS)) {
				throw new ZkTimeoutException(
						String.format(
								"Unable to connect to zookeeper server[%s] within timeout %dms",
								_connection.getServers(),
								maxMsToWaitUntilConnected));
			}
			started = true;
		} catch (InterruptedException e) {
			States state = _connection.getZookeeperState();
			throw new IllegalStateException(
					"Not connected with zookeeper server yet. Current state is "
							+ state);
		} finally {
			getEventLock().unlock();
			if (!started) {
				close();
			}
		}
	}

	public long getCreationTime(String path) {
		try {
			getEventLock().lockInterruptibly();
			return _connection.getCreateTime(path);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
	}

	public synchronized void close() throws ZkInterruptedException {
		if (_eventThread == null) {
			return;
		}
//		LogUtils.logInfo("Closing ZkClient...");
		getEventLock().lock();
		try {
			setShutdownTrigger(true);
			_currentState = null;
			_eventThread.interrupt();
			_eventThread.join(2000);
			_connection.close();
			_eventThread = null;
		} catch (InterruptedException e) {
			throw new ZkInterruptedException(e);
		} finally {
			getEventLock().unlock();
		}
		LogUtils.info("Closing ZkClient...done");
	}

	private void reconnect() throws InterruptedException {
		getEventLock().lock();
		try {
			_connection.close();
			_connection.connect(this);
		} finally {
			getEventLock().unlock();
		}
	}

	private void setShutdownTrigger(boolean triggerState) {
		_shutdownTriggered = triggerState;
	}

	private boolean getShutdownTrigger() {
		return _shutdownTriggered;
	}

	public int numberOfListeners() {
		int listeners = 0;
		for (Set<IZkChildListener> childListeners : _childListener.values()) {
			listeners += childListeners.size();
		}
		for (Set<IZkDataListener> dataListeners : _dataListener.values()) {
			listeners += dataListeners.size();
		}
		listeners += _stateListener.size();

		return listeners;
	}

	@Override
	public List<?> multi(final Iterable<?> ops) {
		return retryUntilConnected(new Callable<List<?>>() {
			@Override
			public List<?> call() throws Exception {
				return _connection.multi(ops);
			}
		});
	}

	public static byte[] toBytes(String s) {
		try {
			return s != null ? s.getBytes("UTF-8") : null;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String toString(byte[] b) {
		try {
			return b != null ? new String(b, "UTF-8") : null;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	// @Override
	public ZooKeeper getZooKeeper() {
		return _connection != null ? _connection.getZooKeeper() : null;
	}

	@Override
	public boolean isConnected() {
		return _currentState == KeeperState.SyncConnected;
	}
}
