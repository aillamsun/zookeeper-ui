package com.sung.zk.ui.server.zookeeper.zk;

import com.github.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


public class ZkEventThread extends Thread {

    private Logger LogUtils = LoggerFactory.getLogger(ZkEventThread.class);

    private final BlockingQueue<ZkEvent> _events = new LinkedBlockingQueue<ZkEvent>();

    private static final AtomicInteger _eventId = new AtomicInteger(0);

    private volatile boolean shutdown = false;

    static abstract class ZkEvent {
        private final String _description;

        public ZkEvent(String description) {
            _description = description;
        }

        public abstract void run() throws Exception;

        @Override
        public String toString() {
            return "ZkEvent[" + _description + "]";
        }
    }

    ZkEventThread(String name) {
        setDaemon(true);
        setName("ZkClient-EventThread-" + getId() + "-" + name);
    }

    @Override
    public void run() {
        LogUtils.info("Starting ZkClient event thread.");
        try {
            while (!isShutdown()) {
                ZkEvent zkEvent = _events.take();
                int eventId = _eventId.incrementAndGet();
                LogUtils.info("Delivering event #" + eventId + " " + zkEvent);
                try {
                    zkEvent.run();
                } catch (InterruptedException e) {
                    shutdown();
                } catch (ZkInterruptedException e) {
                    shutdown();
                } catch (Exception e) {
                    LogUtils.error("Error handling event " + zkEvent, e);
                }
                LogUtils.info("Delivering event #" + eventId + " done");
            }
        } catch (InterruptedException e) {
            LogUtils.error("Terminate ZkClient event thread.", e);
        }
    }

    /**
     * @return the shutdown
     */
    public boolean isShutdown() {
        return shutdown || isInterrupted();
    }

    public void shutdown() {
        this.shutdown = true;
        this.interrupt();
    }

    public void send(ZkEvent event) {
        if (!isShutdown()) {
            LogUtils.info("New event: " + event);
            _events.add(event);
        }
    }
}
