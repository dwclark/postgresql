package db.postresql.protocol.v3;

import java.io.IOException;
import java.net.SocketException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.util.concurrent.Executor;

public class SSLEngineManager {
    
	private final SocketChannel channel;
	private final SSLEngine engine;
    private final Executor executor;
    
	final ByteBuffer appSendBuffer;
	final ByteBuffer netSendBuffer;
	final ByteBuffer appRecvBuffer;
	final ByteBuffer netRecvBuffer;

    private SSLEngineResult engineResult = null;

    private static final Executor defaultExecutor = new Executor() {
            public void execute(Runnable r) {
                r.run();
            }
        };

    public SSLEngineManager(SocketChannel channel, SSLEngine engine) {
        this(channel, engine, defaultExecutor);
    }

	public SSLEngineManager(SocketChannel channel, SSLEngine engine, Executor executor) {
		this.channel = channel;
		this.engine = engine;
		SSLSession session = engine.getSession();
		int netBufferSize = session.getPacketBufferSize();
		int appBufferSize = session.getApplicationBufferSize();
		this.appSendBuffer = ByteBuffer.allocate(appBufferSize);
		this.netSendBuffer = ByteBuffer.allocate(netBufferSize);
		this.appRecvBuffer = ByteBuffer.allocate(appBufferSize);
		this.netRecvBuffer = ByteBuffer.allocate(netBufferSize);
        this.executor = executor;
	}

	public int read() throws IOException, SSLException {
		if (engine.isInboundDone())
			// Kind test to return another EOF:
			// SocketChannels react badly
			// if you try to read at EOF more than once.
			return -1;
		int pos = appRecvBuffer.position();
		// Read from the channel
		int count = channel.read(netRecvBuffer);
		// Unwrap the data just read
		netRecvBuffer.flip();
		engineResult = engine.unwrap(netRecvBuffer, appRecvBuffer);
		netRecvBuffer.compact();
		// Process the engineResult.Status
		switch (engineResult.getStatus()) {
		case BUFFER_UNDERFLOW:
			return 0;// nothing was read, nothing was produced
		case BUFFER_OVERFLOW:
			// no room in appRecvBuffer: application must clear it
			throw new BufferOverflowException();
		case CLOSED:
			channel.socket().shutdownInput();// no more input
			// outbound close_notify will be sent by engine
			break;
		case OK:
			break;
		}
		// process any handshaking now required
		while (processHandshake()) { }
        
		if (count == -1) {
			engine.closeInbound();
			// throws SSLException if close_notify not received.
		}
		if (engine.isInboundDone()) {
			return -1;
		}
		// return count of application data read
		count = appRecvBuffer.position() - pos;
		return count;
	}

	public int write() throws IOException, SSLException {
		int pos = appSendBuffer.position();
		netSendBuffer.clear();
		// Wrap the data to be written
		appSendBuffer.flip();
		engineResult = engine.wrap(appSendBuffer, netSendBuffer);
		appSendBuffer.compact();
		// Process the engineResult.Status
		switch (engineResult.getStatus()) {
		case BUFFER_UNDERFLOW:
			throw new BufferUnderflowException();
		case BUFFER_OVERFLOW:
			// this cannot occur if there is a flush after every
			// wrap, as there is here.
			throw new BufferOverflowException();
		case CLOSED:
			throw new SSLException("SSLEngine is CLOSED");
		case OK:
			break;
		}
		// Process handshakes
		while (processHandshake()) { }
        
		// Flush any pending data to the network
		flush();
		// return count of application bytes written.
		return pos - appSendBuffer.position();
	}

	public int flush() throws IOException {
		netSendBuffer.flip();
		int count = channel.write(netSendBuffer);
		netSendBuffer.compact();
		return count;
	}

	/**
	 * Close the session and the channel.
	 * 
	 * @exception IOException
	 *                on any I/O error.
	 * @exception SSLException
	 *                on any SSL error.
	 */
	public void close() throws IOException, SSLException {
		try {
			// Flush any pending output data
			flush();
			if (!engine.isOutboundDone()) {
				engine.closeOutbound();
				while (processHandshake()) { }
                
				/*
				 * RFC 2246 #7.2.1: if we are initiating this close, we may send
				 * the close_notify without waiting for an incoming
				 * close_notify. If we weren't the initiator we would have
				 * already received the inbound close_notify in read(), and
				 * therefore already have done closeOutbound(), so, we are
				 * initiating the close, so we can skip the closeInbound().
				 */
			} else if (!engine.isInboundDone()) {
				// throws SSLException if close_notify not received.
				engine.closeInbound();
				processHandshake();
			}
		} finally {
			// Close the channel.
			channel.close();
		}
	}

	private boolean processHandshake() throws IOException {
		int count;
		// process the handshake status
		switch (engine.getHandshakeStatus()) {
		case NEED_TASK:
			runDelegatedTasks();
			return false;// can't continue during tasks
		case NEED_UNWRAP:
			// Don't read if inbound is already closed
			count = engine.isInboundDone() ? -1 : channel.read(netRecvBuffer);
			netRecvBuffer.flip();
			engineResult = engine.unwrap(netRecvBuffer, appRecvBuffer);
			netRecvBuffer.compact();
			break;
		case NEED_WRAP:
			appSendBuffer.flip();
			engineResult = engine.wrap(appSendBuffer, netSendBuffer);
			appSendBuffer.compact();
			if (engineResult.getStatus() == SSLEngineResult.Status.CLOSED) {
				// RFC 2246 #7.2.1 requires us to respond to an
				// incoming close_notify with an outgoing
				// close_notify. The engine takes care of this, so we
				// are now trying to send a close_notify, which can
				// only happen if we have just received a
				// close_notify.
				// Try to flush the close_notify.
				try {
					count = flush();
				} catch (SocketException exc) {
					// tried but failed to send close_notify back:
					// this can happen if the peer has sent its
					// close_notify and then closed the socket,
					// which is permitted by RFC 2246.
					// exc.printStackTrace();
				}
			} else {
				// flush without the try/catch,
				// letting any exceptions propagate.
				count = flush();
			}
			break;
		case FINISHED:
		case NOT_HANDSHAKING:
			// handshaking can cease.
			return false;
		}
		// Check the result of the preceding wrap or unwrap.
		switch (engineResult.getStatus()) {
		case BUFFER_UNDERFLOW:// fall through
		case BUFFER_OVERFLOW:
			// handshaking cannot continue.
			return false;
		case CLOSED:
			if (engine.isOutboundDone()) {
				channel.socket().shutdownOutput();// stop sending
			}
			return false;
		case OK:
			// handshaking can continue.
			break;
		}
		return true;
	}

	protected void runDelegatedTasks() {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            executor.execute(task);
        }
	}
}
