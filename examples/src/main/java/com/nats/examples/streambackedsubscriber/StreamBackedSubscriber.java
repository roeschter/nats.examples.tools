package com.nats.examples.streambackedsubscriber;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Random;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.IterableConsumer;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamStatusCheckedException;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.OrderedConsumerContext;
import io.nats.client.StreamContext;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.impl.Headers;

/*
 * https://docs.nats.io/nats-concepts/jetstream/headers
 *

 */


public class StreamBackedSubscriber implements MessageHandler{

	Connection connection;
	String subject;
	String repubSubject;
	StreamContext streamContext;
	MessageHandler handler;
	Dispatcher dispatcher;

	long recoverTimeout = 1000;  //ms to wait for messages before concluding that we have reach the end of the stream

	IterableConsumer consumer;
	LinkedList<Message> messages = new LinkedList<Message>();

	private java.time.ZonedDateTime startTime;
	private long lastDispatchedSequence= 0;  //

	public static String NATSMSGID = "Nats-Msg-Id";
	public static String NATSLASTSEQUENCE = "Nats-Last-Sequence";
	public static String NATSSEQUENCE = "Nats-Sequence";

	String status = STATUS_PAUSED;
	static String STATUS_RECOVERING = "RECOVERING";
	static String STATUS_STOPPED = "STOPPED";
	static String STATUS_PAUSED = "PAUSED";
	static String STATUS_SUBSCRIBING = "SUBSCRIBING";

	private static int SOURCE_STREAM = 1;
	private static int SOURCE_SUBSCRIBER = 2;

	LinkedHashSet<String> delivered = new LinkedHashSet<String>();  //For deduplication
	private static int deliveredMaxSize = 10;

	private volatile boolean chaosTest = false;   //Introduces jitter and lost messages
	private Random rnd;
	private boolean trace = false;

	private void trace(String s) {
		if ( trace )
			System.out.println(s);
	}

	public void chaosTest( boolean test )
	{
		if ( test)
			rnd = new Random();
		chaosTest = test;
	}

	public StreamBackedSubscriber( StreamContext streamContext, Connection connection, String subject, String repubPrefix,  MessageHandler handler ) {
		//this.jetstream = jetstream;
		this.connection = connection;
		this.subject = subject;
		this.handler = handler;
		this.streamContext = streamContext;
		repubSubject = repubPrefix+"."+subject;
	}

	private void cleanDelivered() {
		if ( delivered.size() > 2*deliveredMaxSize )
		{
			int removeSize = delivered.size() - deliveredMaxSize;
			Iterator<String> iterator = delivered.iterator();
			String[] remove = new String[removeSize];
			for ( int i=0; i<removeSize;  i++ ) {
				remove[i] = iterator.next();
			}
			for ( int i=0; i<removeSize;  i++ ) {
				delivered.remove(remove[i]);
			}
		}

	}

	private void stopOrderedConsumer() {
		if ( consumer == null)
			return;

		consumer.stop();
		consumer = null;
	}


	public static long getFromHeaderLong( Message msg, String name ) {
		String val = getFromHeader(msg, name, "0");
		return Long.parseLong(val);
	}

	public static String getFromHeader( Message msg, String name ) {
		return getFromHeader(msg, name, "");
	}

	public static long getID( Message msg  ) {
		if ( msg == null )
			return -1;

		if ( msg.isJetStream())
			return msg.metaData().streamSequence();
		else
			return getFromHeaderLong( msg, NATSSEQUENCE);
	}


	public static String getFromHeader( Message msg, String name, String _default ) {
		if ( msg == null ) return "";

		Headers headers = msg.getHeaders();
		if ( headers == null ) return _default;

		String id = headers.getFirst(name);

		return ( id==null)?_default:id;
	}


	/*
	 * Returns false to signal restart was triggered
	 */
	private boolean dispatch( Message msg, int source ) throws InterruptedException
	{
		long lastExpectedSeq = getFromHeaderLong( msg, NATSLASTSEQUENCE);
		String msgID = getFromHeader( msg, NATSMSGID);
		long currentSeq = getID( msg );

		//FIrst check for duplicates
		if ( delivered.contains(msgID)) {
			trace("skipping duplicate: " + msgID + " : " + currentSeq);
			return true;
		}

		if ( source == SOURCE_STREAM ) {
			trace( "Dispatch from stream: " + currentSeq);
		} else if (source == SOURCE_SUBSCRIBER ) {
			trace( "Dispatch from subscriber: " + currentSeq);

			//We have a gap so we restart to recover
			if ( lastDispatchedSequence != 0 && lastDispatchedSequence != lastExpectedSeq ) {
				trace("gap detected - Last Sequence Dispatched: " + lastDispatchedSequence + " - Expected : " + lastExpectedSeq + " - Current: " +currentSeq);
				try {
					clearList();
					restart( lastDispatchedSequence+1 );
				} catch ( Exception e) {
					//If recovery fails because consumer cannot be created
					status = STATUS_SUBSCRIBING;
					e.printStackTrace();
				}
				return false;
			}
		}

		//we are clear to go
		delivered.add(msgID);
		cleanDelivered();
		lastDispatchedSequence = currentSeq;
		handler.onMessage(msg);
		return true;
	}

	/*
	 *Deliver all pending messages, update dedup list and atomically pdate status
	 */
	private boolean dispatchPending() throws InterruptedException
	{
		Message firstPendingMsg;
		boolean cont = true;

		trace("*** Dispatch pending");
		//Deliver the pending messages
		while ( cont )
		{
			synchronized ( this ) {
				firstPendingMsg = messages.poll();
				if ( firstPendingMsg == null ) {
					//We are done recovering
					trace("*** Done Dispatch pending");

					status = STATUS_SUBSCRIBING;
					return true;
				}
			}
			if ( !dispatch(firstPendingMsg, SOURCE_SUBSCRIBER) )
				return false;
		}
		return true;
	}

	private void dispatchOrderedConsumer()
	{
		try {
			Message msg = null;
			boolean cont = true;
			while ( consumer != null && (msg = consumer.nextMessage(recoverTimeout )) != null && cont) {
				trace( "Consumer: " + getID(msg));

				Message firstPendingMsg = null;
				synchronized ( this ) {
					firstPendingMsg = messages.peek();
				}
				//String pendingID = getFromHeader(firstPendingMsg, NATSMSGID);
				//String incomingID = getFromHeader(msg, NATSMSGID);
				long pendingID = getID( firstPendingMsg );
				long incomingID = getID( msg );
				//Check if we did catch up
				//if ( pendingID.equals(incomingID)) {
				if ( pendingID == incomingID) {
					cont = false;
				} else {
					//Deliver incoming message
					dispatch(msg, SOURCE_STREAM);
				}

			}
			if ( msg == null)
				trace( "OrderedConsumer on " + subject + " stopped after timeout: " + recoverTimeout);


		} catch ( Exception e )
		{
			e.printStackTrace();
			//Going into subscribing mode gives us a chance to recover later when a gap is detected
			synchronized( this)  {
				status = STATUS_SUBSCRIBING;
			}
		}
		finally {
			if ( consumer != null ) {
				consumer.stop();
				consumer = null;
			}
			trace( "Stopped Consumer");
		}
		//Dispatch all buffered messages from subscriber
		try {
			dispatchPending();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		trace( "Done Recovering");
	}

	private void startOrderedConsumer(java.time.ZonedDateTime startTime, long sequence) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {

		trace("*** Start Consumer");
		OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration()
			.filterSubject(subject);

		if ( startTime != null && sequence > 0)
			throw new RuntimeException( "Cannot set both startTime and sequence.");

		if ( startTime != null ) {
			trace("Starting at: " + startTime);
			ocConfig = ocConfig.deliverPolicy(  DeliverPolicy.ByStartTime )
					.startTime(startTime);

		} else
		if ( sequence > 0 ) {
			trace("Starting at: " + sequence);
			ocConfig = ocConfig.deliverPolicy(  DeliverPolicy.ByStartSequence ).startSequence(sequence);
		} else {
			//If there is no filter, we deliver all
			ocConfig = ocConfig.deliverPolicy( DeliverPolicy.All );
		}
		OrderedConsumerContext oc = streamContext.createOrderedConsumer( ocConfig );
		consumer = oc.iterate();

		//Delivering messages should not block
		Thread thread = new Thread(() -> dispatchOrderedConsumer());
	    thread.start();

	}


	@Override
	public void onMessage(Message msg) throws InterruptedException {
		long currentSeq = getID( msg );

		trace( "OnMessage: " + currentSeq);
		if (chaosTest) {
			if (rnd.nextInt(4) == 0 ) {
				trace( "**************Dropping message " + currentSeq);
				return;
			}
		}

		synchronized( this)  {
			if ( status == STATUS_RECOVERING ) {
				trace("Buffering: " + currentSeq);
				messages.addLast(msg);
				return;
			}
			if ( status == STATUS_STOPPED ) {
				trace("Stopped - discarding : " + currentSeq );
				return;
			}
		}
		dispatch(msg, SOURCE_SUBSCRIBER);
	}



	private void startSubscriber() {
		trace( "Subscribing: " + repubSubject);
		dispatcher = connection.createDispatcher();
		dispatcher.subscribe(repubSubject, this);
	}

	private void stopSubscriber() {
		trace( "Unsubscribing: " + repubSubject);
		dispatcher.unsubscribe(repubSubject);
		connection.closeDispatcher(dispatcher);
	}

	synchronized private void clearList() {
		messages.clear();
	}

	synchronized public void start( java.time.ZonedDateTime startTime ) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {
		start( startTime, 0);
	}

	synchronized private void start( java.time.ZonedDateTime startTime, long startSequence ) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {

		if ( status.equals(STATUS_STOPPED ))
			new IllegalStateException("Stopped - cannot restart");

		if ( !status.equals(STATUS_PAUSED ))
			new IllegalStateException("Already started - pause to restart");

		this.startTime = startTime;
		status = STATUS_RECOVERING;
		startSubscriber();

		if (chaosTest ) {
			Thread.sleep(rnd.nextInt(3000));
		}
		startOrderedConsumer(startTime, startSequence );
	}

	synchronized private void restart( long startSequence ) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {
		status = STATUS_RECOVERING;
		stopOrderedConsumer();
		startOrderedConsumer(null, startSequence);
	}

	synchronized public void restart() throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {
		if ( lastDispatchedSequence == 0)
			start(startTime, 0);
		else
			start(null, lastDispatchedSequence+1);
	}

	synchronized public void stop() {

		if ( status.equals(STATUS_STOPPED ) || status.equals(STATUS_PAUSED ) )
			new IllegalStateException("Not started");

		stopSubscriber();
		stopOrderedConsumer();
		clearList();

		status = STATUS_STOPPED;
	}

	synchronized public void pause() {

		if ( status.equals(STATUS_STOPPED ) || status.equals(STATUS_PAUSED ) )
			new IllegalStateException("Not started");

		stopSubscriber();
		stopOrderedConsumer();
		clearList();

		status = STATUS_PAUSED;
	}
}
