package com.nats.examples.streambackedsubscriber2;

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
import io.nats.client.MessageConsumer;
import io.nats.client.MessageHandler;
import io.nats.client.OrderedConsumerContext;
import io.nats.client.StreamContext;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.impl.Headers;

/*
 * https://docs.nats.io/nats-concepts/jetstream/headers
 *
 * * Todo:
 *   - Copy class and simplify
 *      - remove pause and restart
 *      - Pass in a Dispatcher from the outside
 *      - OrderedConsumer also dlivers to Dispatcher
 *      - silently ignore old sequences
 *      - On catchup
 *          - destroy OrderedConsumer
 *      	- deliver synchronously from handler
 *      - Detect gaps when in SUBSCRIBING
 *          - Start OrderedConsumer
 *      - STATUS
 *      	- RECOVERING
 *          - SUBSCRIBING
 *	        - STOPPED
 */


public class StreamBackedSubscriber2 implements MessageHandler{

	//Connection connection;
	String subject;
	String repubSubject;
	StreamContext streamContext;
	MessageHandler handler;
	Dispatcher dispatcher;

	long recoverTimeout = 1000;  //ms to wait for messages before concluding that we have reach the end of the stream

	MessageConsumer consumer;
	LinkedList<Message> messages = new LinkedList<Message>();

	private java.time.ZonedDateTime startTime;
	private long lastDispatchedSequence= 0;  //

	public static String NATSMSGID = "Nats-Msg-Id";
	public static String NATSLASTSEQUENCE = "Nats-Last-Sequence";
	public static String NATSSEQUENCE = "Nats-Sequence";


	String status;
	final static String STATUS_READY = "READY";
	final static String STATUS_RECOVERING = "RECOVERING";
	final static String STATUS_SUBSCRIBING = "SUBSCRIBING";
	final static String STATUS_STOPPED = "STOPPED";


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

	public StreamBackedSubscriber2( StreamContext streamContext, Dispatcher dispatcher, String subject, String repubPrefix,  MessageHandler handler ) {
		status = STATUS_READY;
		this.dispatcher = dispatcher;
		this.subject = subject;
		this.handler = handler;
		this.streamContext = streamContext;
		repubSubject = repubPrefix+"."+subject;
	}

	private void stopOrderedConsumer() {
		if ( consumer != null) {
			consumer.stop();
		}
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



	private void startOrderedConsumer(java.time.ZonedDateTime startTime, long sequence) throws IOException, JetStreamApiException {

		trace("*** Start Consumer");
		OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration().filterSubject(subject);

		if ( startTime != null && sequence > 0)
			throw new RuntimeException( "Cannot set both startTime and sequence.");

		if ( startTime != null ) {
			trace("Starting at: " + startTime);
			ocConfig = ocConfig.deliverPolicy(  DeliverPolicy.ByStartTime ).startTime(startTime);
		} else
		if ( sequence > 0 ) {
			trace("Starting at: " + sequence);
			ocConfig = ocConfig.deliverPolicy(  DeliverPolicy.ByStartSequence ).startSequence(sequence);
		} else {
			//If there is no filter, we deliver all
			ocConfig = ocConfig.deliverPolicy( DeliverPolicy.All );
		}
		OrderedConsumerContext oc = streamContext.createOrderedConsumer( ocConfig );
		consumer = oc.consume(dispatcher, this);
	}


	private boolean dispatch( Message msg, long sequence) throws InterruptedException
	{
		if ( msg.isJetStream() ) {
			trace( "Dispatch from stream: " + sequence);
		} else {
			trace( "Dispatch from subscriber: " + sequence);
		}
		handler.onMessage(msg);
		return true;
	}


	@Override
	public void onMessage(Message msg) throws InterruptedException {
		long currentSeq = getID( msg );

		trace( "OnMessage: " + currentSeq);

		if ( !msg.isJetStream() && chaosTest) {
			if (rnd.nextInt(4) == 0 ) {
				trace( "**************Dropping message " + currentSeq);
				return;
			}
		}

		if ( status == STATUS_STOPPED ) {
			trace("Stopped - discarding : " + currentSeq );
			return;
		}

		Message pendingMsg = null;

		if ( msg.isJetStream()) {
			if ( status == STATUS_SUBSCRIBING ) {
				//Something went wrong
				stopOrderedConsumer(); //Stop again
				return;
			}

			//Check if we did catch up
			pendingMsg = messages.peek();
			long pendingID = getID( pendingMsg );

			// >= because messages MAY have disappeared from the stream, but we have to continue anyway
			if ( pendingMsg != null && currentSeq >= pendingID  ) {
				trace("Catch up - discarding : " + currentSeq );
				stopOrderedConsumer();
				//There no gaps by definition when we are recovering
				//Even if messages in the stream were incomplete we must reset
				lastDispatchedSequence = 0;
				msg = null; //Discard
				status = STATUS_SUBSCRIBING;
				//Dispatch all pending
				pendingMsg = messages.poll();
			} else {
				//Dispatch incoming
				pendingMsg = msg;
			}
		} else {
			//Buffer if we are still recovering
			if ( status == STATUS_RECOVERING ) {
				trace("Buffering: " + currentSeq);
				messages.addLast(msg);
				return;
			} else {
				//Dispatch
				pendingMsg = msg;
			}
		}

		while ( pendingMsg != null )
		{
			//Check for gaps
			if ( !msg.isJetStream() ) {
				long lastExpectedSeq = getFromHeaderLong( msg, NATSLASTSEQUENCE);
				if ( lastDispatchedSequence != 0 && lastDispatchedSequence != lastExpectedSeq ) {
					//We have a gap - so restart after last dispatched
					trace("Gap - Expected: " +  lastDispatchedSequence + " got: " + lastExpectedSeq );
					trace("Gap - Starting recovery from: " + (lastDispatchedSequence+1) );
					messages.addFirst(pendingMsg);  //Put it back
					try {
						startOrderedConsumer(startTime, lastDispatchedSequence+1 );
						status = STATUS_RECOVERING; //Only when successful
						return;
					} catch (IOException | JetStreamApiException e) {
						//We MAY want to retry
						e.printStackTrace();
					}
				}
			}
			currentSeq = getID( msg );
			//Finnally check for duplciates
			if ( currentSeq <= lastDispatchedSequence  ) {
				trace("Duplicate - discarding : " + currentSeq );
			} else {
				dispatch(pendingMsg,  currentSeq);
				lastDispatchedSequence = currentSeq;
			}
			pendingMsg = null;
			if ( status == STATUS_SUBSCRIBING ) //All messages in the queue must go out
				pendingMsg = messages.poll();
		}

	}



	private void startSubscriber() {
		trace( "Subscribing: " + repubSubject);
		dispatcher.subscribe(repubSubject, this);
	}

	private void stopSubscriber() {
		trace( "Unsubscribing: " + repubSubject);
		dispatcher.unsubscribe(repubSubject);
	}

	synchronized private void clearList() {
		messages.clear();
	}

	synchronized public void start( java.time.ZonedDateTime startTime ) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {
		start( startTime, 0);
	}

	synchronized private void start( java.time.ZonedDateTime startTime, long startSequence ) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {

		if ( !status.equals(STATUS_READY ))
			new IllegalStateException("Already started");

		this.startTime = startTime;
		status = STATUS_RECOVERING;
		startSubscriber();
		if (chaosTest )
			Thread.sleep(rnd.nextInt(3000));
		startOrderedConsumer(startTime, startSequence );
	}

	synchronized public void stop() {

		if ( status.equals(STATUS_STOPPED ) )
			new IllegalStateException("Already stopped");

		if ( status.equals(STATUS_READY ) )
			new IllegalStateException("Not yet started");

		stopSubscriber();
		stopOrderedConsumer();
		clearList();

		status = STATUS_STOPPED;
	}
}
