package com.nats.examples.streambackedsubscriberv2;

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
import io.nats.client.Subscription;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.impl.Headers;

/*
 * https://docs.nats.io/nats-concepts/jetstream/headers
 *
 * Global Backgroudnd thread detecting idle subscribers and closing OrderedConsumers
 *
 */


public class StreamBackedSubscriber implements MessageHandler{

	//Connection connection;
	String subject;
	String repubSubject;
	StreamContext streamContext;
	MessageHandler handler;
	Dispatcher dispatcher;

	//Not implemented - Would need a global background thread to detect subscriptions, which never received any data.
	//long recoverTimeout = 1000;  //ms to wait for messages before concluding that we have reach the end of the stream

	MessageConsumer consumer;
	Subscription subscription;
	LinkedList<Message> messages = new LinkedList<Message>();

	//private java.time.ZonedDateTime startTime;
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
			System.out.println( subject + ": " + s);
	}

	public void setTrace( boolean t)
	{
		trace = t;
	}

	public void chaosTest( boolean test )
	{
		if ( test)
			rnd = new Random();
		chaosTest = test;
	}

	public StreamBackedSubscriber( StreamContext streamContext, Dispatcher dispatcher, String subject, String repubSubject,  MessageHandler handler ) {
		status = STATUS_READY;
		this.dispatcher = dispatcher;
		this.subject = subject;
		this.handler = handler;
		this.streamContext = streamContext;
		this.repubSubject = repubSubject;
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

		int retryCount = 0;
		int retryMax = 5;
		do {
			if ( retryCount != 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				consumer = oc.consume(dispatcher, this);
			} catch ( Exception e ) {
				e.printStackTrace();
			}
			retryCount++;
		} while (consumer == null && retryCount < retryMax);
	}

	private void stopOrderedConsumer() {
		if ( consumer != null) {
			try {
				consumer.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
		try {

			long currentSeq = getID( msg );

			trace( "OnMessage: " + currentSeq);

			//Test code
			if ( !msg.isJetStream() && chaosTest) {
				if (rnd.nextInt(4) == 0 ) {
					trace( "**************Dropping message " + currentSeq);
					return;
				}
			}
			//End Test code

			//Safeguard against race condition
			if ( status == STATUS_STOPPED ) {
				trace("Stopped - discarding : " + currentSeq );
				return;
			}

			Message pendingMsg = null;

			if ( msg.isJetStream()) {
				//Safeguard against race condition
				if ( status == STATUS_SUBSCRIBING ) {
					//Something went wrong
					stopOrderedConsumer(); //Stop again
					return;
				}

				//Check if we did catch up
				Message peek = messages.peek();
				long pendingID = getID( peek );

				// >= because messages MAY have disappeared from the stream, but we have to continue anyway
				if ( peek != null && currentSeq >= pendingID  ) {
					trace("Catch up - discarding : " + currentSeq );
					stopOrderedConsumer();
					status = STATUS_SUBSCRIBING;
					trace("Status: " + status);
					msg = null; //Discard incoming
					//Dispatch all pending
					pendingMsg = messages.poll();
				} else {
					//Otherwise dispatch incoming
					pendingMsg = msg;
				}
			//Not Jetstream so its from the repub
			} else {
				//Always add to the buffer
				messages.addLast(msg);

				if ( status == STATUS_RECOVERING ) {
					//Buffer if we are still recovering. Just return.
					trace("Buffering: " + currentSeq);
					return;
				} else {
					//Dispatch the first in the buffer
					pendingMsg = messages.poll();;
				}
			}

			while ( pendingMsg != null )
			{
				currentSeq = getID( pendingMsg );
				trace("lastDispatchedSequence: " + lastDispatchedSequence );
				//Check for duplicates, we may have buffered old messages
				if ( currentSeq <= lastDispatchedSequence  ) {
					trace("Duplicate - discarding : " + currentSeq );
				} else {
					//Next check for gaps if we are delivering from repub
					if ( !pendingMsg.isJetStream() ) {
						long lastExpectedSeq = getFromHeaderLong( pendingMsg, NATSLASTSEQUENCE);
						if ( lastDispatchedSequence != 0 && lastDispatchedSequence != lastExpectedSeq ) {
							//We have a gap - so restart after last dispatched
							trace("Subscriber Gap - Expected: " +  lastDispatchedSequence + " got: " + lastExpectedSeq );
							trace("Subscriber Gap - Starting recovery from: " + (lastDispatchedSequence+1) );
							messages.addFirst(pendingMsg);  //Put it back
							pendingMsg = null;
							try {
								startOrderedConsumer(null, lastDispatchedSequence+1 );
								status = STATUS_RECOVERING; //Only when successful
								trace("Status: " + status);
								return;
							} catch (IOException | JetStreamApiException e) {
								//We will stay in while loop and try again
								e.printStackTrace();
							}
						}
					}

					//Now we are ready to dispatch
					lastDispatchedSequence = currentSeq;
					dispatch(pendingMsg,  currentSeq);
				}

				pendingMsg = null;
				if ( status == STATUS_SUBSCRIBING ) //All messages in the queue must go out
					pendingMsg = messages.poll();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}



	private void startSubscriber() {
		trace( "Subscribing: " + repubSubject);
		subscription = dispatcher.subscribe(repubSubject, this);
	}

	private void stopSubscriber() {
		if ( subscription != null)
		{
			trace( "Unsubscribing: " + repubSubject);
			dispatcher.unsubscribe(subscription, 0);

		}
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

		trace( "Starting");
		startOrderedConsumer(startTime, startSequence );
		status = STATUS_RECOVERING;
		startSubscriber();
		if (chaosTest )
			Thread.sleep(rnd.nextInt(3000));

	}

	synchronized public void stop() {

		if ( status.equals(STATUS_STOPPED ) )
			new IllegalStateException("Already stopped");

		if ( status.equals(STATUS_READY ) )
			new IllegalStateException("Not yet started");

		trace( "Stopping");

		stopSubscriber();
		stopOrderedConsumer();
		clearList();

		status = STATUS_STOPPED;
	}
}
