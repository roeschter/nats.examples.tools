package com.nats.examples.streamperformancepubsubmultisubject;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;

import com.nats.examples.streambackedsubscriberv2.StreamBackedSubscriber;
import com.nats.examples.streambackedsubscriberv2.StreamBackedSubscriberTest.Handler;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.OrderedConsumerContext;
import io.nats.client.StreamContext;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

public class StreamMultiSubjectStreamBackedSubscriber {
	public static String[] SERVER = new String[] { "nats://localhost:4222" };

	private static String STREAM = "stream-1";
	private static String SUBJECTPREFIX = "raw01";
	private static int STARTINDEX = 0;
	private static int STOPINDEX = 9999;
	private static long STARTDELAY = 1;
	private static long RECOVER = 10;


	public static long getFromHeaderLong( Message msg, String name ) {
		String val = getFromHeader(msg, name, "0");
		return Long.parseLong(val);
	}

	public static String getFromHeader( Message msg, String name ) {
		return getFromHeader(msg, name, "");
	}

	public static String getFromHeader( Message msg, String name, String _default ) {
		if ( msg == null ) return "";

		Headers headers = msg.getHeaders();
		if ( headers == null ) return _default;

		String id = headers.getFirst(name);

		return ( id==null)?_default:id;
	}


	public static long lastActiveCheck = 0;
    public static void verifyActive() {
    	long time = System.currentTimeMillis();
    	if ( (time - lastActiveCheck ) < 1000 )
    		return;

    	lastActiveCheck = time;
    	for ( int idx=STARTINDEX; idx<=STOPINDEX; idx++ )
        {
    		if (lastActiveTime[idx] != 0 && (time-lastActiveTime[idx])>3000 )
    		{
    			System.out.println("**** INACTIVE: " + idx + "  ************");
    		}
        }
    }

    volatile static long[] lastseq = new long[STOPINDEX+1];
	volatile static long[] lastActiveTime = new long[STOPINDEX+1];;

	public static class Handler implements MessageHandler {

		volatile long delayedDatatime = 0;
		volatile static int mcount = 0;

		public Handler() {

		}

		@Override
		public void onMessage(Message msg) throws InterruptedException {
			//String msgID = StreamBackedSubscriber.getFromHeader(msg, StreamBackedSubscriber.NATSMSGID);
			//System.out.println(msg);
			long deviceSeq = getFromHeaderLong(msg, "seq");
			long lid = getFromHeaderLong(msg, "id");
			long msgTime = getFromHeaderLong(msg, "time");

			int id = (int)lid;

			if (lastseq[id] == 0)
				System.out.println("First message for: " + msg.getSubject() + " - " + deviceSeq);


			if ( lastseq[id] != 0 && deviceSeq != lastseq[id]+1 ) {
				//System.out.println("**************************************************************************************");
				System.out.println("**** GAP  in: " + msg.getSubject() + " - Expected " + (lastseq[id]+1) + " got " + deviceSeq + "   ************");
				System.out.println( msg.getSubject() + " : " + deviceSeq + " : " + StreamBackedSubscriber.getID(msg) );
				//System.out.println("**************************************************************************************");
			}
			lastseq[id] = deviceSeq;
			long time = System.currentTimeMillis();
			lastActiveTime[id] = time;
			long delay = time - msgTime;
			if ( delay> 1000 && (time - delayedDatatime ) > 10000 ) {
				System.out.println("**** Delayed data " + id  + " : " + delay + "   ************");
				delayedDatatime = time;
			}

			mcount++;
			if (( mcount % 1000 ) == 0 ) {
				System.out.println(mcount);
				verifyActive();
			}

		}
	}

	public static void main(String[] arg) throws Exception {

		int i = 0;
		while ( arg.length > i)
		{
			if ( arg[i].equals("-stream") )
			{
				i++;
				STREAM = arg[i];
				i++;
			} else if ( arg[i].equals("-subject") )
			{
				i++;
				STREAM = arg[i];
				i++;
			} else if ( arg[i].equals("-startidx") )
			{
				i++;
				STARTINDEX = Integer.parseInt( arg[i] );
				i++;
			} else if ( arg[i].equals("-stopidx") )
			{
				i++;
				STOPINDEX = Integer.parseInt( arg[i] );
				i++;
			} else if ( arg[i].equals("-delay") )
			{
				i++;
				STARTDELAY = Integer.parseInt( arg[i] );
				i++;
			} else if (arg[i].equals("--")) { //Terminate parameter processing
				i = arg.length;
			} else {
				System.out.println("Unknown command line parameter: " + arg[i]);

				System.exit(1);
			}

		}

		Options options = Options.builder().servers(SERVER).build();

		 try (Connection nc = Nats.connect(options)) {
	            JetStream js = nc.jetStream();
	            JetStreamManagement jsm = nc.jetStreamManagement();
	            StreamContext streamContext = js.getStreamContext(STREAM);

	            long start = System.currentTimeMillis();

	            boolean running = true;
	            int count = 0;

	            MessageHandler handler = new Handler();

            	long batchStart = System.currentTimeMillis();
	            for ( int idx=STARTINDEX; idx<=STOPINDEX; idx++ )
	            {
	            	//Start OrderedConsumer
	            	ZonedDateTime startTime = ZonedDateTime.now().minusSeconds(RECOVER);

	            	String rawsubject = SUBJECTPREFIX+"."+idx;
	            	String repubsubject = "repub."+idx;

	            	Dispatcher dispatcher = nc.createDispatcher();

		            StreamBackedSubscriber listener = new StreamBackedSubscriber( streamContext, dispatcher, rawsubject, repubsubject, handler);
		            lastActiveTime[idx] = System.currentTimeMillis();
		            //listener.setTrace(true);

		            listener.start(startTime);

	            	Thread.sleep( STARTDELAY );
	            }


	            long stop = System.currentTimeMillis();
	            System.out.println( "Started in: " + ( (stop-start) / 1000 ) + " seconds");

	            Thread.sleep(1000000000);


		 } catch ( JetStreamApiException | IOException | InterruptedException ioe) {
			 	ioe.printStackTrace();

		 }
		 System.out.println("DONE");
	}
}
