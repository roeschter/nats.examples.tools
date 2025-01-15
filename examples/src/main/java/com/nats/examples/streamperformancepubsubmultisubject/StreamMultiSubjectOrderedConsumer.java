package com.nats.examples.streamperformancepubsubmultisubject;

import java.io.IOException;
import java.time.ZonedDateTime;

import com.nats.examples.streambackedsubscriber.StreamBackedSubscriber;

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
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.api.Republish;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

public class StreamMultiSubjectOrderedConsumer {
	public static String[] SERVER = new String[] { "nats://localhost:4222" };

	private static String STREAM = "stream-1";
	private static String SUBJECTPREFIX = "raw01";
	private static int STARTINDEX = 0;
	private static int STOPINDEX = 999;
	private static long STARTDELAY = 30;
	private static long RECOVER = 0;


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

	public static class Handler implements MessageHandler {

		long[] lastseq = new long[STOPINDEX+1];
		volatile static int mcount = 0;
		public Handler() {

		}

		@Override
		public void onMessage(Message msg) throws InterruptedException {
			//String msgID = StreamBackedSubscriber.getFromHeader(msg, StreamBackedSubscriber.NATSMSGID);
			//System.out.println(msg);
			long seq = getFromHeaderLong(msg, "seq");
			long lid = getFromHeaderLong(msg, "id");
			long time = getFromHeaderLong(msg, "time");

			int id = (int)lid;
			if ( lastseq[id] != 0 && seq != lastseq[id]+1 ) {
				//System.out.println("**************************************************************************************");
				System.out.println("**** GAP - Expected " + (lastseq[id]+1) + " got " + seq + "   ************");
				//System.out.println("**************************************************************************************");
			}
			lastseq[id] = seq;
			long delay =  System.currentTimeMillis() - time;
			if ( delay> 1000 ) {
				System.out.println("**** Delayed data " + id  + " : " + delay + "   ************");
			}

			mcount++;
			if (( mcount % 1000 ) == 0 )
				System.out.println(mcount);

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

	            long start = System.currentTimeMillis();

	            boolean running = true;
	            int count = 0;

	            Dispatcher dispatcher = nc.createDispatcher();

	            MessageHandler handler = new Handler();

            	long batchStart = System.currentTimeMillis();
	            for ( int idx=STARTINDEX; idx<=STOPINDEX; idx++ )
	            {
	            	//Start OrderedConsumer
	            	ZonedDateTime startTime = ZonedDateTime.now().minusSeconds(RECOVER);

	            	String subject = SUBJECTPREFIX+"."+idx;
	            	OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration()
	            			.filterSubject(subject);

            		if ( RECOVER > 0 ) {
            			ocConfig = ocConfig.deliverPolicy(  DeliverPolicy.ByStartTime )
            					.startTime(startTime);
            		} else {
            			//If there is no filter, we deliver all
            			ocConfig = ocConfig.deliverPolicy( DeliverPolicy.New );
            		}

            		StreamContext streamContext = js.getStreamContext(STREAM);

            		OrderedConsumerContext oc = streamContext.createOrderedConsumer( ocConfig );


            		oc.consume(dispatcher, handler);
            		System.out.println( "OrderedConsumer: " + subject);

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
