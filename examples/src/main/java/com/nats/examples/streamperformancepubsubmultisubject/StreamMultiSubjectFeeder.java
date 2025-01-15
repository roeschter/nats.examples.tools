package com.nats.examples.streamperformancepubsubmultisubject;

import java.io.IOException;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.Republish;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

public class StreamMultiSubjectFeeder {
	public static String[] SERVER = new String[] { "nats://localhost:4222" };

	private static String STREAM = "stream-1";
	private static String SUBJECTPREFIX = "raw01";
	private static int STARTINDEX = 0;
	private static int STOPINDEX = 9999 ;
	private static long DELAY = 1000;
	private static boolean DELETESTREAM = true;
	private static int SIZE = 2000;
	private static int REPLICA = 3;


	public static String NATSMSGID = "Nats-Msg-Id";

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
				DELAY = Integer.parseInt( arg[i] );
				i++;
			} else if ( arg[i].equals("-size") )
			{
				i++;
				SIZE = Integer.parseInt( arg[i] );
				i++;
			} else if ( arg[i].equals("-keepstream") )
			{
				i++;
				DELETESTREAM = false;
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

	            if ( DELETESTREAM ) {
		            try {
		            	jsm.deleteStream(STREAM);
		            } catch (Exception e) {}

		            StreamConfiguration sc = StreamConfiguration.builder()
		                .name(STREAM)
		                .storageType(StorageType.File)
		                .subjects(SUBJECTPREFIX + ".>")
		                .replicas(REPLICA)
		                .maxBytes(10l * 1000000000l)
		                .republish( new Republish(SUBJECTPREFIX + ".*", "repub.{{wildcard(1)}}", false)  )
		                .build();

		            //Info
		            StreamInfo si;

		            si = jsm.addStream(sc);
		            System.out.println(si);
	            }

	            byte[] data = new byte[SIZE];
	            for( int n=0; n<data.length; n++ )
	            	data[n] = (byte)n;

	            long start = System.currentTimeMillis();

	            int[] deviceSequence = new int[STOPINDEX+1];

	            boolean running = true;
	            int count = 0;
	            while ( running ) {

	            	long batchStart = System.currentTimeMillis();
		            for ( int idx=STARTINDEX; idx<=STOPINDEX; idx++ )
		            {
		            	Headers headers = new Headers();

		            	String msgDd = idx + "." + deviceSequence[idx];
		            	deviceSequence[idx]++;
		            	headers.add( "Nats-Msg-Id", msgDd);
		            	headers.add( "id", ""+idx);
		            	headers.add( "seq", ""+deviceSequence[idx]);
		            	headers.add( "time", ""+System.currentTimeMillis());
		            	//Send
		            	Message msg = new NatsMessage( SUBJECTPREFIX + "." + idx, null,  headers, data );
		            	count++;

		            	js.publishAsync(msg);

		            }
		            long batchTime = System.currentTimeMillis() - batchStart;
		            long remainingdelay = DELAY - batchTime;
		            if (  remainingdelay > 0 ) {
		            	Thread.sleep( remainingdelay );
		            } else {
		            	System.out.println("Slow sender: " + (-remainingdelay));
		            }
	            }
	            long stop = System.currentTimeMillis();
	            System.out.println( "Count: " + count + " Message/s: " + ( count*1000 /(stop-start) ) );
	            //Rip down
	            /*
	            jsm.deleteStream(STREAM);
	            */


		 } catch ( JetStreamApiException | IOException | InterruptedException ioe) {
			 	ioe.printStackTrace();

		 }
		 System.out.println("DONE");
	}
}
