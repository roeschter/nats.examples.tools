package com.nats.examples.streambackedsubscriber;

import java.io.IOException;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.Republish;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.Message;

/*
 * Setup and publish
 *
 */

public class StreamBackedSubscriberSampleFeeder {

public static String[] SERVER = new String[] { "nats://localhost:4222" };

	private static final String STREAM = "stream_backed_subscriber";

	public static void main(String[] args) throws Exception {

		Options options = Options.builder().servers(SERVER).build();

		 try (Connection nc = Nats.connect(options)) {
	            JetStream js = nc.jetStream();
	            JetStreamManagement jsm = nc.jetStreamManagement();

	            try {
	            	jsm.deleteStream(STREAM);
	            } catch (Exception e) {}

	            StreamConfiguration sc = StreamConfiguration.builder()
	                .name(STREAM)
	                .storageType(StorageType.File)
	                .subjects("ingest.devices.*")
	                .replicas(1)
	                .republish( new Republish("ingest.devices.*", "repub.ingest.devices.{{wildcard(1)}}", false)  )
	                .build();

	            //Info
	            StreamInfo si;

	            si = jsm.addStream(sc);
	            System.out.println(si);

	            byte[] data = new byte[512];
	            for( int i=0; i<data.length; i++ )
	            	data[i] = 32;

	            int deviceCount = 1;
	            int count = 1000000;
	            long delay = 1000;  //Ticke rate per device
	            int nextDevice = 0;
	            int[] deviceSequence = new int[deviceCount];

	            long start = System.currentTimeMillis();
	            long session = start;
	            for ( int i=1; i<=count; i++ )
	            {

	            	//Header with device sequence
	            	//The stream back subsriber relies on a Msg-Id for additional deduplication
	            	Headers headers = new Headers();
	            	deviceSequence[nextDevice]++;
	            	String msgDd = nextDevice + "." + session + "." +deviceSequence[nextDevice];
	            	headers.add( "Nats-Msg-Id", msgDd);

	            	//Send
	            	Message msg = new NatsMessage("ingest.devices."+nextDevice, null, headers, data);
	            	boolean retry = true;
	            	while ( retry) {
		            	try {
		            		js.publish(msg);
		            		retry = false;
		            	} catch ( Exception e ) {
		            		System.out.println("Retrying: " + msgDd );
		            	}
	            	}

	            	nextDevice = (nextDevice+1) % deviceCount;
	            	Thread.sleep( delay / deviceCount);
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