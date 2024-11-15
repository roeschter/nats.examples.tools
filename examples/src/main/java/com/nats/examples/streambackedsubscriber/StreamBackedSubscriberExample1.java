package com.nats.examples.streambackedsubscriber;


import java.io.IOException;
import java.time.ZonedDateTime;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.StreamContext;
import io.nats.client.Message;
import io.nats.client.MessageHandler;

/*
 */

public class StreamBackedSubscriberExample1 {

public static String[] SERVER = new String[] { "nats://localhost:4222" };

	static final String STREAM = "stream_backed_subscriber";

	static long lastSeq = 0;
	static long deviceSeq = 0;

	public static long extractDeviceSeq( String s ) {
		int pos = s.lastIndexOf('.');
		return Integer.parseInt(s.substring(pos+1));
	}

	public static class Handler implements MessageHandler {

		@Override
		public void onMessage(Message msg) throws InterruptedException {
			String msgID = StreamBackedSubscriber.getFromHeader(msg, StreamBackedSubscriber.NATSMSGID);
			lastSeq = StreamBackedSubscriber.getID(msg);
			System.out.println( "Received: "
					+  msgID + " : "
					+ lastSeq );

			long currentDeviceSeq =extractDeviceSeq( msgID );
			if ( deviceSeq != 0 && currentDeviceSeq != deviceSeq+1 ) {
				System.out.println("**************************************************************************************");
				System.out.println("**** GAP - Expected " + (deviceSeq+1) + " got " + currentDeviceSeq + "   ************");
				System.out.println("**************************************************************************************");
			}
			deviceSeq = currentDeviceSeq;
		}

	}

	public static void main(String[] args) throws Exception {

		Options options = Options.builder().servers(SERVER).build();

		 try (Connection nc = Nats.connect(options)) {
	            JetStream js = nc.jetStream();
	            StreamContext ctx = js.getStreamContext(STREAM);

	            StreamBackedSubscriber listener = new StreamBackedSubscriber( ctx, nc, "ingest.devices.0", "repub", new Handler());
	            ZonedDateTime time = ZonedDateTime.now().minusSeconds(10);

	            System.out.println( "********* Starting with 10s window" );
	            listener.start(time);

	            //Run for 30 seconds
	            Thread.sleep(3000);

	            System.out.println( "********* Pausing " );
	            listener.pause();
	            Thread.sleep(10000);

	            System.out.println( "********* Starting where we left off but maximum 5s window" );
	            //Start again with a new 10s reover interval
	            time = ZonedDateTime.now().minusSeconds(5);
	            listener.start(time);

	            Thread.sleep(10000);

	            System.out.println( "********* Stopping" );
	            listener.stop();


		 } catch ( /*JetStreamApiException |*/ IOException | InterruptedException ioe) {
			 	ioe.printStackTrace();
	                // JetStreamApiException:
	                //      the stream or consumer did not exist
	                // IOException:
	                //      problem making the connection
	                // InterruptedException:
	                //      thread interruption in the body of the example
		 }
		 System.out.println("DONE");
	}

}