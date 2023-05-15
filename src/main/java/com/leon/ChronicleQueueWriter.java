package com.leon;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import java.util.UUID;

public class ChronicleQueueWriter
{
    private ChronicleQueue queue;
    private ExcerptAppender appender;

    public static void main( String[] args )
    {
        System.out.println( "Creating chronicle queue..." );
        //BasicConfigurator.configure();
        try
        {
            ChronicleQueueWriter writer = new ChronicleQueueWriter();
            writer.write();
            writer.close();
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    public ChronicleQueueWriter()
    {
        queue = ChronicleQueue.singleBuilder("C:\\data\\chronicle-queue-input").blockSize(1024 * 1024).build();
        appender = queue.acquireAppender();
    }

    public void close()
    {
        System.out.println( "Sending END_OF_STREAM and closing chronicle queue..." );
        appender.writeDocument(wire -> wire.write(() -> "key").text("END_OF_STREAM"));
        queue.close();
    }

    public void write()
    {
        System.out.println( "Writing messages to chronicle queue..." );
        for(long count = 0; count < 100; ++count)
        {
            String json = String.format("CASH_CHECK_REQUEST={\"clientId\": %s, \"instrumentId\":  %d, \"requestType\":  \"CASH_CHECK_REQUEST\"}", UUID.randomUUID(), count);
            appender.writeDocument(wire -> wire.write(() -> "key").text(json));
        }
    }
}
