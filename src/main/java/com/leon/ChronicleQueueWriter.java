package com.leon;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.TimeZone;
import java.util.UUID;

public class ChronicleQueueWriter
{
    private ChronicleQueue queue;
    private ExcerptAppender appender;

    public static void main( String[] args )
    {
        System.out.println( "Creating chronicle queue..." );
        BasicConfigurator.configure();
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

    public ChronicleQueueWriter() throws IOException
    {
        String sample = String.format("CASH_CHECK_REQUEST={\"clientId\": %s, \"instrumentId\":  %d, \"requestType\":  \"CASH_CHECK_REQUEST\"}", UUID.randomUUID(), 1_000_000);
        queue = ChronicleQueue.singleBuilder("C:\\data\\chronicle-queue-input").blockSize(1024 * 1024).build();
        appender = queue.acquireAppender();
    }

    public void close()
    {
        queue.close();
    }

    public void write() throws Exception
    {
        System.out.println( "Writing messages to chronicle queue..." );
        for(long count = 100; count < 200; ++count)
        {
            String json = String.format("CASH_CHECK_REQUEST={\"clientId\": %s, \"instrumentId\":  %d, \"requestType\":  \"CASH_CHECK_REQUEST\"}", UUID.randomUUID(), count);
            appender.writeDocument(wire -> wire.write(() -> "key").text(json));
        }
    }
}
