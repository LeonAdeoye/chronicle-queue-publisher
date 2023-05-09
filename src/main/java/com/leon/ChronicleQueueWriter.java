package com.leon;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;


public class ChronicleQueueWriter
{
    private static Chronicle chronicle;

    public static void main( String[] args )
    {
        System.out.println( "Writing messages to chronicle queue..." );
        try
        {
            ChronicleQueueWriter writer = new ChronicleQueueWriter();
            writer.write();
            chronicle.close();
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    public ChronicleQueueWriter() throws IOException
    {
		/*There are three concepts characteristic of a Chronicle Queue:
		Excerpt – is a data container
		Appender – appender is used for writing data
		Tailer – is used for sequentially reading data
		We'll reserve the portion of memory for read-write operations using the Chronicle interface. */

        File queueDir = Files.createTempDirectory("chronicle-queue").toFile();
        chronicle = ChronicleQueueBuilder.indexed(queueDir).build();

        // We will need a base directory where the queue will persist records in memory-mapped files.
        // ChronicleQueueBuilder class provides different types of queues.
        // In this case, we used IndexedChronicleQueue, which uses the sequential index to maintain memory offsets of records in a queue.
    }

    public void write() throws Exception
    {
        // To write the items to a queue, we'll need to create an object of ExcerptAppender class using the Chronicle instance.
        //After creating the appender, we will start the appender using a startExcerpt method.
        // It starts an Excerpt with the default message capacity of 128K. We can use an overloaded version of startExcerpt to provide a custom capacity.
        //Once started, we can write any literal or object value to the queue using a wide range of write methods provided by the library.
        //Finally, when we're done with writing, we'll finish the excerpt, save the data to a queue, and later to the disc.
        ExcerptAppender appender = chronicle.createAppender();
        appender.startExcerpt(40_000_000);
        for(long count = 0; count < 200_000; ++count)
        {
            String value = String.format("CASH_CHECK_REQUEST={\"clientId\": %s, \"instrumentId\":  %d, \"requestType\":  \"CASH_CHECK_REQUEST\", \"lockCash\": 1, \"unlockCash\": 0}"
                    ,String.valueOf(UUID.randomUUID()), count);

            appender.writeUTF(value);
            System.out.println(value);
        }
        appender.finish();
    }
}
