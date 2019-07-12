package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.jupiter.MockitoExtension;


import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("ALL")
@ExtendWith(MockitoExtension.class)
class KclRecordProcessorTests {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private String shardId;
    private KclRecordProcessor processor;
    private ArrayBlockingQueue<KclRecordsWrapper> queue;
    private ConcurrentHashMap<String, ShardInfo> shardRegister;
    private Clock clock;


    @BeforeEach
    void init() {
        shardId = "testShardId1";

        queue = new ArrayBlockingQueue<>(2);
        shardRegister = new ConcurrentHashMap<>();
        clock = Clock.fixed(Instant.parse("2001-01-01T00:00:00.00Z"), ZoneId.of("UTC"));

        processor = new KclRecordProcessor("testTableName", queue, shardRegister, clock);

        InitializationInput initializationInput = new InitializationInput().withShardId(shardId);
        processor.initialize(initializationInput);
    }

    private ProcessRecordsInput getProcessRecordsInput(String sequenceNumber) {
        List<Record> records = Collections.singletonList(new Record().withSequenceNumber(sequenceNumber));
        return new ProcessRecordsInput().withRecords(records);
    }

    @Test
    void initializationRegistersNewShardToRegistry() {
        // Arrange
        String shardId = "testShardId2";
        InitializationInput initializationInput = new InitializationInput().withShardId(shardId);

        // Act
        processor.initialize(initializationInput);

        // Assert
        assertTrue(shardRegister.containsKey(shardId));
        assertEquals(shardId, shardRegister.get(shardId).getShardId());
        assertEquals("", shardRegister.get(shardId).getLastCommittedRecordSeqNo());
    }

    @Test
    void processRecordsPutsRecordsToQueue() throws InterruptedException {
        // Arrange
        ProcessRecordsInput processRecordsInput = getProcessRecordsInput("SQ1");

        // Act
        processor.processRecords(processRecordsInput);
        KclRecordsWrapper received = queue.poll(1, TimeUnit.SECONDS);

        // Assert
        assertEquals(shardId, received.getShardId());
        assertSame(processRecordsInput.getRecords(), received.getRecords());
    }

    @Test
    void processRecordsDoesNothingIfNoRecordsReturned() throws InterruptedException {
        // Arrange
        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput();

        // Act
        processor.processRecords(processRecordsInput);
        KclRecordsWrapper received = queue.poll(1, TimeUnit.SECONDS);

        // Assert
        assertNull(received);
    }

    @Test
    void processRecordsBlocksAndWaitsIfQueueIsFull() throws InterruptedException {
        // Arrange
        ProcessRecordsInput processRecordsInput1 = getProcessRecordsInput("SQ1");
        ProcessRecordsInput processRecordsInput2 = getProcessRecordsInput("SQ2");
        ProcessRecordsInput processRecordsInput3 = getProcessRecordsInput("SQ3");

        // Act
        executor.execute(() -> {
            processor.processRecords(processRecordsInput1);
            processor.processRecords(processRecordsInput2);
            // Gets blocked until thirst one/two are  consumed
            processor.processRecords(processRecordsInput3);
        });

        // Assert
        while (queue.size() < 2) {  // White while queue becomes full
            Thread.sleep(100);
        }

        queue.poll(); // Take first two records out
        queue.poll();

        KclRecordsWrapper lastRecord = queue.poll(5, TimeUnit.SECONDS); // Wait for last one to arrive
        assertSame(processRecordsInput3.getRecords(), lastRecord.getRecords());
    }

    @Test
    void processRecordsStopsTryingToPutRecordIntoFullQueueIfShutdownIsRequested() throws InterruptedException {
        // Arrange
        ProcessRecordsInput processRecordsInput1 = getProcessRecordsInput("SQ1");
        ProcessRecordsInput processRecordsInput2 = getProcessRecordsInput("SQ2");
        ProcessRecordsInput processRecordsInput3 = getProcessRecordsInput("SQ3");

        AtomicBoolean blockedProccesorFinished = new AtomicBoolean(false);

        // Act & Assert
        executor.execute(() -> {
            processor.processRecords(processRecordsInput1);
            processor.processRecords(processRecordsInput2);
            // Gets blocked until thirst one/two are consumed or shutdown is requested
            processor.processRecords(processRecordsInput3);
            blockedProccesorFinished.set(true);
        });


        while (queue.size() < 2) {  // White while queue becomes full
            Thread.sleep(100);
        }

        // Give some time for third record bo be blocked...
        Thread.sleep(200);
        assertFalse(blockedProccesorFinished.get());

        // Requesting shutdown
        ShutdownInput shutdownInput = new ShutdownInput().withShutdownReason(ShutdownReason.ZOMBIE);
        processor.shutdown(shutdownInput);

        // processRecords gets unblocked without pushing recrod into queue
        Thread.sleep(200);
        assertTrue(blockedProccesorFinished.get());
    }

    @Test
    void processRecordsCheckpointsIfDefinedIntervalIsExceeded() throws InvalidStateException, ShutdownException, NoSuchFieldException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        ProcessRecordsInput processRecordsInput1 = getProcessRecordsInput("SQ1").withCheckpointer(checkpointer);
        ProcessRecordsInput processRecordsInput2 = getProcessRecordsInput("SQ2").withCheckpointer(checkpointer);

        // Act

        setClockForProcessor(processor, Clock.offset(clock, Duration.ofSeconds(1)));
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("SQ1"); // Simulate instantaneous commit to Kafka
        processor.processRecords(processRecordsInput1);

        // Assert
        verify(checkpointer, never()).checkpoint(ArgumentMatchers.anyString());

        // Act
        setClockForProcessor(processor, Clock.offset(clock, Duration.ofSeconds(15)));
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("SQ2"); // Simulate instantaneous commit to Kafka
        processor.processRecords(processRecordsInput2);

        // Assert
        verify(checkpointer, only()).checkpoint(ArgumentMatchers.eq("SQ2"));
    }

    @Test
    void processRecordsCheckpointsSequenceNumberOfTheLastRecordCommittedToKafkaFromThisShard() throws InvalidStateException, ShutdownException, NoSuchFieldException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        ProcessRecordsInput processRecordsInput = getProcessRecordsInput("SQ1").withCheckpointer(checkpointer);
        setClockForProcessor(processor, Clock.offset(clock, Duration.ofSeconds(15)));
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("SQ1");

        // Act
        processor.processRecords(processRecordsInput);

        // Assert
        verify(checkpointer, only()).checkpoint(ArgumentMatchers.eq("SQ1"));
    }

    @Test
    void processRecordsCheckpointsEventIfThereIsNoNewRecordsReturned() throws InvalidStateException, ShutdownException, NoSuchFieldException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput().withCheckpointer(checkpointer);
        setClockForProcessor(processor, Clock.offset(clock, Duration.ofSeconds(15)));
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("SQ1");

        // Act
        processor.processRecords(processRecordsInput);

        // Assert
        verify(checkpointer, only()).checkpoint(ArgumentMatchers.eq("SQ1"));
    }

    @Test
    void processRecordsSkipsCheckpointingIfNoRecordsCommittedToKafkaFromThisShard() throws InvalidStateException, ShutdownException, NoSuchFieldException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        ProcessRecordsInput processRecordsInput = getProcessRecordsInput("SQ1").withCheckpointer(checkpointer);
        setClockForProcessor(processor, Clock.offset(clock, Duration.ofSeconds(15)));
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("");

        // Act
        processor.processRecords(processRecordsInput);

        // Assert
        verify(checkpointer, never()).checkpoint(ArgumentMatchers.eq("SQ2"));
    }

    @Test
    void shutdownWaitsForLastRecordToBeCommittedOnShardEndBeforeCheckpoint() throws InvalidStateException, ShutdownException, InterruptedException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        ProcessRecordsInput processRecordsInput = getProcessRecordsInput("SQ1").withCheckpointer(checkpointer);

        // Act
        processor.processRecords(processRecordsInput);
        executor.execute(() -> {
            ShutdownInput shutdownInput = new ShutdownInput()
                    .withShutdownReason(ShutdownReason.TERMINATE)
                    .withCheckpointer(checkpointer);

            processor.shutdown(shutdownInput);
        });

        // Assert
        verify(checkpointer, never()).checkpoint(); // Still waiting for commit

        // Act
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("SQ1"); // Committing
        while (shardRegister.containsKey(shardId)) {
            Thread.sleep(100);
        }

        // Assert
        verify(checkpointer, only()).checkpoint();
    }

        @Test
    void shutdownSucceddsIfCalledMultipleTimes() throws InvalidStateException, ShutdownException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        ProcessRecordsInput processRecordsInput = getProcessRecordsInput("SQ1").withCheckpointer(checkpointer);
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("SQ1");

        // Act
        processor.processRecords(processRecordsInput);
        ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.TERMINATE)
                .withCheckpointer(checkpointer);

        processor.shutdown(shutdownInput);
        processor.shutdown(shutdownInput);

        // Assert
        verify(checkpointer, times(2)).checkpoint();
        assertFalse(shardRegister.containsKey(shardId));
    }

    @Test
    void shutdownCheckpointsIfNoDataWasReceivedFromThisShard() throws InvalidStateException, ShutdownException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);

        // Act
        ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.TERMINATE)
                .withCheckpointer(checkpointer);

        processor.shutdown(shutdownInput);

        // Assert
        verify(checkpointer, only()).checkpoint();
        assertFalse(shardRegister.containsKey(shardId));
    }

    @Test
    void shutdownRemovesShardFromRegisterOnZombie() throws InvalidStateException, ShutdownException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        ShutdownInput shutdownInput = new ShutdownInput()
                .withShutdownReason(ShutdownReason.ZOMBIE)
                .withCheckpointer(checkpointer);

        // Act
        processor.shutdown(shutdownInput);

        // Assert
        verify(checkpointer, never()).checkpoint();
        assertFalse(shardRegister.containsKey(shardId));
    }

    @Test
    void gracefullShutdownCheckpointsLastCommittedSequenceNumberOnWorkerShutdown() throws InvalidStateException, ShutdownException {
        // Arrange
        IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
        shardRegister.get(shardId).setLastCommittedRecordSeqNo("SQ1");

        // Act
        processor.shutdownRequested(checkpointer);

        // Assert
        verify(checkpointer, only()).checkpoint(ArgumentMatchers.eq("SQ1"));
        assertFalse(shardRegister.containsKey(shardId));
    }

    private void setClockForProcessor(KclRecordProcessor processor, Clock newClock) throws NoSuchFieldException {
        FieldSetter
                .setField(
                        processor,
                        processor.getClass().getDeclaredField("clock"),
                        newClock
                );
    }
}