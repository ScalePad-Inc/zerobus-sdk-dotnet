using System.Diagnostics;
using Grpc.Core;
using NUnit.Framework;

namespace Databricks.Zerobus.IntegrationTests;

[TestFixture]
[Parallelizable(ParallelScope.Children)]
public class IntegrationTests
{
    private const string TestTableName = "test_catalog.test_schema.test_table";

    // ── Stream Creation ───────────────────────────────────────────────

    [Test]
    public async Task SuccessfulStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        Assert.That(stream, Is.Not.Null);
    }

    [Test]
    public async Task TimeoutedStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1", delayMs: 300),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            RecoveryTimeoutMs = 100,
            Recovery = false,
        };

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });

        // Give background tasks a moment to realize the timeout occurred.
        await Task.Delay(100);
    }

    [Test]
    public async Task NonRetriableErrorDuringStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unauthenticated, "Non-retriable error"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = true,
        };

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });
    }

    [Test]
    public async Task RetriableErrorWithoutRecoveryDuringStreamCreation()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.ErrorResponse(StatusCode.Unavailable, "Retriable error"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
            RecoveryTimeoutMs = 200,
            RecoveryBackoffMs = 200,
        };

        var sw = Stopwatch.StartNew();

        Assert.Throws<ZerobusException>(() =>
        {
            sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);
        });

        sw.Stop();

        // Should fail reasonably quickly without retry. Allow buffer for test environment variability.
        Assert.That(sw.ElapsedMilliseconds, Is.LessThan(1000),
            $"Expected reasonable failure time, but took {sw.ElapsedMilliseconds}ms");
    }

    // ── Close ─────────────────────────────────────────────────────────

    [Test]
    public async Task GracefulClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0, delayMs: 100),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offsetId = stream.IngestRecord(testRecord);

        Assert.That(offsetId, Is.EqualTo(0));

        stream.Close();

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IdempotentClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        // First close should succeed.
        stream.Close();

        // Second close should also succeed (idempotent).
        stream.Close();
    }

    [Test]
    public async Task IngestAfterClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        stream.Close();

        // Ingesting after close should throw.
        // Close() zeroes the native ptr, so the native layer returns an error.
        Assert.That(() => stream.IngestRecord("test record data"u8.ToArray()),
            Throws.InstanceOf<Exception>());
    }

    // ── Single Record Ingestion ───────────────────────────────────────

    [Test]
    public async Task IngestSingleRecord()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0), // Ack for offset 0
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offsetId = stream.IngestRecord(testRecord);

        Assert.That(offsetId, Is.EqualTo(0));

        // Give the server time to process.
        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    // ── Multiple Record Ingestion ─────────────────────────────────────

    [Test]
    public async Task IngestMultipleRecords()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0), // Ack for offset 0
            MockResponses.RecordAckResponse(1), // Ack for offset 1
            MockResponses.RecordAckResponse(2), // Ack for offset 2
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        const int numRecords = 3;
        for (var i = 0; i < numRecords; i++)
        {
            var testRecord = System.Text.Encoding.UTF8.GetBytes($"test record {i}");
            var offsetId = stream.IngestRecord(testRecord);

            Assert.That(offsetId, Is.EqualTo(i));
        }

        // Give the server time to process.
        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)numRecords));
        Assert.That(maxOffset, Is.EqualTo(numRecords - 1));
    }

    // ── Batch Record Ingestion ────────────────────────────────────────

    [Test]
    public async Task IngestBatchRecords()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0), // Ack for the batch at offset 0
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        byte[][] batch =
        [
            "record 1"u8.ToArray(),
            "record 2"u8.ToArray(),
            "record 3"u8.ToArray(),
            "record 4"u8.ToArray(),
            "record 5"u8.ToArray(),
        ];

        var offsetId = stream.IngestRecords(batch);

        Assert.That(offsetId, Is.EqualTo(0));

        // Give the server time to process.
        await Task.Delay(100);

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)batch.Length));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    // ── Batch Ingest After Close ──────────────────────────────────────

    [Test]
    public async Task IngestRecordsAfterClose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_batch_after_close"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        stream.Close();

        byte[][] batch = ["record 1"u8.ToArray(), "record 2"u8.ToArray()];

        Assert.That(() => stream.IngestRecords(batch),
            Throws.InstanceOf<Exception>());
    }

    // ── Async Single Record Ingestion ─────────────────────────────────

    [Test]
    public async Task IngestSingleRecordAsync_Json()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0), // Ack for offset 0
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        // No descriptor proto = JSON mode
        var tableProps = new TableProperties(TestTableName);

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
            RecordType = RecordType.Json,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var testRecord = "{\"id\": 1, \"message\": \"test\"}";
        var offset = await stream.IngestRecordAsync(testRecord);

        Assert.That(offset, Is.EqualTo(0));

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestSingleRecordAsync_Proto_ByteArray()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        var testRecord = "test record data"u8.ToArray();
        var offset = await stream.IngestRecordAsync(testRecord);

        Assert.That(offset, Is.EqualTo(0));

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestSingleRecordAsync_Proto_ReadOnlyMemory()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        ReadOnlyMemory<byte> testRecord = "test record data"u8.ToArray();
        var offset = await stream.IngestRecordAsync(testRecord);

        Assert.That(offset, Is.EqualTo(0));

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(1));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    // ── Async Multiple Record Ingestion ───────────────────────────────

    [Test]
    public async Task IngestMultipleRecordsAsync_Concurrent()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
            MockResponses.RecordAckResponse(1),
            MockResponses.RecordAckResponse(2),
            MockResponses.RecordAckResponse(3),
            MockResponses.RecordAckResponse(4),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        const int numRecords = 5;

        // Concurrently ingest multiple records
        var tasks = Enumerable.Range(0, numRecords)
            .Select(i => stream.IngestRecordAsync(System.Text.Encoding.UTF8.GetBytes($"test record {i}")))
            .ToArray();

        var offsets = await Task.WhenAll(tasks);

        // Offsets should be 0-4 (order may vary due to concurrency)
        Assert.That(offsets.Distinct().Count(), Is.EqualTo(numRecords));
        Assert.That(offsets.Min(), Is.EqualTo(0));
        Assert.That(offsets.Max(), Is.EqualTo(numRecords - 1));

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)numRecords));
        Assert.That(maxOffset, Is.EqualTo(numRecords - 1));
    }

    // ── Async Batch Record Ingestion ──────────────────────────────────

    [Test]
    public async Task IngestRecordsAsync_JsonBatch()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        // No descriptor proto = JSON mode
        var tableProps = new TableProperties(TestTableName);

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
            RecordType = RecordType.Json,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        string[] batch =
        [
            "{\"device\": \"sensor-001\", \"temp\": 20}",
            "{\"device\": \"sensor-002\", \"temp\": 21}",
            "{\"device\": \"sensor-003\", \"temp\": 22}",
        ];

        var offset = await stream.IngestRecordsAsync(batch);

        Assert.That(offset, Is.EqualTo(0));

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)batch.Length));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestRecordsAsync_ProtoBatch()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        byte[][] batch =
        [
            "record 1"u8.ToArray(),
            "record 2"u8.ToArray(),
            "record 3"u8.ToArray(),
            "record 4"u8.ToArray(),
            "record 5"u8.ToArray(),
        ];

        var offset = await stream.IngestRecordsAsync(batch);

        Assert.That(offset, Is.EqualTo(0));

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo((ulong)batch.Length));
        Assert.That(maxOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task IngestRecordsAsync_EmptyBatch()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        // No descriptor proto = JSON mode
        var tableProps = new TableProperties(TestTableName);

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
            RecordType = RecordType.Json,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        string[] emptyBatch = [];

        var offset = await stream.IngestRecordsAsync(emptyBatch);

        // Empty batch should return -1
        Assert.That(offset, Is.EqualTo(-1));
    }

    // ── Async Wait and Flush ──────────────────────────────────────────

    [Test]
    public async Task WaitForOffsetAsync()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0, delayMs: 50),
            MockResponses.RecordAckResponse(1, delayMs: 50),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        // Ingest synchronously to get offsets quickly
        var offset0 = stream.IngestRecord("record 0"u8.ToArray());
        var offset1 = stream.IngestRecord("record 1"u8.ToArray());

        Assert.That(offset0, Is.EqualTo(0));
        Assert.That(offset1, Is.EqualTo(1));

        // Wait for offset 1 asynchronously (should also ensure offset 0 is acked)
        await stream.WaitForOffsetAsync(offset1);

        var maxOffset = fixture.MockServer.GetMaxOffsetSent();
        Assert.That(maxOffset, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task FlushAsync()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
            MockResponses.RecordAckResponse(0, delayMs: 30),
            MockResponses.RecordAckResponse(1, delayMs: 30),
            MockResponses.RecordAckResponse(2, delayMs: 30),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        // Ingest multiple records synchronously
        stream.IngestRecord("record 0"u8.ToArray());
        stream.IngestRecord("record 1"u8.ToArray());
        stream.IngestRecord("record 2"u8.ToArray());

        // Flush asynchronously
        await stream.FlushAsync();

        var writeCount = fixture.MockServer.GetWriteCount();
        var maxOffset = fixture.MockServer.GetMaxOffsetSent();

        Assert.That(writeCount, Is.EqualTo(3));
        Assert.That(maxOffset, Is.EqualTo(2));
    }

    // ── Async Cancellation ────────────────────────────────────────────

    [Test]
    public async Task IngestRecordAsync_PreCancelled()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Should throw TaskCanceledException (subclass of OperationCanceledException)
        Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await stream.IngestRecordAsync("test record"u8.ToArray(), cts.Token);
        });
    }

    [Test]
    public async Task FlushAsync_PreCancelled()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        using var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Should throw TaskCanceledException (subclass of OperationCanceledException)
        Assert.ThrowsAsync<TaskCanceledException>(async () =>
        {
            await stream.FlushAsync(cts.Token);
        });
    }

    // ── Async Error Handling ──────────────────────────────────────────

    [Test]
    public async Task IngestRecordAsync_AfterDispose()
    {
        await using var fixture = await MockServerFixture.StartAsync();

        fixture.MockServer.InjectResponses(TestTableName,
        [
            MockResponses.CreateStreamResponse("test_stream_1"),
        ]);

        using var sdk = new ZerobusSdk(fixture.ServerUrl, "https://mock-uc.com");

        var tableProps = new TableProperties(TestTableName, TestDescriptor.CreateTestDescriptorProto());

        var options = StreamConfigurationOptions.Default with
        {
            MaxInflightRequests = 100,
            Recovery = false,
        };

        var stream = sdk.CreateStreamWithHeadersProvider(tableProps, new TestHeadersProvider(), options);

        stream.Dispose();

        // Should throw ObjectDisposedException
        Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await stream.IngestRecordAsync("test record");
        });
    }
}
