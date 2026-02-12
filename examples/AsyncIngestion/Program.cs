using ScalePad.Databricks.Zerobus;

// Simple example demonstrating async ingestion patterns
Console.WriteLine("=== Zerobus Async Ingestion Example ===\n");

// Configuration
const string zerobusEndpoint = "http://localhost:50051";
const string unityCatalogEndpoint = "https://your-workspace.databricks.com";
const string tableName = "catalog.schema.events";

// Create SDK instance
using var sdk = new ZerobusSdk(zerobusEndpoint, unityCatalogEndpoint);

// Configure stream for JSON records
var tableProps = new TableProperties(tableName);
var options = StreamConfigurationOptions.Default with
{
    MaxInflightRequests = 100_000,
    RecordType = RecordType.Json,
};

// Create stream with headers provider
using var stream = sdk.CreateStreamWithHeadersProvider(
    tableProps,
    new SimpleHeadersProvider("your-token"),
    options);

Console.WriteLine("Stream created successfully\n");

// Example 1: Single async ingestion with await
Console.WriteLine("Example 1: Single async ingestion");
var record1 = "{\"id\": 1, \"event\": \"user_login\", \"timestamp\": \"2026-02-11T20:00:00Z\"}";
var offset1 = await stream.IngestRecordAsync(record1);
Console.WriteLine($"✓ Record ingested and acknowledged at offset {offset1}\n");

// Example 2: Concurrent async ingestion
Console.WriteLine("Example 2: Concurrent async ingestion (5 records)");
var tasks = Enumerable.Range(2, 5)
    .Select(i => stream.IngestRecordAsync($"{{\"id\": {i}, \"event\": \"page_view\", \"page\": \"/home\"}}"))
    .ToArray();

var offsets = await Task.WhenAll(tasks);
Console.WriteLine($"✓ All {offsets.Length} records ingested and acknowledged");
Console.WriteLine($"  Offsets: [{string.Join(", ", offsets)}]\n");

// Example 3: Batch async ingestion
Console.WriteLine("Example 3: Batch async ingestion");
string[] batch =
[
    "{\"id\": 100, \"event\": \"purchase\", \"amount\": 49.99}",
    "{\"id\": 101, \"event\": \"purchase\", \"amount\": 99.99}",
    "{\"id\": 102, \"event\": \"purchase\", \"amount\": 149.99}",
];

var batchOffset = await stream.IngestRecordsAsync(batch);
Console.WriteLine($"✓ Batch of {batch.Length} records ingested at offset {batchOffset}\n");

// Example 4: Async flush to ensure durability
Console.WriteLine("Example 4: Async flush");
await stream.FlushAsync();
Console.WriteLine("✓ All records flushed and durable\n");

// Example 5: With cancellation token
Console.WriteLine("Example 5: With cancellation token (5 second timeout)");
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
try
{
    var record = "{\"id\": 999, \"event\": \"timeout_test\"}";
    var offset = await stream.IngestRecordAsync(record, cts.Token);
    Console.WriteLine($"✓ Record ingested at offset {offset}\n");
}
catch (OperationCanceledException)
{
    Console.WriteLine("✗ Operation cancelled due to timeout\n");
}

Console.WriteLine("=== Example Complete ===");

// Simple headers provider implementation
public class SimpleHeadersProvider : IHeadersProvider
{
    private readonly string _token;

    public SimpleHeadersProvider(string token)
    {
        _token = token;
    }

    public IDictionary<string, string> GetHeaders()
    {
        return new Dictionary<string, string>
        {
            ["authorization"] = $"Bearer {_token}",
            ["x-databricks-zerobus-table-name"] = "events",
        };
    }
}
