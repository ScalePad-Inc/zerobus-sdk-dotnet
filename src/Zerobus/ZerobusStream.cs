using System.Runtime.InteropServices;
using ScalePad.Databricks.Zerobus.Native;

namespace ScalePad.Databricks.Zerobus;

/// <summary>
/// Represents an active bidirectional gRPC stream for ingesting records.
/// Records can be ingested concurrently and will be acknowledged asynchronously.
/// </summary>
/// <remarks>
/// <para>
/// The stream is thread-safe — you may call <see cref="IngestRecord(string)"/> from
/// multiple threads concurrently, just like the Go SDK supports goroutines.
/// </para>
/// <para>
/// Always dispose the stream when finished. <see cref="Dispose"/> will flush
/// all pending records before closing. If you need to check for unacknowledged
/// records after a failure, call <see cref="GetUnackedRecords"/> before disposing.
/// </para>
/// </remarks>
public sealed class ZerobusStream : IDisposable
{
    private IntPtr _ptr;
    private bool _disposed;

    // Prevent the GCHandle / delegate from being collected while the native code holds a reference.
    // Not readonly: GCHandle is not a readonly struct, so calling Free() on a readonly field
    // creates a defensive copy — the field is never actually mutated, causing double-free.
    private GCHandle _bridgeHandle;
    private readonly HeadersProviderCallback? _callbackRef;

    internal ZerobusStream(IntPtr ptr)
    {
        _ptr = ptr;
    }

    internal ZerobusStream(IntPtr ptr, GCHandle bridgeHandle, HeadersProviderCallback callbackRef)
    {
        _ptr = ptr;
        _bridgeHandle = bridgeHandle;
        _callbackRef = callbackRef;
    }

    // ── Single-record ingestion ──────────────────────────────────────────

    /// <summary>
    /// Ingests a single record and returns the offset.
    /// This is the primary API for record ingestion.
    /// </summary>
    /// <param name="payload">
    /// The record payload. Pass a <see cref="string"/> for JSON records
    /// or a <c>byte[]</c> / <see cref="ReadOnlySpan{T}"/> of <see cref="byte"/>
    /// for Protocol Buffer records.
    /// </param>
    /// <returns>The offset of the ingested record.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown if the payload type is not <c>string</c> or <c>byte[]</c>.
    /// </exception>
    /// <example>
    /// <code>
    /// // JSON
    /// long offset = stream.IngestRecord("{\"id\": 1, \"message\": \"Hello\"}");
    ///
    /// // Protobuf
    /// byte[] protoBytes = SerializeMyProto(myMessage);
    /// long offset = stream.IngestRecord(protoBytes);
    /// </code>
    /// </example>
    public long IngestRecord(string payload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(payload);

        return NativeInterop.StreamIngestJsonRecord(_ptr, payload);
    }

    /// <inheritdoc cref="IngestRecord(string)"/>
    public long IngestRecord(byte[] payload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(payload);

        return NativeInterop.StreamIngestProtoRecord(_ptr, payload);
    }

    /// <inheritdoc cref="IngestRecord(string)"/>
    public long IngestRecord(ReadOnlySpan<byte> payload)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return NativeInterop.StreamIngestProtoRecord(_ptr, payload);
    }

    // ── Single-record async ingestion ────────────────────────────────────

    /// <summary>
    /// Asynchronously ingests a single record, waits for acknowledgment, and returns the offset.
    /// This method will not block the calling thread while waiting for the server to acknowledge the record.
    /// </summary>
    /// <param name="payload">
    /// The record payload. Pass a <see cref="string"/> for JSON records
    /// or a <c>byte[]</c> / <see cref="ReadOnlyMemory{T}"/> of <see cref="byte"/>
    /// for Protocol Buffer records.
    /// </param>
    /// <param name="cancellationToken">
    /// A cancellation token to observe while waiting for acknowledgment. Note that cancellation
    /// does not prevent the record from being ingested — it only stops waiting for acknowledgment.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// the offset of the ingested record after it has been acknowledged by the server.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if ingestion or acknowledgment fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown if the cancellation token is triggered before acknowledgment completes.
    /// </exception>
    /// <example>
    /// <code>
    /// // JSON
    /// long offset = await stream.IngestRecordAsync("{\"id\": 1, \"message\": \"Hello\"}");
    ///
    /// // Protobuf
    /// byte[] protoBytes = SerializeMyProto(myMessage);
    /// long offset = await stream.IngestRecordAsync(protoBytes);
    ///
    /// // With cancellation
    /// var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
    /// long offset = await stream.IngestRecordAsync(data, cts.Token);
    /// </code>
    /// </example>
    public Task<long> IngestRecordAsync(string payload, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(payload);

        return Task.Run(() =>
        {
            long offset = NativeInterop.StreamIngestJsonRecord(_ptr, payload);
            cancellationToken.ThrowIfCancellationRequested();
            NativeInterop.StreamWaitForOffset(_ptr, offset);
            return offset;
        }, cancellationToken);
    }

    /// <inheritdoc cref="IngestRecordAsync(string, CancellationToken)"/>
    public Task<long> IngestRecordAsync(byte[] payload, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(payload);

        return Task.Run(() =>
        {
            long offset = NativeInterop.StreamIngestProtoRecord(_ptr, payload);
            cancellationToken.ThrowIfCancellationRequested();
            NativeInterop.StreamWaitForOffset(_ptr, offset);
            return offset;
        }, cancellationToken);
    }

    /// <inheritdoc cref="IngestRecordAsync(string, CancellationToken)"/>
    public Task<long> IngestRecordAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return Task.Run(() =>
        {
            long offset = NativeInterop.StreamIngestProtoRecord(_ptr, payload.Span);
            cancellationToken.ThrowIfCancellationRequested();
            NativeInterop.StreamWaitForOffset(_ptr, offset);
            return offset;
        }, cancellationToken);
    }

    // ── Batch ingestion ──────────────────────────────────────────────────

    /// <summary>
    /// Ingests a batch of JSON records and returns one offset for the entire batch.
    /// All records in the batch must be JSON strings.
    /// </summary>
    /// <param name="records">The JSON record strings to ingest.</param>
    /// <returns>The offset representing the entire batch, or -1 if the batch is empty.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// string[] records =
    /// [
    ///     "{\"device\": \"sensor-001\", \"temp\": 20}",
    ///     "{\"device\": \"sensor-002\", \"temp\": 21}",
    /// ];
    /// long batchOffset = stream.IngestRecords(records);
    /// </code>
    /// </example>
    public long IngestRecords(string[] records)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(records);

        return NativeInterop.StreamIngestJsonRecords(_ptr, records);
    }

    /// <summary>
    /// Ingests a batch of protobuf records and returns one offset for the entire batch.
    /// All records in the batch must be serialised protobuf byte arrays.
    /// </summary>
    /// <param name="records">The protobuf record byte spans to ingest.</param>
    /// <returns>The offset representing the entire batch, or -1 if the batch is empty.</returns>
    /// <exception cref="ZerobusException">Thrown if ingestion fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    public long IngestRecords(byte[][] records)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(records);

        return NativeInterop.StreamIngestProtoRecords(_ptr, records);
    }

    // ── Batch async ingestion ────────────────────────────────────────────

    /// <summary>
    /// Asynchronously ingests a batch of JSON records, waits for acknowledgment,
    /// and returns the offset for the entire batch.
    /// All records in the batch must be JSON strings.
    /// </summary>
    /// <param name="records">The JSON record strings to ingest.</param>
    /// <param name="cancellationToken">
    /// A cancellation token to observe while waiting for acknowledgment.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// the offset representing the entire batch, or -1 if the batch is empty.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if ingestion or acknowledgment fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown if the cancellation token is triggered before acknowledgment completes.
    /// </exception>
    /// <example>
    /// <code>
    /// string[] records =
    /// [
    ///     "{\"device\": \"sensor-001\", \"temp\": 20}",
    ///     "{\"device\": \"sensor-002\", \"temp\": 21}",
    /// ];
    /// long batchOffset = await stream.IngestRecordsAsync(records);
    /// </code>
    /// </example>
    public Task<long> IngestRecordsAsync(string[] records, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(records);

        return Task.Run(() =>
        {
            long offset = NativeInterop.StreamIngestJsonRecords(_ptr, records);
            if (offset >= 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                NativeInterop.StreamWaitForOffset(_ptr, offset);
            }
            return offset;
        }, cancellationToken);
    }

    /// <summary>
    /// Asynchronously ingests a batch of protobuf records, waits for acknowledgment,
    /// and returns the offset for the entire batch.
    /// All records in the batch must be serialised protobuf byte arrays.
    /// </summary>
    /// <param name="records">The protobuf record byte arrays to ingest.</param>
    /// <param name="cancellationToken">
    /// A cancellation token to observe while waiting for acknowledgment.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// the offset representing the entire batch, or -1 if the batch is empty.
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if ingestion or acknowledgment fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown if the cancellation token is triggered before acknowledgment completes.
    /// </exception>
    public Task<long> IngestRecordsAsync(byte[][] records, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(records);

        return Task.Run(() =>
        {
            long offset = NativeInterop.StreamIngestProtoRecords(_ptr, records);
            if (offset >= 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                NativeInterop.StreamWaitForOffset(_ptr, offset);
            }
            return offset;
        }, cancellationToken);
    }

    // ── Acknowledgment / flush ───────────────────────────────────────────

    /// <summary>
    /// Blocks until the server acknowledges the record at the specified offset.
    /// Use this with offsets returned from <see cref="IngestRecord(string)"/> to wait for
    /// specific records to be durably written without waiting for all pending records.
    /// </summary>
    /// <param name="offset">The offset to wait for.</param>
    /// <exception cref="ZerobusException">Thrown if the wait fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// long offset = stream.IngestRecord(data);
    /// // ... do other work ...
    /// stream.WaitForOffset(offset);
    /// </code>
    /// </example>
    public void WaitForOffset(long offset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        NativeInterop.StreamWaitForOffset(_ptr, offset);
    }

    /// <summary>
    /// Blocks until all pending records have been acknowledged by the server.
    /// This ensures durability guarantees before proceeding.
    /// </summary>
    /// <exception cref="ZerobusException">
    /// Thrown if the flush times out or a record fails with a non-retryable error.
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// stream.Flush();
    /// Console.WriteLine("All records durably stored.");
    /// </code>
    /// </example>
    public void Flush()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        NativeInterop.StreamFlush(_ptr);
    }

    /// <summary>
    /// Asynchronously waits until the server acknowledges the record at the specified offset.
    /// This method will not block the calling thread while waiting.
    /// </summary>
    /// <param name="offset">The offset to wait for.</param>
    /// <param name="cancellationToken">
    /// A cancellation token to observe while waiting for acknowledgment.
    /// </param>
    /// <returns>A task that represents the asynchronous wait operation.</returns>
    /// <exception cref="ZerobusException">Thrown if the wait fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown if the cancellation token is triggered before acknowledgment completes.
    /// </exception>
    /// <example>
    /// <code>
    /// long offset = stream.IngestRecord(data);
    /// // ... do other work ...
    /// await stream.WaitForOffsetAsync(offset);
    /// </code>
    /// </example>
    public Task WaitForOffsetAsync(long offset, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Task.Run(() => NativeInterop.StreamWaitForOffset(_ptr, offset), cancellationToken);
    }

    /// <summary>
    /// Asynchronously waits until all pending records have been acknowledged by the server.
    /// This ensures durability guarantees before proceeding without blocking the calling thread.
    /// </summary>
    /// <param name="cancellationToken">
    /// A cancellation token to observe while waiting for acknowledgment.
    /// </param>
    /// <returns>A task that represents the asynchronous flush operation.</returns>
    /// <exception cref="ZerobusException">
    /// Thrown if the flush times out or a record fails with a non-retryable error.
    /// </exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <exception cref="OperationCanceledException">
    /// Thrown if the cancellation token is triggered before flush completes.
    /// </exception>
    /// <example>
    /// <code>
    /// await stream.FlushAsync();
    /// Console.WriteLine("All records durably stored.");
    /// </code>
    /// </example>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Task.Run(() => NativeInterop.StreamFlush(_ptr), cancellationToken);
    }

    // ── Unacknowledged records ───────────────────────────────────────────

    /// <summary>
    /// Retrieves all records that have not yet been acknowledged by the server.
    /// <para>
    /// <strong>Important:</strong> This should only be called after the stream has
    /// closed or failed. Calling it on an active stream will return an error.
    /// </para>
    /// </summary>
    /// <returns>
    /// An array where each element is either a <see cref="string"/> (JSON)
    /// or a <c>byte[]</c> (protobuf).
    /// </returns>
    /// <exception cref="ZerobusException">Thrown if retrieval fails.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the stream has been disposed.</exception>
    /// <example>
    /// <code>
    /// try
    /// {
    ///     stream.Flush();
    /// }
    /// catch (ZerobusException)
    /// {
    ///     var unacked = stream.GetUnackedRecords();
    ///     Console.WriteLine($"Failed to acknowledge {unacked.Length} records");
    /// }
    /// </code>
    /// </example>
    public object[] GetUnackedRecords()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return NativeInterop.StreamGetUnackedRecords(_ptr);
    }

    // ── Close / Dispose ──────────────────────────────────────────────────

    /// <summary>
    /// Gracefully closes the stream after flushing all pending records.
    /// The stream cannot be used after calling this method.
    /// </summary>
    /// <remarks>
    /// This is automatically called by <see cref="Dispose"/>, but you may call
    /// it explicitly if you need to inspect the close error.
    /// </remarks>
    /// <exception cref="ZerobusException">
    /// Thrown if flush or close fails.
    /// </exception>
    public void Close()
    {
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr == IntPtr.Zero) return;

        GC.SuppressFinalize(this);

        try
        {
            NativeInterop.StreamClose(ptr);
        }
        finally
        {
            NativeMethods.StreamFree(ptr);
            FreeBridgeHandle();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        GC.SuppressFinalize(this);

        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr != IntPtr.Zero)
        {
            try
            {
                NativeInterop.StreamClose(ptr);
            }
            catch
            {
                // Suppress during dispose — users should call Close() explicitly
                // if they need to observe the error.
            }
            finally
            {
                NativeMethods.StreamFree(ptr);
                FreeBridgeHandle();
            }
        }
        else
        {
            FreeBridgeHandle();
        }
    }

    /// <summary>
    /// Safety-net release of native memory for leaked instances.
    /// Only frees memory — does NOT call <see cref="NativeInterop.StreamClose"/>,
    /// which performs blocking gRPC I/O and may trigger managed callbacks on
    /// Rust-created threads that corrupt the .NET execution context
    /// (infinite recursion in <c>CultureInfo.CurrentUICulture</c>).
    /// </summary>
    ~ZerobusStream()
    {
        var ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
        if (ptr != IntPtr.Zero)
        {
            NativeMethods.StreamFree(ptr);
        }

        FreeBridgeHandle();
    }

    private void FreeBridgeHandle()
    {
        if (_bridgeHandle.IsAllocated)
            _bridgeHandle.Free();
    }
}
