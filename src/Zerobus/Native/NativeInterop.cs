// High-level safe wrappers around P/Invoke calls.
// Handles marshalling, error conversion, and memory management.
// This is the .NET equivalent of the unexported ffi* functions in ffi.go.

using System.Runtime.InteropServices;
using System.Text;

namespace ScalePad.Databricks.Zerobus.Native;

/// <summary>
/// Provides safe, managed wrappers around the raw P/Invoke layer.
/// All methods convert <see cref="CResult"/> errors into <see cref="ZerobusException"/>.
/// </summary>
internal static class NativeInterop
{
    /// <summary>
    /// Converts a <see cref="CResult"/> to a <see cref="ZerobusException"/> (or null on success).
    /// Frees the native error message string.
    /// </summary>
    internal static ZerobusException? ToException(ref CResult result)
    {
        if (result.Success)
            return null;

        string message;
        if (result.ErrorMessage != IntPtr.Zero)
        {
            message = Marshal.PtrToStringUTF8(result.ErrorMessage) ?? "unknown error";
            NativeMethods.FreeErrorMessage(result.ErrorMessage);
            result.ErrorMessage = IntPtr.Zero;
        }
        else
        {
            message = "unknown error";
        }

        return new ZerobusException(message, result.IsRetryable);
    }

    /// <summary>
    /// Throws if the <see cref="CResult"/> indicates failure.
    /// </summary>
    internal static void ThrowIfFailed(ref CResult result)
    {
        var ex = ToException(ref result);
        if (ex is not null)
            throw ex;
    }

    /// <summary>
    /// Converts a transient <see cref="CResult"/> pointer (valid only for the duration of
    /// a native callback) to a <see cref="ZerobusException"/>.
    /// Unlike <see cref="ToException(ref CResult)"/>, this overload does <b>not</b> free
    /// the error message — Rust owns and frees it after the callback returns.
    /// </summary>
    private static unsafe ZerobusException ToException(CResult* result)
    {
        var message = result->ErrorMessage != IntPtr.Zero
            ? Marshal.PtrToStringUTF8(result->ErrorMessage) ?? "unknown error"
            : "unknown error";
        return new ZerobusException(message, result->IsRetryable);
    }

    private static unsafe void ApplyResult(TaskCompletionSource tcs, CResult* result)
    {
        if (result->Success)
            tcs.TrySetResult();
        else
            tcs.TrySetException(ToException(result));
    }

    private static unsafe void ApplyResult<T>(TaskCompletionSource<T> tcs, CResult* result, T successValue)
    {
        if (result->Success)
            tcs.TrySetResult(successValue);
        else
            tcs.TrySetException(ToException(result));
    }

    /// <summary>
    /// Creates a new SDK instance.
    /// </summary>
    public static IntPtr SdkNew(string zerobusEndpoint, string unityCatalogUrl)
    {
        var result = new CResult();
        var ptr = NativeMethods.SdkNew(zerobusEndpoint, unityCatalogUrl, ref result);

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to create SDK instance", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Creates a stream with OAuth credentials.
    /// </summary>
    public static unsafe IntPtr SdkCreateStream(
        IntPtr sdkPtr,
        string tableName,
        ReadOnlySpan<byte> descriptorProto,
        string clientId,
        string clientSecret,
        ref CStreamConfigurationOptions options)
    {
        var result = new CResult();
        IntPtr ptr;

        fixed (byte* descPtr = descriptorProto)
        {
            ptr = NativeMethods.SdkCreateStream(
                sdkPtr,
                tableName,
                descPtr,
                (nuint)descriptorProto.Length,
                clientId,
                clientSecret,
                ref options,
                ref result);
        }

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to create stream", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Creates a stream with OAuth credentials asynchronously.
    /// Returns immediately; the returned <see cref="Task{IntPtr}"/> completes on the Tokio
    /// thread when stream creation succeeds or fails.
    /// </summary>
    /// <remarks>
    /// <paramref name="descriptorProto"/> is copied by Rust before this method returns, so
    /// the span does not need to remain valid after the call.
    /// </remarks>
    public static unsafe Task<IntPtr> SdkCreateStreamAsync(
        IntPtr sdkPtr,
        string tableName,
        ReadOnlySpan<byte> descriptorProto,
        string clientId,
        string clientSecret,
        ref CStreamConfigurationOptions options)
    {
        var tcs = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);

        CreateStreamCallback callbackDelegate = (_, stream, result) =>
        {
            ApplyResult(tcs, result, stream);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        fixed (byte* descPtr = descriptorProto)
        {
            NativeMethods.SdkCreateStreamAsync(
                sdkPtr,
                tableName,
                descPtr,
                (nuint)descriptorProto.Length,
                clientId,
                clientSecret,
                ref options,
                callbackDelegate,
                IntPtr.Zero);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Creates a stream with a custom headers provider callback.
    /// </summary>
    public static unsafe IntPtr SdkCreateStreamWithHeadersProvider(
        IntPtr sdkPtr,
        string tableName,
        ReadOnlySpan<byte> descriptorProto,
        HeadersProviderCallback callback,
        IntPtr userData,
        ref CStreamConfigurationOptions options)
    {
        var result = new CResult();
        IntPtr ptr;

        fixed (byte* descPtr = descriptorProto)
        {
            ptr = NativeMethods.SdkCreateStreamWithHeadersProvider(
                sdkPtr,
                tableName,
                descPtr,
                (nuint)descriptorProto.Length,
                callback,
                userData,
                ref options,
                ref result);
        }

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to create stream with headers provider", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Creates a stream with a custom headers provider callback asynchronously.
    /// Returns immediately; the returned <see cref="Task{IntPtr}"/> completes on the Tokio
    /// thread when stream creation succeeds or fails.
    /// </summary>
    /// <remarks>
    /// <para><paramref name="headersCallback"/> and the <paramref name="headersUserData"/> it
    /// references must remain valid until the returned task completes.</para>
    /// <para><paramref name="descriptorProto"/> is copied by Rust before this method returns.</para>
    /// </remarks>
    public static unsafe Task<IntPtr> SdkCreateStreamWithHeadersProviderAsync(
        IntPtr sdkPtr,
        string tableName,
        ReadOnlySpan<byte> descriptorProto,
        HeadersProviderCallback headersCallback,
        IntPtr headersUserData,
        ref CStreamConfigurationOptions options)
    {
        var tcs = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);

        CreateStreamCallback callbackDelegate = (_, stream, result) =>
        {
            ApplyResult(tcs, result, stream);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        fixed (byte* descPtr = descriptorProto)
        {
            NativeMethods.SdkCreateStreamWithHeadersProviderAsync(
                sdkPtr,
                tableName,
                descPtr,
                (nuint)descriptorProto.Length,
                headersCallback,
                headersUserData,
                ref options,
                callbackDelegate,
                IntPtr.Zero);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Recreates a stream from an existing stream.
    /// </summary>
    public static IntPtr SdkRecreateStream(IntPtr sdkPtr, IntPtr streamPtr)
    {
        var result = new CResult();
        var ptr = NativeMethods.SdkRecreateStream(sdkPtr, streamPtr, ref result);

        if (ptr == IntPtr.Zero)
        {
            var ex = ToException(ref result);
            throw ex ?? new ZerobusException("Failed to recreate stream", isRetryable: false);
        }

        return ptr;
    }

    /// <summary>
    /// Recreates a stream from an existing stream asynchronously.
    /// Returns immediately; the returned <see cref="Task{IntPtr}"/> completes on the Tokio
    /// thread when recreation succeeds or fails.
    /// </summary>
    /// <remarks>
    /// <paramref name="sdkPtr"/> and <paramref name="streamPtr"/> must remain valid until
    /// the returned task completes.
    /// </remarks>
    public static unsafe Task<IntPtr> SdkRecreateStreamAsync(IntPtr sdkPtr, IntPtr streamPtr)
    {
        var tcs = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);

        CreateStreamCallback callbackDelegate = (_, stream, result) =>
        {
            ApplyResult(tcs, result, stream);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        NativeMethods.SdkRecreateStreamAsync(
            sdkPtr,
            streamPtr,
            callbackDelegate,
            IntPtr.Zero);

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Ingests a single protobuf record asynchronously and returns the offset.
    /// Returns immediately; the returned <see cref="Task{Int64}"/> completes on the Tokio
    /// thread when the ingest succeeds or fails.
    /// </summary>
    /// <remarks>
    /// <paramref name="data"/> is copied by Rust before this method returns, so
    /// the span does not need to remain valid after the call.
    /// </remarks>
    public static unsafe Task<long> StreamIngestProtoRecordAsync(IntPtr streamPtr, ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            throw new ZerobusException("empty data", isRetryable: false);

        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        IngestRecordCallback callbackDelegate = (_, offset, result) =>
        {
            ApplyResult(tcs, result, offset);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        fixed (byte* dataPtr = data)
        {
            NativeMethods.StreamIngestProtoRecordAsync(
                streamPtr,
                dataPtr,
                (nuint)data.Length,
                callbackDelegate,
                IntPtr.Zero);
        }

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Ingests a single JSON record asynchronously and returns the offset.
    /// Returns immediately; the returned <see cref="Task{Int64}"/> completes on the Tokio
    /// thread when the ingest succeeds or fails.
    /// </summary>
    /// <remarks>
    /// <paramref name="jsonData"/> is copied by Rust before this method returns.
    /// </remarks>
    public static unsafe Task<long> StreamIngestJsonRecordAsync(IntPtr streamPtr, string jsonData)
    {
        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        IngestRecordCallback callbackDelegate = (_, offset, result) =>
        {
            ApplyResult(tcs, result, offset);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        NativeMethods.StreamIngestJsonRecordAsync(
            streamPtr,
            jsonData,
            callbackDelegate,
            IntPtr.Zero);

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Ingests a single protobuf record and returns the offset.
    /// </summary>
    public static unsafe long StreamIngestProtoRecord(IntPtr streamPtr, ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            throw new ZerobusException("empty data", isRetryable: false);

        var result = new CResult();
        long offset;

        fixed (byte* dataPtr = data)
        {
            offset = NativeMethods.StreamIngestProtoRecord(
                streamPtr,
                dataPtr,
                (nuint)data.Length,
                ref result);
        }

        if (offset < 0)
        {
            ThrowIfFailed(ref result);
            throw new ZerobusException("Ingest failed with unknown error", isRetryable: false);
        }

        return offset;
    }

    /// <summary>
    /// Ingests a single JSON record and returns the offset.
    /// </summary>
    public static long StreamIngestJsonRecord(IntPtr streamPtr, string jsonData)
    {
        var result = new CResult();
        var offset = NativeMethods.StreamIngestJsonRecord(streamPtr, jsonData, ref result);

        if (offset < 0)
        {
            ThrowIfFailed(ref result);
            throw new ZerobusException("Ingest failed with unknown error", isRetryable: false);
        }

        return offset;
    }

    /// <summary>
    /// Ingests a batch of protobuf records and returns the last offset.
    /// </summary>
    public static unsafe long StreamIngestProtoRecords(IntPtr streamPtr, byte[][] records)
    {
        if (records.Length == 0)
            return -1;

        var result = new CResult();
        var numRecords = (nuint)records.Length;

        // Pin all record buffers and collect pointers
        var handles = new GCHandle[records.Length];
        var ptrs = stackalloc byte*[records.Length];
        var lens = stackalloc nuint[records.Length];

        try
        {
            for (int i = 0; i < records.Length; i++)
            {
                handles[i] = GCHandle.Alloc(records[i], GCHandleType.Pinned);
                ptrs[i] = (byte*)handles[i].AddrOfPinnedObject();
                lens[i] = (nuint)records[i].Length;
            }

            var offset = NativeMethods.StreamIngestProtoRecords(
                streamPtr,
                ptrs,
                lens,
                numRecords,
                ref result);

            if (offset == -2) return -1; // empty batch
            if (offset < 0)
            {
                ThrowIfFailed(ref result);
                throw new ZerobusException("Batch ingest failed with unknown error", isRetryable: false);
            }

            return offset;
        }
        finally
        {
            for (int i = 0; i < handles.Length; i++)
            {
                if (handles[i].IsAllocated)
                    handles[i].Free();
            }
        }
    }

    /// <summary>
    /// Ingests a batch of protobuf records asynchronously and returns the last offset.
    /// Returns immediately; the returned <see cref="Task{Int64}"/> completes on the Tokio
    /// thread when the ingest succeeds or fails.
    /// </summary>
    /// <remarks>
    /// All record data is copied by Rust before this method returns.
    /// </remarks>
    public static unsafe Task<long> StreamIngestProtoRecordsAsync(IntPtr streamPtr, byte[][] records)
    {
        if (records.Length == 0)
            return Task.FromResult(-1L);

        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        var handles = new GCHandle[records.Length];
        var ptrs = stackalloc byte*[records.Length];
        var lens = stackalloc nuint[records.Length];

        for (int i = 0; i < records.Length; i++)
        {
            handles[i] = GCHandle.Alloc(records[i], GCHandleType.Pinned);
            ptrs[i] = (byte*)handles[i].AddrOfPinnedObject();
            lens[i] = (nuint)records[i].Length;
        }

        IngestRecordCallback callbackDelegate = (_, offset, result) =>
        {
            ApplyResult(tcs, result, offset == -2 ? -1 : offset);
        };

        var callbackHandle = GCHandle.Alloc(callbackDelegate);

        NativeMethods.StreamIngestProtoRecordsAsync(
            streamPtr,
            ptrs,
            lens,
            (nuint)records.Length,
            callbackDelegate,
            IntPtr.Zero);

        // Ensure the callback delegate's GCHandle is freed once the operation completes.
        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (callbackHandle.IsAllocated)
                    callbackHandle.Free();

                for (int i = 0; i < handles.Length; i++)
                {
                    if (handles[i].IsAllocated)
                        handles[i].Free();
                }
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Ingests a batch of JSON records and returns the last offset.
    /// </summary>
    public static unsafe long StreamIngestJsonRecords(IntPtr streamPtr, string[] records)
    {
        if (records.Length == 0)
            return -1;

        var result = new CResult();
        var numRecords = (nuint)records.Length;

        // Encode each string as null-terminated UTF-8 and pin
        var handles = new GCHandle[records.Length];
        var ptrs = stackalloc byte*[records.Length];

        try
        {
            for (int i = 0; i < records.Length; i++)
            {
                // Encode with null terminator
                var utf8 = Encoding.UTF8.GetBytes(records[i] + '\0');
                handles[i] = GCHandle.Alloc(utf8, GCHandleType.Pinned);
                ptrs[i] = (byte*)handles[i].AddrOfPinnedObject();
            }

            var offset = NativeMethods.StreamIngestJsonRecords(
                streamPtr,
                ptrs,
                numRecords,
                ref result);

            if (offset == -2) return -1; // empty batch
            if (offset < 0)
            {
                ThrowIfFailed(ref result);
                throw new ZerobusException("Batch ingest failed with unknown error", isRetryable: false);
            }

            return offset;
        }
        finally
        {
            for (int i = 0; i < handles.Length; i++)
            {
                if (handles[i].IsAllocated)
                    handles[i].Free();
            }
        }
    }

    /// <summary>
    /// Ingests a batch of JSON records asynchronously and returns the last offset.
    /// Returns immediately; the returned <see cref="Task{Int64}"/> completes on the Tokio
    /// thread when the ingest succeeds or fails.
    /// </summary>
    /// <remarks>
    /// All JSON strings are copied by Rust before this method returns.
    /// </remarks>
    public static unsafe Task<long> StreamIngestJsonRecordsAsync(IntPtr streamPtr, string[] records)
    {
        if (records.Length == 0)
            return Task.FromResult(-1L);

        var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        var handles = new GCHandle[records.Length];
        var ptrs = stackalloc byte*[records.Length];

        for (int i = 0; i < records.Length; i++)
        {
            var utf8 = Encoding.UTF8.GetBytes(records[i] + '\0');
            handles[i] = GCHandle.Alloc(utf8, GCHandleType.Pinned);
            ptrs[i] = (byte*)handles[i].AddrOfPinnedObject();
        }

        IngestRecordCallback callbackDelegate = (_, offset, result) =>
        {
            ApplyResult(tcs, result, offset == -2 ? -1 : offset);
        };

        var callbackHandle = GCHandle.Alloc(callbackDelegate);

        NativeMethods.StreamIngestJsonRecordsAsync(
            streamPtr,
            ptrs,
            (nuint)records.Length,
            callbackDelegate,
            IntPtr.Zero);

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (callbackHandle.IsAllocated)
                    callbackHandle.Free();

                for (int i = 0; i < handles.Length; i++)
                {
                    if (handles[i].IsAllocated)
                        handles[i].Free();
                }
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Waits for a specific offset to be acknowledged asynchronously.
    /// Returns immediately; the returned <see cref="Task"/> completes on the Tokio
    /// thread when the acknowledgment arrives or an error occurs.
    /// </summary>
    public static unsafe Task StreamWaitForOffsetAsync(IntPtr streamPtr, long offset)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        VoidOperationCallback callbackDelegate = (_, result) =>
        {
            ApplyResult(tcs, result);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        NativeMethods.StreamWaitForOffsetAsync(
            streamPtr,
            offset,
            callbackDelegate,
            IntPtr.Zero);

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Waits for a specific offset to be acknowledged.
    /// </summary>
    public static void StreamWaitForOffset(IntPtr streamPtr, long offset)
    {
        var result = new CResult();
        var success = NativeMethods.StreamWaitForOffset(streamPtr, offset, ref result);

        if (!success)
            ThrowIfFailed(ref result);
    }

    /// <summary>
    /// Flushes all pending records.
    /// </summary>
    public static void StreamFlush(IntPtr streamPtr)
    {
        var result = new CResult();
        var success = NativeMethods.StreamFlush(streamPtr, ref result);

        if (!success)
            ThrowIfFailed(ref result);
    }

    /// <summary>
    /// Flushes all pending records asynchronously.
    /// Returns immediately; the returned <see cref="Task"/> completes on the Tokio
    /// thread when the flush succeeds or fails.
    /// </summary>
    public static unsafe Task StreamFlushAsync(IntPtr streamPtr)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        VoidOperationCallback callbackDelegate = (_, result) =>
        {
            ApplyResult(tcs, result);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        NativeMethods.StreamFlushAsync(
            streamPtr,
            callbackDelegate,
            IntPtr.Zero);

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Retrieves all unacknowledged records from a closed/failed stream.
    /// </summary>
    public static unsafe object[] StreamGetUnackedRecords(IntPtr streamPtr)
    {
        var result = new CResult();
        var cArray = NativeMethods.StreamGetUnackedRecords(streamPtr, ref result);

        if (cArray.Records == IntPtr.Zero)
        {
            if ((int)cArray.Len == 0)
            {
                var ex = ToException(ref result);
                if (ex is not null) throw ex;
                return [];
            }

            ThrowIfFailed(ref result);
            return [];
        }

        if ((int)cArray.Len == 0)
            return [];

        var records = new object[(int)cArray.Len];
        var recordSize = Marshal.SizeOf<CRecord>();

        for (int i = 0; i < (int)cArray.Len; i++)
        {
            var cRecord = Marshal.PtrToStructure<CRecord>(cArray.Records + i * recordSize);
            var data = new byte[(int)cRecord.DataLen];
            Marshal.Copy(cRecord.Data, data, 0, data.Length);

            records[i] = cRecord.IsJson ? Encoding.UTF8.GetString(data) : data;
        }

        NativeMethods.FreeRecordArray(cArray);
        return records;
    }

    /// <summary>
    /// Closes the stream gracefully asynchronously.
    /// Returns immediately; the returned <see cref="Task"/> completes on the Tokio
    /// thread when the close succeeds or fails.
    /// </summary>
    public static unsafe Task StreamCloseAsync(IntPtr streamPtr)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        VoidOperationCallback callbackDelegate = (_, result) =>
        {
            ApplyResult(tcs, result);
        };

        var handle = GCHandle.Alloc(callbackDelegate);

        NativeMethods.StreamCloseAsync(
            streamPtr,
            callbackDelegate,
            IntPtr.Zero);

        _ = tcs.Task.ContinueWith(
            _ =>
            {
                if (handle.IsAllocated)
                    handle.Free();
            },
            TaskContinuationOptions.ExecuteSynchronously);

        return tcs.Task;
    }

    /// <summary>
    /// Closes the stream gracefully.
    /// </summary>
    public static void StreamClose(IntPtr streamPtr)
    {
        var result = new CResult();
        var success = NativeMethods.StreamClose(streamPtr, ref result);

        if (!success)
            ThrowIfFailed(ref result);
    }

    /// <summary>
    /// Converts managed <see cref="StreamConfigurationOptions"/> to the native struct,
    /// applying defaults for unset values.
    /// </summary>
    public static CStreamConfigurationOptions ConvertConfig(StreamConfigurationOptions? options)
    {
        if (options is null)
            return NativeMethods.GetDefaultConfig();

        var defaults = StreamConfigurationOptions.Default;

        return new CStreamConfigurationOptions
        {
            MaxInflightRequests = (nuint)(options.MaxInflightRequests > 0
                ? options.MaxInflightRequests
                : defaults.MaxInflightRequests),
            Recovery = options.Recovery,
            RecoveryTimeoutMs = options.RecoveryTimeoutMs > 0
                ? options.RecoveryTimeoutMs
                : defaults.RecoveryTimeoutMs,
            RecoveryBackoffMs = options.RecoveryBackoffMs > 0
                ? options.RecoveryBackoffMs
                : defaults.RecoveryBackoffMs,
            RecoveryRetries = options.RecoveryRetries > 0
                ? options.RecoveryRetries
                : defaults.RecoveryRetries,
            ServerLackOfAckTimeoutMs = options.ServerLackOfAckTimeoutMs > 0
                ? options.ServerLackOfAckTimeoutMs
                : defaults.ServerLackOfAckTimeoutMs,
            FlushTimeoutMs = options.FlushTimeoutMs > 0
                ? options.FlushTimeoutMs
                : defaults.FlushTimeoutMs,
            RecordType = (int)(options.RecordType != RecordType.Unspecified
                ? options.RecordType
                : defaults.RecordType),
            StreamPausedMaxWaitTimeMs = options.StreamPausedMaxWaitTimeMs ?? 0,
            HasStreamPausedMaxWaitTimeMs = options.StreamPausedMaxWaitTimeMs.HasValue,
            CallbackMaxWaitTimeMs = 0,
            HasCallbackMaxWaitTimeMs = false,
        };
    }
}
