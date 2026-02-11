// P/Invoke bindings to the Rust FFI layer (zerobus-ffi).
// This is the .NET equivalent of ffi.go in the Go SDK.

using System.Runtime.InteropServices;

namespace Databricks.Zerobus.Native;

/// <summary>
/// A single header key-value pair for C FFI.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CHeader
{
    public IntPtr Key;   // char*
    public IntPtr Value; // char*
}

/// <summary>
/// A collection of headers returned from a managed callback.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CHeaders
{
    public IntPtr Headers;      // CHeader*
    public nuint Count;
    public IntPtr ErrorMessage; // char*
}

/// <summary>
/// Opaque SDK handle. We only ever hold pointers to this.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CZerobusSdk
{
    // Opaque - zero-sized in C, only used via pointer.
}

/// <summary>
/// Result struct returned by most FFI calls.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CResult
{
    [MarshalAs(UnmanagedType.U1)]
    public bool Success;

    public IntPtr ErrorMessage; // char* — must be freed with zerobus_free_error_message

    [MarshalAs(UnmanagedType.U1)]
    public bool IsRetryable;
}

/// <summary>
/// Opaque stream handle.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CZerobusStream
{
    // Opaque.
}

/// <summary>
/// Stream configuration options passed to the native layer.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CStreamConfigurationOptions
{
    public nuint MaxInflightRequests;

    [MarshalAs(UnmanagedType.U1)]
    public bool Recovery;

    public ulong RecoveryTimeoutMs;
    public ulong RecoveryBackoffMs;
    public uint RecoveryRetries;
    public ulong ServerLackOfAckTimeoutMs;
    public ulong FlushTimeoutMs;
    public int RecordType;
    public ulong StreamPausedMaxWaitTimeMs;

    [MarshalAs(UnmanagedType.U1)]
    public bool HasStreamPausedMaxWaitTimeMs;

    public ulong CallbackMaxWaitTimeMs;

    [MarshalAs(UnmanagedType.U1)]
    public bool HasCallbackMaxWaitTimeMs;
}

/// <summary>
/// Represents a single record (either Proto or JSON) returned by get_unacked_records.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CRecord
{
    [MarshalAs(UnmanagedType.U1)]
    public bool IsJson;

    public IntPtr Data;    // uint8_t*
    public nuint DataLen;
}

/// <summary>
/// An array of records returned by get_unacked_records.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct CRecordArray
{
    public IntPtr Records; // CRecord*
    public nuint Len;
}

/// <summary>
/// Callback signature for the headers provider.
/// Matches: CHeaders (*HeadersProviderCallback)(void* user_data)
/// </summary>
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate CHeaders HeadersProviderCallback(IntPtr userData);

/// <summary>
/// P/Invoke declarations for the zerobus_ffi native library.
/// </summary>
internal static partial class NativeMethods
{
    private const string LibName = "zerobus_ffi";

    // --- SDK lifecycle ---

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_sdk_new")]
    public static extern IntPtr SdkNew(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string zerobusEndpoint,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string unityCatalogUrl,
        ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_sdk_free")]
    public static extern void SdkFree(IntPtr sdk);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_sdk_set_use_tls")]
    public static extern void SdkSetUseTls(
        IntPtr sdk,
        [MarshalAs(UnmanagedType.U1)] bool useTls);

    // --- Stream creation ---

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_sdk_create_stream")]
    public static extern unsafe IntPtr SdkCreateStream(
        IntPtr sdk,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName,
        byte* descriptorProtoBytes,
        nuint descriptorProtoLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string clientId,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string clientSecret,
        ref CStreamConfigurationOptions options,
        ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_sdk_create_stream_with_headers_provider")]
    public static extern unsafe IntPtr SdkCreateStreamWithHeadersProvider(
        IntPtr sdk,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string tableName,
        byte* descriptorProtoBytes,
        nuint descriptorProtoLen,
        HeadersProviderCallback headersCallback,
        IntPtr userData,
        ref CStreamConfigurationOptions options,
        ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_free")]
    public static extern void StreamFree(IntPtr stream);

    // --- Record ingestion ---

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_ingest_proto_record")]
    public static extern unsafe long StreamIngestProtoRecord(
        IntPtr stream,
        byte* data,
        nuint dataLen,
        ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_ingest_json_record")]
    public static extern long StreamIngestJsonRecord(
        IntPtr stream,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string jsonData,
        ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_ingest_proto_records")]
    public static extern unsafe long StreamIngestProtoRecords(
        IntPtr stream,
        byte** records,
        nuint* recordLens,
        nuint numRecords,
        ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_ingest_json_records")]
    public static extern unsafe long StreamIngestJsonRecords(
        IntPtr stream,
        byte** jsonRecords,
        nuint numRecords,
        ref CResult result);

    // --- Acknowledgment / flush ---

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_wait_for_offset")]
    [return: MarshalAs(UnmanagedType.U1)]
    public static extern bool StreamWaitForOffset(
        IntPtr stream,
        long offset,
        ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_flush")]
    [return: MarshalAs(UnmanagedType.U1)]
    public static extern bool StreamFlush(IntPtr stream, ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_get_unacked_records")]
    public static extern CRecordArray StreamGetUnackedRecords(IntPtr stream, ref CResult result);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_free_record_array")]
    public static extern void FreeRecordArray(CRecordArray array);

    // --- Stream close ---

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_stream_close")]
    [return: MarshalAs(UnmanagedType.U1)]
    public static extern bool StreamClose(IntPtr stream, ref CResult result);

    // --- Memory management ---

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_free_error_message")]
    public static extern void FreeErrorMessage(IntPtr message);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_free_headers")]
    public static extern void FreeHeaders(CHeaders headers);

    // --- Configuration ---

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl, EntryPoint = "zerobus_get_default_config")]
    public static extern CStreamConfigurationOptions GetDefaultConfig();
}
