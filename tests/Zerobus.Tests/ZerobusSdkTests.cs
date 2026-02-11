using Databricks.Zerobus;
using NUnit.Framework;

namespace Databricks.Zerobus.Tests;

[TestFixture]
public class ZerobusSdkTests
{
    [Test]
    public void Constructor_NullEndpoint_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ZerobusSdk(null!, "https://workspace.databricks.com"));
    }

    [Test]
    public void Constructor_NullCatalogUrl_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ZerobusSdk("https://zerobus.databricks.com", null!));
    }

    [Test]
    public void CreateStream_NullTableProperties_ThrowsArgumentNullException()
    {
        // The null check on tableProperties fires before the native call.
        // If the native lib isn't loaded, the constructor will throw first,
        // so we verify our parameter guard with a separate Assert.
        Assert.Throws<ArgumentNullException>(() =>
        {
            using var sdk = new ZerobusSdk("https://test.com", "https://test.com");
            sdk.CreateStream(null!, "id", "secret");
        });
    }

    [Test]
    public void CreateStream_NullClientId_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
        {
            using var sdk = new ZerobusSdk("https://test.com", "https://test.com");
            sdk.CreateStream(new TableProperties("t"), null!, "secret");
        });
    }

    [Test]
    public void CreateStream_NullClientSecret_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
        {
            using var sdk = new ZerobusSdk("https://test.com", "https://test.com");
            sdk.CreateStream(new TableProperties("t"), "id", null!);
        });
    }
}
