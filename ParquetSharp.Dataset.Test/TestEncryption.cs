using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using NUnit.Framework;
using ParquetSharp.Arrow;
using ParquetSharp.Dataset.Partitioning;
using ParquetSharp.Encryption;

namespace ParquetSharp.Dataset.Test;

/// <summary>
/// Test reading encrypted Parquet files
/// Note that files encrypted with external key material are not yet supported.
/// </summary>
[TestFixture]
public class TestEncryption
{
    [Test]
    public static async Task TestReadEncryptedData()
    {
        using var tmpDir = new DisposableDirectory();
        var originalBatch = GenerateBatch();
        WriteParquetFile(tmpDir.AbsPath("data0.parquet"), GenerateBatch());

        using var decryptionConfig = new DecryptionConfiguration();
        using var connectionConfig = new KmsConnectionConfig();
        using var cryptoFactory = new CryptoFactory(_ => new TestKmsClient());
        using var properties = ReaderProperties.GetDefaultReaderProperties();
        using var decryptionProperties = cryptoFactory.GetFileDecryptionProperties(connectionConfig, decryptionConfig);
        properties.FileDecryptionProperties = decryptionProperties;

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();

        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            new NoPartitioning(),
            schema: schema,
            readerProperties: properties);

        using var reader = dataset.ToBatches();

        var rowsRead = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            rowsRead += batch.Length;
        }

        Assert.That(rowsRead, Is.EqualTo(originalBatch.Length));
    }

    [Test]
    public static void TestReadWithoutDecryptionConfig()
    {
        using var tmpDir = new DisposableDirectory();
        WriteParquetFile(tmpDir.AbsPath("data0.parquet"), GenerateBatch());

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", new Int32Type(), false))
            .Field(new Field("x", new FloatType(), false))
            .Build();
        var dataset = new DatasetReader(
            tmpDir.DirectoryPath,
            new NoPartitioning(),
            schema: schema);

        using var reader = dataset.ToBatches();

        var exception = Assert.ThrowsAsync<ParquetException>(async () => { await reader.ReadNextRecordBatchAsync(); });
        Assert.That(exception!.Message, Does.Contain("no decryption found"));
    }

    private static RecordBatch GenerateBatch()
    {
        const int numRows = 100;
        var builder = new RecordBatch.Builder();
        var idValues = Enumerable.Range(0, numRows).ToArray();
        builder.Append("id", false, new Int32Array.Builder().Append(idValues));
        var xValues = Enumerable.Range(0, numRows).Select(i => i * 0.1f).ToArray();
        builder.Append("x", false, new FloatArray.Builder().Append(xValues));
        return builder.Build();
    }

    private static void WriteParquetFile(string path, RecordBatch batch)
    {
        using var encryptionConfig = new EncryptionConfiguration("Key0");
        encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
        {
            { "Key1", batch.Schema.FieldsList.Select(f => f.Name).ToArray() },
        };
        using var connectionConfig = new KmsConnectionConfig();
        using var cryptoFactory = new CryptoFactory(_ => new TestKmsClient());
        using var encryptionProperties = cryptoFactory.GetFileEncryptionProperties(connectionConfig, encryptionConfig);
        using var propertiesBuilder = new WriterPropertiesBuilder();
        propertiesBuilder.Encryption(encryptionProperties);
        using var properties = propertiesBuilder.Build();

        using var writer = new FileWriter(path, batch.Schema, properties);
        writer.WriteRecordBatch(batch);

        writer.Close();
    }

    /// <summary>
    /// Test KMS client with hard-coded master keys.
    /// </summary>
    private sealed class TestKmsClient : IKmsClient
    {
        public string WrapKey(byte[] keyBytes, string masterKeyIdentifier)
        {
            var masterKey = MasterKeys[masterKeyIdentifier];
            using var aes = Aes.Create();
            aes.Key = masterKey;
            using var encryptor = aes.CreateEncryptor(aes.Key, aes.IV);
            var encrypted = EncryptBytes(encryptor, keyBytes);
            return $"{System.Convert.ToBase64String(aes.IV)}:{System.Convert.ToBase64String(encrypted)}";
        }

        public byte[] UnwrapKey(string wrappedKey, string masterKeyIdentifier)
        {
            var split = wrappedKey.Split(':');
            var iv = System.Convert.FromBase64String(split[0]);
            var encryptedKey = System.Convert.FromBase64String(split[1]);
            var masterKey = MasterKeys[masterKeyIdentifier];
            using var aes = Aes.Create();
            aes.Key = masterKey;
            aes.IV = iv;
            using var decryptor = aes.CreateDecryptor(aes.Key, aes.IV);
            return DecryptBytes(decryptor, encryptedKey);
        }

        private static byte[] EncryptBytes(ICryptoTransform encryptor, byte[] plainText)
        {
            using var memoryStream = new MemoryStream();
            using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
            {
                cryptoStream.Write(plainText, 0, plainText.Length);
            }

            return memoryStream.ToArray();
        }

        private static byte[] DecryptBytes(ICryptoTransform decryptor, byte[] cipherText)
        {
            using var memoryStream = new MemoryStream(cipherText);
            using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
            var buffer = new byte[16];
            var offset = 0;
            while (true)
            {
                var read = cryptoStream.Read(buffer, offset, buffer.Length - offset);
                if (read == 0)
                {
                    break;
                }

                offset += read;
            }

            return buffer.Take(offset).ToArray();
        }

        private static readonly Dictionary<string, byte[]> MasterKeys = new()
        {
            { "Key0", new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 } },
            { "Key1", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 } },
        };
    }
}
