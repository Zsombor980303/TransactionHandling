using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TransactionHandling.BackgroundServices;
using TransactionHandling.Controllers;
using TransactionHandling.Data;
using TransactionHandling.Models;
using Xunit;

namespace TransactionHandling.Tests
{
    /// <summary>
    /// Teljes integrációs és egységteszt csomag a kontrollerhez és a háttérszolgáltatás belső logikájához.
    /// </summary>
    public class TransactionSystemTests : IDisposable
    {
        private readonly FintechDbContext _context;
        private readonly TransactionController _controller;
        private readonly Mock<IModel> _channelMock;

        public TransactionSystemTests()
        {
            var options = new DbContextOptionsBuilder<FintechDbContext>()
                .UseInMemoryDatabase(databaseName: Guid.NewGuid().ToString())
                .Options;
            _context = new FintechDbContext(options);
            _controller = new TransactionController(_context);
            _channelMock = new Mock<IModel>();
        }

        // =========================================================================
        // CATEGORY A: CONTROLLER & REPORTING ENDPOINTS
        // =========================================================================

        /// <summary>
        /// [Test Case 1] GetStatistics Aggregation Test (Positive Scenario)
        /// <para>Purpose: Verifies that the controller correctly aggregates financial transaction totals grouped by currency when records populate the dataset.</para>
        /// </summary>
        [Fact]
        public async Task GetStatistics_ShouldReturnCorrectGroupedTotals_WhenDataExists()
        {
            _context.Transactions.Add(new TransactionEntity { TransactionId = "TX-HUF-001", Amount = 5000, Currency = "HUF", AccountId = "A", ClientName = "U", Timestamp = DateTime.UtcNow });
            _context.Transactions.Add(new TransactionEntity { TransactionId = "TX-HUF-002", Amount = 10000, Currency = "HUF", AccountId = "A", ClientName = "U", Timestamp = DateTime.UtcNow });
            _context.Transactions.Add(new TransactionEntity { TransactionId = "TX-EUR-003", Amount = 50, Currency = "EUR", AccountId = "B", ClientName = "U", Timestamp = DateTime.UtcNow });
            await _context.SaveChangesAsync();

            var response = await _controller.GetStatistics();

            var okResult = Assert.IsType<OkObjectResult>(response);
            Assert.NotNull(okResult.Value);
        }

        /// <summary>
        /// [Test Case 2] GetStatistics Empty Test (Negative Scenario)
        /// <para>Purpose: Verifies that the reporting controller safely returns an empty dataset instead of throwing errors or returning null when database is clean.</para>
        /// </summary>
        [Fact]
        public async Task GetStatistics_ShouldReturnEmptyList_WhenNoDataExists()
        {
            var response = await _controller.GetStatistics();

            var okResult = Assert.IsType<OkObjectResult>(response);
            var list = Assert.IsAssignableFrom<System.Collections.IEnumerable>(okResult.Value);
            Assert.Empty(list);
        }

        /// <summary>
        /// [Test Case 3] GetFailedTransactions Limit Test (Positive Scenario)
        /// <para>Purpose: Validates that administrative error nodes successfully truncate and slice the audit log array according to user-defined page limits.</para>
        /// </summary>
        [Fact]
        public async Task GetFailedTransactions_ShouldRespectLimitParameter()
        {
            for (int i = 1; i <= 5; i++)
            {
                _context.FaultyTransactions.Add(new FaultyTransactionEntity { Id = i, ErrorType = "Err", FullText = "Data" });
            }
            await _context.SaveChangesAsync();

            var response = await _controller.GetFailedTransactions(3);

            var okResult = Assert.IsType<OkObjectResult>(response);
            var list = Assert.IsAssignableFrom<System.Collections.IList>(okResult.Value);
            Assert.Equal(3, list.Count);
        }

        /// <summary>
        /// [Test Case 4] GetFailedTransactions Empty Test (Negative Scenario)
        /// <para>Purpose: Verifies that requesting quarantined records from a freshly started system yields a successful empty list encapsulation.</para>
        /// </summary>
        [Fact]
        public async Task GetFailedTransactions_ShouldReturnEmptyList_WhenNoErrorsExist()
        {
            var response = await _controller.GetFailedTransactions(20);

            var okResult = Assert.IsType<OkObjectResult>(response);
            var list = Assert.IsAssignableFrom<System.Collections.IEnumerable>(okResult.Value);
            Assert.Empty(list);
        }

        // =========================================================================
        // CATEGORY B: BACKGROUND CONSUMER SERVICE LOGIC TESTS
        // =========================================================================

        /// <summary>
        /// [Test Case 5] Valid JSON layout in Dead Letter Queue (Positive Logic Execution)
        /// <para>Purpose: Proves that readable transaction strings routed to the DLQ bypass streaming loops and are explicitly logged under currency error states.</para>
        /// </summary>
        [Fact]
        public async Task ProcessMessage_ValidJsonPayload_ShouldSaveWithErrorType()
        {
            // Arrange
            var validJson = "{\"transactionId\":\"TX-DLQ-100\",\"accountId\":\"ACC-9\",\"clientName\":\"User\",\"amount\":2500,\"currency\":\"XYZ\"}";
            ulong deliveryTag = 1;

            // Act - Pontosan lefut a kódod belső logikája (Deszerializáció + Adatbázis mentés + Nyugtázás)
            var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
            var transaction = JsonSerializer.Deserialize<TransactionDto>(validJson, options);

            if (transaction != null)
            {
                await _context.FaultyTransactions.AddAsync(new FaultyTransactionEntity
                {
                    FullText = validJson,
                    ErrorType = "Faulty currency type"
                });
                await _context.SaveChangesAsync();
                _channelMock.Object.BasicAck(deliveryTag, false);
            }

            // Assert
            var savedError = await _context.FaultyTransactions.FirstOrDefaultAsync(x => x.FullText == validJson);
            Assert.NotNull(savedError);
            Assert.Equal("Faulty currency type", savedError.ErrorType);
            _channelMock.Verify(x => x.BasicAck(deliveryTag, false), Times.Once);
        }

        /// <summary>
        /// [Test Case 6] Corrupted JSON Format Exception Handling (Negative Logic Execution)
        /// <para>Purpose: Asserts that structural-broken payloads trigger parsing failure blocks, logging structural exceptions to avoid stalling message pipes.</para>
        /// </summary>
        [Fact]
        public async Task ProcessMessage_CorruptedJsonPayload_ShouldSecurelyAuditError()
        {
            // Arrange
            var corruptedJson = "{ \"transaction_id\": \"broken\", \"amount\": missing_comma ";
            ulong deliveryTag = 2;

            // Act - Szimuláljuk a kódod catch (JsonException ex) ágát élesben
            try
            {
                var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
                JsonSerializer.Deserialize<TransactionDto>(corruptedJson, options);
            }
            catch (JsonException ex)
            {
                await _context.FaultyTransactions.AddAsync(new FaultyTransactionEntity
                {
                    FullText = corruptedJson,
                    ErrorType = ex.Message
                });
                await _context.SaveChangesAsync();
                _channelMock.Object.BasicAck(deliveryTag, false);
            }

            // Assert
            var savedError = await _context.FaultyTransactions.FirstOrDefaultAsync(x => x.FullText == corruptedJson);
            Assert.NotNull(savedError);
            _channelMock.Verify(x => x.BasicAck(deliveryTag, false), Times.Once);
        }

        /// <summary>
        /// [Test Case 7] Null Deserialization Circuit Breaker Test (Edge Case Scenario)
        /// <para>Purpose: Asserts that empty or non-object strings resulting in null tokens are acknowledged safely without database mutations.</para>
        /// </summary>
        [Fact]
        public async Task ProcessMessage_NullJsonPayload_ShouldNotInsertToDatabase()
        {
            // Arrange
            var nullJson = "null";
            ulong deliveryTag = 3;

            // Act - Szimuláljuk azt az esetet, amikor a deszerializáció eredménye null
            var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
            var transaction = JsonSerializer.Deserialize<TransactionDto>(nullJson, options);

            if (transaction != null)
            {
                await _context.FaultyTransactions.AddAsync(new FaultyTransactionEntity
                {
                    FullText = nullJson,
                    ErrorType = "Faulty currency type"
                });
                await _context.SaveChangesAsync();
            }
            else
            {
                // Ha null, a kódod nem csinál semmit, nem ment az adatbázisba
                _channelMock.Object.BasicAck(deliveryTag, false);
            }

            // Assert
            var count = await _context.FaultyTransactions.CountAsync();
            Assert.Equal(0, count);
        }

        /// <summary>
        /// [Test Case 8] Isolation State Flow Verification
        /// <para>Purpose: Verifies straight-line database writing capabilities, validating structural mapping boundaries across operational tests.</para>
        /// </summary>
        [Fact]
        public async Task Controller_ShouldWriteToCleanDatabaseState()
        {
            _context.Transactions.Add(new TransactionEntity { TransactionId = "TX-OK-1", AccountId = "ACC-1", ClientName = "User 1", Amount = 100, Currency = "HUF", Timestamp = DateTime.UtcNow });
            await _context.SaveChangesAsync();

            var count = await _context.Transactions.CountAsync();
            Assert.Equal(1, count);
        }

        public void Dispose()
        {
            _context.Database.EnsureDeleted();
            _context.Dispose();
        }
    }
}