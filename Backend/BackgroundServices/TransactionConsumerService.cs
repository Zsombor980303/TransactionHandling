using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using TransactionHandling.Data;
using TransactionHandling.Models;

namespace TransactionHandling.BackgroundServices;

public class TransactionConsumerService : BackgroundService
{
    //ILogger is an in-built interface. We create a private local variable here (_logger) from that type. With the class name in <> we help our debugging, because when we check our log later, we will see that the exact log line is related to this specific class
    private readonly ILogger<TransactionConsumerService> _logger;
    private readonly IServiceProvider _serviceProvider; // In-built interface, which is crucial to avoid Captive Dependency --> only establish connection with database, when and how long we need it. After that (eg saving) close it immediately.
    // Different approach then previous projects - RealEstate, Vehicletelemtry - Why?
    //In VehicleTelemetry we used Controllers. Controllers have a short life, once a POST request arrives, .NET wake up the controller to fullfil the request, and then close it as well.
    //In RealEstate we used using... logic, which could also work here, with another logic (once we reach the 50 received data row, we establish the connection "manually"). Manual instantiation forces us to manage the DbContext creation lifecycle and configuration routing inside the business logic layer, instead of letting the central .NET DI Container handle it professionally.

    private readonly List<TransactionDto> _batchBuffer = new(); // Create a TransactionDto type List for receiving data rows before bulk inserting it to the database
    private readonly int _batchSize = 50; // Threshold for data row numbers we handle at once
    private readonly TimeSpan _batchTimeout = TimeSpan.FromSeconds(5); // If we did not have 50 transactions in 5 seconds, we save anyway (we will check if there is at least 1 row which needs to be saved)
    private DateTime _lastFlushTime = DateTime.UtcNow; // For storing the last saving moment

    private IConnection? _connection; // _connection will be responsible for our conection/bridge with Docker. IConnection is also an in-built interface. Here we only create a local variable with this type, later on, we will establish the real connection. This is responsible only for keeping the connection, it can not transfer data! We create this here, so we can reach it everywhere after
    private IModel? _channel; // _channel is our virtual communication channel built on top of the physical connection. A single channel can manage multiple queues (like 'huf' and 'fx') simultaneously, allowing us to send commands and receive messages efficiently without wasting resources. It is responsible for data transfer itself!
    // We would need more channels, if we would operate on more threads. Rule is each threads have it's own channel

    public TransactionConsumerService(ILogger<TransactionConsumerService> logger, IServiceProvider serviceProvider)
    {
        _logger = logger; // .Net will automatically fill this logger parameter with its in-built value here
        _serviceProvider = serviceProvider;
        ConnectToRabbitMQ();
    }

    private void ConnectToRabbitMQ()
    {
        // We create an object from the built-in ConnectionFactory class and configure it to target our local Docker environment.
        var factory = new ConnectionFactory { HostName = "localhost" };

        // We call the built-in function to open the real TCP connection using the factory settings and save it into our variable.
        _connection = factory.CreateConnection();

        // We call the built-in function to create our virtual communication channel on top of the established connection.
        _channel = _connection.CreateModel();

        // Configure Backpressure: Tell RabbitMQ to push a maximum of 50 unacknowledged messages at a time.
        // This prevents memory saturation (OutOfMemoryException) if the Python producer floods the system
        _channel.BasicQos(prefetchSize: 0, prefetchCount: 50, global: false);

        // Declare the target queues. 'durable: true' ensures that RabbitMQ persists the queues and 
        // messages directly to disk (Docker volume) so no transaction data is lost during sudden system crashes.
        _channel.QueueDeclare(queue: "fintech.transactions.huf", durable: true, exclusive: false, autoDelete: false, arguments: null);
        _channel.QueueDeclare(queue: "fintech.transactions.fx", durable: true, exclusive: false, autoDelete: false, arguments: null);

        _logger.LogInformation("Successfully established the physical connection and virtual channel to RabbitMQ!");
    }

    // This is the core execution loop of the background process, mandated by the BackgroundService base class.
    // The .NET hosting framework automatically invokes this method immediately after the constructor completes.
    // The 'stoppingToken' acts as a safety signal that monitors if the application is shutting down, allowing us 
    // to gracefully stop our processing loops without corrupting active transactions.

    protected override async Task ExecuteAsync(CancellationToken stoppingToken) //CancellationToken --> In-built object, which is handled by .NET.
    {
        _logger.LogInformation("Fintech pipeline consumer service is starting to listen...");

        // Create a built-in event-driven consumer instance attached to our virtual channel.
        // This logic is similar to HttpPost, RabbitMQ will send us a signal, that there is new data, wake up! So it is not our C# code which is constantly asking RabbitMQ if there is anything new.
        // The reason we choose this is simply efficiency, and safety (if our C# backend trying to receive 10 000 data in a second, it can break. With RabbitMQ, we can limit the amount of request handled in one step, without losing the remaining ones. Also our Python code can still generate data continously, not like in Http server, where if we break down, that code will also get an error, as not reaching the desired server to sedn the generated data.
        var consumer = new EventingBasicConsumer(_channel);

        //  Subscribe to the 'Received' event. This block executes automatically whenever a message hits the queues.
        //  The async keyword allows us to perform non-blocking asynchronous operations inside the handler.
        // The "+=" sign can be misleading here if we see it in this context for the first time. It is not like our usual +=... It will not append or add anything. It is just a command for consumer.Received. We are asking for memory space for the 50 data rows we are always getting (or fewer, 50 is the maximum). We will use this memory space to handle the data reading, and when we dealt with the whole current package, we can receive the next one from RabbitMQ.
        // We do not pass values to 'model' and 'ea' manually. 
        // When a transaction arrives from Python, the background RabbitMQ library automatically creates these objects:
        // 1. 'model': The sender object (the EventingBasicConsumer instance that triggered this execution).
        // 2. 'ea' (Event Arguments): The envelope containing the physical payload bytes, queue tags, and routing metadata.
        // The library populates them with data and automatically executes our code block below during runtime.
        consumer.Received += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var messageJson = System.Text.Encoding.UTF8.GetString(body);

            // We use JsonSerializer to map the incoming JSON string into our nested TransactionDto record.
            // 'JsonNamingPolicy.CamelCase' matches C# properties with Python's naming conventions.
            // On success, we manually acknowledge the message via 'BasicAck' using the unique DeliveryTag.
            // Malformed payloads throw a JsonException, triggering a 'BasicNack' (requeue: false) to prevent poison loops.
            // Unexpected errors trigger a 'BasicNack' (requeue: true) to safely re-queue the message for a retry.
            try
            {
                // A: Beállítjuk a beolvasót, hogy kezelni tudja a Python kisbetűs/camelCase mezőneveit
                var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

                // B: A JsonSerializer beolvassa a szöveget a TransactionDto rekordunkba
                var transaction = JsonSerializer.Deserialize<TransactionDto>(messageJson, options);

                if (transaction != null)
                {
                    // Enforce thread-safety using a mutual exclusion lock (mutex) on the buffer.
                    // This prevents race conditions and memory corruption when multiple concurrent 
                    // RabbitMQ background threads attempt to modify the underlying List simultaneously.
                    lock (_batchBuffer)
                    {
                        _batchBuffer.Add(transaction);
                    }
                    _logger.LogInformation("Message added to batch buffer. Current size: {Count}", _batchBuffer.Count);
                }

                // Explicit manual acknowledgment to RabbitMQ that the message is safe in our RAM buffer
                _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Critical error during JSON deserialization. Malformed message payload.");

                // 1. Open a short-lived dependency injection scope to handle the database operation safely
                using (var scope = _serviceProvider.CreateScope())
                {
                    // 2. Resolve the DbContext instance from the current scope
                    var dbContext = scope.ServiceProvider.GetRequiredService<FintechDbContext>();

                    // 3. Construct the faulty transaction record using your new entity class
                    var faultyRecord = new FaultyTransactionEntity
                    {
                        FullText = messageJson, // The raw, malformed JSON payload string
                        ErrorType = ex.Message  // The structural error reason thrown by the JSON deserializer
                    };

                    // 4. Persist the faulty record into the PostgreSQL database and wait for completion
                    await dbContext.FaultyTransactions.AddAsync(faultyRecord);
                    await dbContext.SaveChangesAsync();
                }

                // 5. Acknowledge the message to RabbitMQ since the error is now safely audited and stored
                 _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error occurred during message processing.");

                // F: Váratlan rendszerhiba esetén visszatesszük a sorba, hátha legközelebb sikerül
                _channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
            }
            //await Task.Yield(); // Ideiglenes aszinkron helykitöltő, hogy ne kapjunk figyelmeztetést
        };

        // Activating the stream: Instruct RabbitMQ to start pushing messages from both specific queues defined by Python.
        // 'autoAck: false' guarantees that RabbitMQ keeps the transactions safely on disk until we issue a manual approval.
        // We pass our 'consumer' so the system knows exactly which code block to trigger when a message arrives.
        _channel.BasicConsume(queue: "fintech.transactions.huf", autoAck: false, consumer: consumer);
        _channel.BasicConsume(queue: "fintech.transactions.fx", autoAck: false, consumer: consumer);

        // Continuously monitors the buffer on the main execution thread while the service runs.
        // It triggers a database flush if the batch size hits the threshold or if the timeout expires.
        while (!stoppingToken.IsCancellationRequested) //In-built object, which is handled by .NET. It will guarantee no data loss when our background service close suddenly.
        {
            bool shouldFlush = false;

            lock (_batchBuffer)
            {
                if (_batchBuffer.Count > 0 &&
                    (_batchBuffer.Count >= _batchSize || DateTime.UtcNow - _lastFlushTime >= _batchTimeout))
                {
                    shouldFlush = true;
                }
            }

            if (shouldFlush)
            {
                await FlushBatchAsync(); // Execute bulk persistence
            }

            await Task.Delay(100, stoppingToken); // Prevent CPU thrashing, wait 100ms waiting time, except our program ends or crashed, then wake up because we need stoppingToken
        }
    } // This explicitly closes the protected override async Task ExecuteAsync method!

    private async Task FlushBatchAsync()
    {
        List<TransactionDto> currentBatch;
        //this could also work here: (part1) --> var currentBatch = new List<TransactionDto>();

        // Thread-safe swap: instantly copy the buffer contents to a local list and clear the global buffer.
        // This ensures the background RabbitMQ threads can keep pushing messages without blocking.
        lock (_batchBuffer)
        {
            //this could also work here: (part2) --> currentBatch.AddRange(_batchBuffer);
            currentBatch = new List<TransactionDto>(_batchBuffer); // We have to use this format, because List is a reference type (we would use memory address not a simple value), not like a simple int for example. With currentBatch = _batchBuffer in the next line with .Clear we would clear both.
            _batchBuffer.Clear();
            _lastFlushTime = DateTime.UtcNow;
        }

        // Defensive check: if the buffer was empty (e.g., forced timeout flush), stop execution immediately.
        if (currentBatch.Count == 0) return;

        // This avoids the 'Captive Dependency' anti-pattern and guarantees clean connection disposal.
        using var scope = _serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<FintechDbContext>(); //We request .NET to establish connection with our FintechDbContext database

        try
        {
            var entitiesToInsert = new List<TransactionEntity>(); // Create a list with TransactionEntity type rows inside

            foreach (var dto in currentBatch)
            {
                // Iempotency check (Database-level duplication check)
                // Check if the UUID from the Python simulator already exists in the database.
                bool exists = await dbContext.Transactions.AnyAsync<TransactionEntity>(t => t.TransactionId == dto.TransactionId);
                if (exists)
                {
                    _logger.LogWarning("Duplicate transaction skipped in DB layer: {TransactionId}", dto.TransactionId);
                    continue; // Skip this record and move to the next one
                }

                //Flattening the structural JSON DTO into a flat relational table layout.
                var entity = new TransactionEntity // Create a TransactionEntity object, which we will add to our list defined above
                {
                    TransactionId = dto.TransactionId,
                    AccountId = dto.AccountId,
                    ClientName = dto.ClientName,
                    Timestamp = dto.Timestamp,
                    TransactionType = dto.TransactionType,
                    Amount = dto.Amount,
                    Currency = dto.Currency,
                    MerchantName = dto.MerchantDetails?.MerchantName ?? string.Empty,
                    MerchantCategoryCode = dto.MerchantDetails?.CategoryCode ?? string.Empty,
                    MerchantLocation = dto.MerchantDetails?.Location ?? string.Empty,
                    ClientIp = dto.SecurityTelemetry?.ClientIp ?? string.Empty,
                    DeviceFingerprint = dto.SecurityTelemetry?.DeviceFingerprint ?? string.Empty,
                    IsVpn = dto.SecurityTelemetry?.IsVpn ?? false
                };

                entitiesToInsert.Add(entity); //Add to our list
            }

            if (entitiesToInsert.Count > 0)
            {
                // Bulk insert
                // Add the entire collection to the Change Tracker and commit it in a single SQL roundtrip.
                await dbContext.Transactions.AddRangeAsync(entitiesToInsert);
                await dbContext.SaveChangesAsync();

                _logger.LogInformation("Successfully persisted batch of {Count} transactions to PostgreSQL.", entitiesToInsert.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist transaction batch to the database.");
        }

    }
}
