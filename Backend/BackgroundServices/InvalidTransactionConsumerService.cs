using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;
using TransactionHandling.Data;
using TransactionHandling.Models;

namespace TransactionHandling.BackgroundServices
{
    public class InvalidTransactionConsumerService : BackgroundService
    {
        //ILogger is an in-built interface. We create a private local variable here (_logger) from that type. With the class name in <> we help our debugging, because when we check our log later, we will see that the exact log line is related to this specific class
        private readonly ILogger<InvalidTransactionConsumerService> _logger;
        private readonly IServiceProvider _serviceProvider; // In-built interface, which is crucial to avoid Captive Dependency --> only establish connection with database, when and how long we need it. After that (eg saving) close it immediately.

        private IConnection? _connection; // _connection will be responsible for our conection/bridge with Docker. IConnection is also an in-built interface. Here we only create a local variable with this type, later on, we will establish the real connection. This is responsible only for keeping the connection, it can not transfer data! We create this here, so we can reach it everywhere after
        private IModel? _channel; // _channel is our virtual communication channel built on top of the physical connection. A single channel can manage multiple queues (like 'huf' and 'fx') simultaneously, allowing us to send commands and receive messages efficiently without wasting resources. It is responsible for data transfer itself!
                                  // We would need more channels, if we would operate on more threads. Rule is each threads have it's own channel

        public InvalidTransactionConsumerService(ILogger<InvalidTransactionConsumerService> logger, IServiceProvider serviceProvider)
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
            _channel.QueueDeclare(queue: "fintech.transactions.dead_letter", durable: true, exclusive: false, autoDelete: false, arguments: null);

            _logger.LogInformation("Successfully established the physical connection and virtual channel to RabbitMQ!");
        }

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

                // We need the same check as in TransactionConsumerService, because in that one we only checked the other two queues, not this dead letter one, so here also can be some faulty format
                try
                {
                    // A: Beállítjuk a beolvasót, hogy kezelni tudja a Python kisbetűs/camelCase mezőneveit
                    var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

                    // B: A JsonSerializer beolvassa a szöveget a TransactionDto rekordunkba
                    var transaction = JsonSerializer.Deserialize<TransactionDto>(messageJson, options);

                    if (transaction != null)
                    {
                        // 1. Open a short-lived dependency injection scope to handle the database operation safely
                        using (var scope = _serviceProvider.CreateScope())
                        {
                            // 2. Resolve the DbContext instance from the current scope
                            var dbContext = scope.ServiceProvider.GetRequiredService<FintechDbContext>();
                            // 3. Construct the faulty transaction record using your new entity class
                            var faultyRecord = new FaultyTransactionEntity
                            {
                                FullText = messageJson, // The raw, malformed JSON payload string
                                ErrorType = "Faulty currency type"
                            };

                            // 4. Persist the faulty record into the PostgreSQL database and wait for completion
                            await dbContext.FaultyTransactions.AddAsync(faultyRecord);
                            await dbContext.SaveChangesAsync();

                        }

                        // 5. Acknowledge the message to RabbitMQ since the error is now safely audited and stored
                        _channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
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
            };

            _channel.BasicConsume(queue: "fintech.transactions.dead_letter", autoAck: false, consumer: consumer);
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
