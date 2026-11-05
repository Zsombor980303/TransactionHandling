using Microsoft.EntityFrameworkCore;
using TransactionHandling.BackgroundServices;
using TransactionHandling.Data;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
//builder.Services.AddOpenApi();

// Register our custom background pipeline process so the .NET hosting framework automatically 
// starts it at application launch and provides all required dependencies (like ILogger) to its constructor.
builder.Services.AddHostedService<TransactionHandling.BackgroundServices.TransactionConsumerService>();
builder.Services.AddHostedService<InvalidTransactionConsumerService>();

// Read the connection string from appsettings.json and register FintechDbContext into the .NET DI Container using PostgreSQL
builder.Services.AddDbContext<FintechDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("PostgresConnection")));

var app = builder.Build();

// Configure the HTTP request pipeline.

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
