using Microsoft.EntityFrameworkCore;
using TransactionHandling.Models;

namespace TransactionHandling.Data;

public class FintechDbContext : DbContext
{
    public FintechDbContext(DbContextOptions<FintechDbContext> options) : base(options)
    {
    }

    public DbSet<TransactionEntity> Transactions => Set<TransactionEntity>();
    public DbSet<FaultyTransactionEntity> FaultyTransactions => Set<FaultyTransactionEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder); //Runs the default Entity Framework configuration to preserve built-in behaviors and settings.

        // Better to use lower-case names in PostgreSQL
        foreach (var entity in modelBuilder.Model.GetEntityTypes())  //Entity Framework goes through all the classes where it detects that we want to create database table(s).
        {
            //var tableName = entity.GetTableName()?.ToLowerScalar();  //Get the table name, and make it fully lower-case (for example Transactions table will be transaction)
            var tableName = entity.GetTableName()?.ToLower();  //Get the table name, and make it fully lower-case (for example Transactions table will be transaction)
            if (tableName != null)
            {
                entity.SetTableName(tableName);
            }

            foreach (var property in entity.GetProperties()) //Entity Framework here goes through the attributes now inside the table
            {
                var columnName = property.GetColumnName();
                // We use Regex on attributes. We look for attribute names like ClientName (usual naming convention in C#) and change it to PostgreSQL compatible. Also change every attribute to lower-case. Also add a "_" between the 2 parts (Client and Name)
                var snakeCaseName = System.Text.RegularExpressions.Regex.Replace(columnName, "([a-z0-9])([A-Z])", "$1_$2").ToLower();

                property.SetColumnName(snakeCaseName); // here we set every attribute name to the above defined one.
            }
        }

        // Set rules for each attributes (how many charachters etc.) in TransactionEntity type tables
        modelBuilder.Entity<TransactionEntity>(entity =>
        {
            // Set how many digits and decimals we can have for payment amount
            entity.Property(e => e.Amount).HasColumnType("decimal(13,2)");

            // Fix each attribute length
            entity.Property(e => e.TransactionId).HasMaxLength(50);
            entity.Property(e => e.AccountId).HasMaxLength(50);
            entity.Property(e => e.ClientName).HasMaxLength(100);
            entity.Property(e => e.TransactionType).HasMaxLength(50);
            entity.Property(e => e.Currency).HasMaxLength(10);
            entity.Property(e => e.MerchantName).HasMaxLength(100);
            entity.Property(e => e.MerchantCategoryCode).HasMaxLength(50);
            entity.Property(e => e.MerchantLocation).HasMaxLength(100);
            entity.Property(e => e.ClientIp).HasMaxLength(45);
            entity.Property(e => e.DeviceFingerprint).HasMaxLength(100);

            // TransactionId has to be unique, we can not have 2 lines/rows with the same TransactionId
            entity.HasIndex(e => e.TransactionId).IsUnique();
        });
    }
}

/*public static class StringExtensions
{
    public static string ToLowerScalar(this string input) => input.ToLower();
}*/
