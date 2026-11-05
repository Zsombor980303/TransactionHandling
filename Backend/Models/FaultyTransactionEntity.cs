namespace TransactionHandling.Models
{
    public class FaultyTransactionEntity
    {
        public int Id { get; set; }
        public string FullText { get; set; } = string.Empty;
        public string ErrorType { get; set; } = string.Empty;
    }
}
