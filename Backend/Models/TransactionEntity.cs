using System;

namespace TransactionHandling.Models;

public class TransactionEntity
{
    public long Id { get; set; }
    public string TransactionId { get; set; } = string.Empty;
    public string AccountId { get; set; } = string.Empty;
    public string ClientName { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public string TransactionType { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Currency { get; set; } = string.Empty;

    // MerchantDetails flattening - in database tables, we can not have that json-format
    public string MerchantName { get; set; } = string.Empty;
    public string MerchantCategoryCode { get; set; } = string.Empty;
    public string MerchantLocation { get; set; } = string.Empty;

    // SecurityTelemetry flattening - in database tables, we can not have that json-format
    public string ClientIp { get; set; } = string.Empty;
    public string DeviceFingerprint { get; set; } = string.Empty;
    public bool IsVpn { get; set; }
}
