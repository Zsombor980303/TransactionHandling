// This class is purely for receveing the Python generated data from RabbitMQ in the same json format as it is stored there, so we can easily receive it, without losing any important data
// Difference between this, and my previous projects (like VehicleTelemetry):
// Here we create Record instead of "usual" class. Why?
// 1. Because we can not modify it after we initialize it (object creation done, and filled with data). This class (because it is still a class, even though inside we have record(s)) is only for receiving data, we do not want to corrupt them in any way.
// 2. It is easier to compare records than normal classes
// We could also do it with normal classes technically, but considering the points above, let's choose Records
namespace TransactionHandling.Models;

public record TransactionDto(
    string TransactionId,
    string AccountId,
    string ClientName,
    DateTime Timestamp,
    string TransactionType,
    decimal Amount,
    string Currency,
    MerchantDetailsDto MerchantDetails,
    SecurityTelemetryDto SecurityTelemetry
);

public record MerchantDetailsDto(
    string MerchantName,
    string CategoryCode,
    string Location
);

public record SecurityTelemetryDto(
    string ClientIp,
    string DeviceFingerprint,
    bool IsVpn
);