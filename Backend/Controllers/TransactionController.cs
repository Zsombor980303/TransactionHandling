using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TransactionHandling.Data;

namespace TransactionHandling.Controllers
{
        [ApiController]
        [Route("[controller]")]
    public class TransactionController : ControllerBase
    {
        private readonly FintechDbContext _context; //This is an empty socket on the wall of our controller class. An active database driver engine will be plugged into this slot later.
        public TransactionController(FintechDbContext context)
        {
            _context = context; // We take the live connection cable ('context') handed to us by .NET, and we plug it into our predefined empty socket ('_context'). From this point onward, our '_context' slot is alive and ready to send data to the hard drive.
        }

        [HttpGet]
        public IActionResult GetStatus()
        {
            return Ok(new
            {
                status = "ONLINE",
                message = "A Fintech adatfeldolgozó API éles, és készen áll az adatok kiszolgálására."
            });
        }

        [HttpGet("statistics")]
        public async Task<IActionResult> GetStatistics()
        {
            var total_amount_for_currencies = await _context.Transactions
                .GroupBy(a => new { a.Currency })
                .Select(b => new { b.Key.Currency, Amount_Sum = b.Sum(x => x.Amount)}).ToListAsync();

            return Ok(total_amount_for_currencies);
        }

        [HttpGet("errors")]
        public async Task<IActionResult> GetFailedTransactions([FromQuery] int limit = 20)
        {
            var errors = await _context.FaultyTransactions
                .OrderByDescending(e => e.Id) // Fetch the latest errors first
                .Take(limit)
                .ToListAsync();

            return Ok(errors);
        }

    }
}
