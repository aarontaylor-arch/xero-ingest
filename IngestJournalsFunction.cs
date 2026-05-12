using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace XeroIngest
{
    public class IngestJournalsFunction
    {
        private readonly ILogger<IngestJournalsFunction> _logger;
        private static readonly HttpClient _httpClient = new HttpClient();

        public IngestJournalsFunction(ILogger<IngestJournalsFunction> logger)
        {
            _logger = logger;
        }

        private static int GetInt(JsonElement el, string prop)
        {
            if (!el.TryGetProperty(prop, out var val)) return 0;
            if (val.ValueKind == JsonValueKind.Number) return val.GetInt32();
            if (val.ValueKind == JsonValueKind.String) return int.TryParse(val.GetString(), out var n) ? n : 0;
            return 0;
        }

        private static decimal GetDecimal(JsonElement el, string prop)
        {
            if (!el.TryGetProperty(prop, out var val)) return 0m;
            if (val.ValueKind == JsonValueKind.Number) return val.GetDecimal();
            if (val.ValueKind == JsonValueKind.String) return decimal.TryParse(val.GetString(), out var n) ? n : 0m;
            return 0m;
        }

        private static DateTime ParseXeroDate(string xeroDate)
        {
            if (!string.IsNullOrEmpty(xeroDate) && xeroDate.StartsWith("/Date("))
            {
                var end = xeroDate.IndexOf('+', 6);
                if (end < 0) end = xeroDate.IndexOf(')', 6);
                var ms = long.Parse(xeroDate.Substring(6, end - 6));
                return DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
            }
            return DateTime.Parse(xeroDate);
        }

        [Function("IngestXroJournalsTimerFunction")]
        public async Task Run([TimerTrigger("0 */15 * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation("Xero journal ingest started at {time}", DateTime.UtcNow);

            var kvUri = Environment.GetEnvironmentVariable("kvUri");
            var clientId = Environment.GetEnvironmentVariable("xroClientId");
            var clientSecret = Environment.GetEnvironmentVariable("xroClientSecret");
            var tenantId = Environment.GetEnvironmentVariable("xroTenantId");
            var sqlConnection = Environment.GetEnvironmentVariable("SqlConnectionString");

            var kvClient = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());
            var refreshTokenSecret = await kvClient.GetSecretAsync("XeroTokenRefresh");
            var refreshToken = refreshTokenSecret.Value.Value;
            _logger.LogInformation("Retrieved refresh token from Key Vault");

            var basicAuth = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));
            var tokenRequest = new HttpRequestMessage(HttpMethod.Post, "https://identity.xero.com/connect/token");
            tokenRequest.Headers.Authorization = new AuthenticationHeaderValue("Basic", basicAuth);
            tokenRequest.Content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "refresh_token"),
                new KeyValuePair<string, string>("refresh_token", refreshToken)
            });

            var tokenResponse = await _httpClient.SendAsync(tokenRequest);
            var tokenBody = await tokenResponse.Content.ReadAsStringAsync();

            if (!tokenResponse.IsSuccessStatusCode)
            {
                _logger.LogError("Error getting Xero access token: {status} ({reason}). Body: {body}",
                    (int)tokenResponse.StatusCode, tokenResponse.ReasonPhrase, tokenBody);
                return;
            }

            var tokenJson = JsonDocument.Parse(tokenBody);
            var accessToken = tokenJson.RootElement.GetProperty("access_token").GetString();
            var newRefreshToken = tokenJson.RootElement.GetProperty("refresh_token").GetString();
            _logger.LogInformation("Successfully obtained access token");

            await kvClient.SetSecretAsync("XeroTokenRefresh", newRefreshToken);
            _logger.LogInformation("Saved new refresh token to Key Vault");

            int maxJournalNumber = 0;
            using (var conn = new SqlConnection(sqlConnection))
            {
                await conn.OpenAsync();
                var cmd = new SqlCommand("SELECT ISNULL(MAX([JournalNumber]), 0) FROM [XroJournal]", conn);
                maxJournalNumber = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            }
            _logger.LogInformation("Max journal number in DB: {max}", maxJournalNumber);

            int offset = maxJournalNumber;
            int totalUpserted = 0;
            int totalLines = 0;

            while (true)
            {
                var journalRequest = new HttpRequestMessage(HttpMethod.Get,
                    $"https://api.xero.com/api.xro/2.0/Journals?offset={offset}&paymentsOnly=false");
                journalRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                journalRequest.Headers.Add("xero-tenant-id", tenantId);
                journalRequest.Headers.Add("Accept", "application/json");

                var journalResponse = await _httpClient.SendAsync(journalRequest);
                var journalBody = await journalResponse.Content.ReadAsStringAsync();

                if (!journalResponse.IsSuccessStatusCode)
                {
                    _logger.LogError("Error fetching journals from Xero: {status} {body}",
                        (int)journalResponse.StatusCode, journalBody);
                    break;
                }

                var journalJson = JsonDocument.Parse(journalBody);
                var journals = journalJson.RootElement.GetProperty("Journals");

                if (journals.GetArrayLength() == 0)
                {
                    _logger.LogInformation("No more journals to fetch");
                    break;
                }

                using (var conn = new SqlConnection(sqlConnection))
                {
                    await conn.OpenAsync();

                    foreach (var journal in journals.EnumerateArray())
                    {
                        var journalId     = journal.GetProperty("JournalID").GetString();
                        var journalNumber = GetInt(journal, "JournalNumber");
                        var journalDate   = ParseXeroDate(journal.GetProperty("JournalDate").GetString());
                        var postedDateUtc = ParseXeroDate(journal.GetProperty("CreatedDateUTC").GetString());
                        var sourceId      = journal.TryGetProperty("SourceID",   out var srcId)   ? srcId.GetString()   : null;
                        var sourceType    = journal.TryGetProperty("SourceType", out var srcType) ? srcType.GetString() : null;

                        var upsertJournal = @"
                            MERGE [XroJournal] AS target
                            USING (SELECT @JournalId AS JournalId) AS source
                            ON target.JournalId = source.JournalId
                            WHEN MATCHED THEN UPDATE SET
                                JournalNumber = @JournalNumber,
                                JournalDate   = @JournalDate,
                                PostedDateUtc = @PostedDateUtc,
                                SourceId      = @SourceId,
                                SourceType    = @SourceType
                            WHEN NOT MATCHED THEN INSERT
                                (JournalId, JournalNumber, JournalDate, PostedDateUtc, SourceId, SourceType)
                            VALUES
                                (@JournalId, @JournalNumber, @JournalDate, @PostedDateUtc, @SourceId, @SourceType);";

                        using (var cmd = new SqlCommand(upsertJournal, conn))
                        {
                            cmd.Parameters.AddWithValue("@JournalId",     journalId);
                            cmd.Parameters.AddWithValue("@JournalNumber", journalNumber);
                            cmd.Parameters.AddWithValue("@JournalDate",   journalDate);
                            cmd.Parameters.AddWithValue("@PostedDateUtc", postedDateUtc);
                            cmd.Parameters.AddWithValue("@SourceId",      (object)sourceId   ?? DBNull.Value);
                            cmd.Parameters.AddWithValue("@SourceType",    (object)sourceType ?? DBNull.Value);
                            await cmd.ExecuteNonQueryAsync();
                        }
                        totalUpserted++;

                        if (journal.TryGetProperty("JournalLines", out var lines))
                        {
                            int lineIndex = 0;
                            foreach (var line in lines.EnumerateArray())
                            {
                                var lineId      = line.GetProperty("JournalLineID").GetString();
                                var accountCode = line.TryGetProperty("AccountCode",  out var accCode) ? accCode.GetString() : null;
                                var netAmount   = GetDecimal(line, "NetAmount");
                                var grossAmount = GetDecimal(line, "GrossAmount");
                                var taxAmount   = GetDecimal(line, "TaxAmount");
                                var taxType     = line.TryGetProperty("TaxType",     out var taxT) ? taxT.GetString() : null;
                                var taxName     = line.TryGetProperty("TaxName",     out var taxN) ? taxN.GetString() : null;
                                var description = line.TryGetProperty("Description", out var desc) ? desc.GetString() : null;
                                var region      = line.TryGetProperty("Region",      out var reg)  ? reg.GetString()  : null;

                                var upsertLine = @"
                                    MERGE [XroJournalLine] AS target
                                    USING (SELECT @JournalLineId AS JournalLineId) AS source
                                    ON target.JournalLineId = source.JournalLineId
                                    WHEN MATCHED THEN UPDATE SET
                                        JournalLineIndex = @JournalLineIndex,
                                        JournalId        = @JournalId,
                                        AccountCode      = @AccountCode,
                                        NetAmount        = @NetAmount,
                                        GrossAmount      = @GrossAmount,
                                        TaxAmount        = @TaxAmount,
                                        TaxType          = @TaxType,
                                        TaxName          = @TaxName,
                                        Description      = @Description,
                                        Region           = @Region
                                    WHEN NOT MATCHED THEN INSERT
                                        (JournalLineId, JournalLineIndex, JournalId, AccountCode, NetAmount, GrossAmount, TaxAmount, TaxType, TaxName, Description, Region)
                                    VALUES
                                        (@JournalLineId, @JournalLineIndex, @JournalId, @AccountCode, @NetAmount, @GrossAmount, @TaxAmount, @TaxType, @TaxName, @Description, @Region);";

                                using (var cmd = new SqlCommand(upsertLine, conn))
                                {
                                    cmd.Parameters.AddWithValue("@JournalLineId",    lineId);
                                    cmd.Parameters.AddWithValue("@JournalLineIndex", lineIndex);
                                    cmd.Parameters.AddWithValue("@JournalId",        journalId);
                                    cmd.Parameters.AddWithValue("@AccountCode",      (object)accountCode ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@NetAmount",        netAmount);
                                    cmd.Parameters.AddWithValue("@GrossAmount",      grossAmount);
                                    cmd.Parameters.AddWithValue("@TaxAmount",        taxAmount);
                                    cmd.Parameters.AddWithValue("@TaxType",          (object)taxType     ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@TaxName",          (object)taxName     ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@Description",      (object)description ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@Region",           (object)region      ?? DBNull.Value);
                                    await cmd.ExecuteNonQueryAsync();
                                }
                                lineIndex++;
                                totalLines++;
                            }
                        }

                        if (journalNumber > offset)
                            offset = journalNumber;
                    }
                }

                _logger.LogInformation("Batch processed. Total upserted so far: {total}", totalUpserted);

                if (journals.GetArrayLength() < 100) break;
            }

            _logger.LogInformation("Upserted journals: {journals}",  totalUpserted);
            _logger.LogInformation("Upserted journal lines: {lines}", totalLines);
            _logger.LogInformation("Xero journal ingest completed at {time}", DateTime.UtcNow);
        }
    }
}