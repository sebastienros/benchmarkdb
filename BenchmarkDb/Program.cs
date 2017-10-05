using System;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace BenchmarkDb
{
    class Program
    {
        static int MaxThreads = 128;
        static int MaxTransactions = MaxThreads * 200;
        static long Counter = 0;

        static object synlock = new object();

        static void Main(string[] args)
        {
            if (args.Length > 2)
            {
                int.TryParse(args[1], out MaxThreads);
                int.TryParse(args[2], out MaxTransactions);
            }

            //DbProviderFactory factory = Npgsql.NpgsqlFactory.Instance;
            //var connectionString = "Server=172.16.228.78;Database=hello_world;User Id=benchmarkdbuser;Password=benchmarkdbpass;Maximum Pool Size=1024;NoResetOnClose=true";

            //DbProviderFactory factory = MySql.Data.MySqlClient.MySqlClientFactory.Instance;
            //var connectionString = "Server=172.16.228.78;Database=hello_world;User Id=benchmarkdbuser;Password=benchmarkdbpass";

            DbProviderFactory factory = MySql.Data.MySqlClient.MySqlClientFactory.Instance;
            var connectionString = "Server=172.16.228.78;Database=hello_world;User Id=benchmarkdbuser;Password=benchmarkdbpass";

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var tasks = Enumerable.Range(1, MaxThreads).Select(i =>
            {
                return Thing();

                async Task Thing()
                {
                    while (Interlocked.Add(ref Counter, 1) < MaxTransactions)
                    {
                        using (var connection = factory.CreateConnection())
                        {
                            connection.ConnectionString = connectionString;
                            await connection.QueryAsync("SELECT id,message FROM fortune");
                        }
                    }
                }
            });

            Task.WhenAll(tasks).GetAwaiter().GetResult();

            stopwatch.Stop();
            Console.WriteLine($"{MaxTransactions / stopwatch.Elapsed.TotalSeconds}");
        }
    }
}
