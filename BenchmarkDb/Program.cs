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
        static int Threads = 1024;
        static int MaxTransactions = 100000000;
        static int Counter = 0;

        static object synlock = new object();

        static void Main(string[] args)
        {
            //if (args.Length > 2)
            //{
            //    int.TryParse(args[1], out MaxThreads);
            //    int.TryParse(args[2], out TransactionsPerThread);
            //}

            //DbProviderFactory factory = Npgsql.NpgsqlFactory.Instance;
            //var connectionString = "Server=172.16.228.78;Database=hello_world;User Id=benchmarkdbuser;Password=benchmarkdbpass;Maximum Pool Size=1024;NoResetOnClose=true";

            DbProviderFactory factory = System.Data.SqlClient.SqlClientFactory.Instance;
            var connectionString = "Server=172.16.228.78;Database=hello_world;User Id=sa;Password=Benchmarkdbp@55;";
            //var connectionString = "Server=SEBROS-Z440;Database=benchmarks;User Id=sa;Password=Demo123!;";

            //DbProviderFactory factory = MySql.Data.MySqlClient.MySqlClientFactory.Instance;
            //var connectionString = "Server=172.16.228.78;Database=hello_world;User Id=benchmarkdbuser;Password=benchmarkdbpass;Maximum Pool Size=1024;";

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var tasks = Enumerable.Range(1, Threads).Select(i =>
            {
                return Thing();

                async Task Thing()
                {
                    while (Interlocked.Add(ref Counter, 1) < MaxTransactions)
                    {
                        using (var connection = factory.CreateConnection())
                        {
                            connection.ConnectionString = connectionString;
                            var results = await connection.QueryAsync("SELECT id,message FROM fortune");

                            if (results.Count() != 12)
                            {
                                throw new ApplicationException();
                            }
                        }
                    }
                }
            }).ToList();


            tasks.Add(Task.Delay(TimeSpan.FromSeconds(10)));

            Task.WhenAny(tasks).GetAwaiter().GetResult();

            if (Counter <= 1)
            {
                throw new ApplicationException("Connection strings seems wrong");
            }

            stopwatch.Stop();
            Console.WriteLine($"{Counter / stopwatch.Elapsed.TotalSeconds}");
        }
    }
}
