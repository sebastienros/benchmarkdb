using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;
using Npgsql;

namespace BenchmarkDbFull
{
    class Program
    {
        static int Threads = 1024;
        static int Concurrency = 32;
        static int MaxTransactions = 100000000;
        static int Counter = 0;

        const string PostgreSql = nameof(PostgreSql);
        const string MySql = nameof(MySql);
        const string SqlServer = nameof(SqlServer);

        static object synlock = new object();

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("usage: database connectionstring");
                Environment.Exit(1);
            }

            var connectionString = args[1];

            DbProviderFactory factory = null;

            switch (args[0])
            {
                case PostgreSql:
                    factory = NpgsqlFactory.Instance;
                    break;

                case MySql:
                    factory = MySqlClientFactory.Instance;
                    break;

                case SqlServer:
                    factory = SqlClientFactory.Instance;
                    break;

                default:
                    Console.WriteLine($"Acceped database values: {SqlServer}, {MySql}, {PostgreSql}");
                    Environment.Exit(2);
                    break;
            }

            Console.WriteLine($"Running with {args[0]} on {connectionString}");

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var tasks = Enumerable.Range(1, Concurrency).Select(i => Task.Run(async () =>
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
            })).ToList();

            tasks.Add(Task.Delay(TimeSpan.FromSeconds(10)));

            Task.WhenAny(tasks).GetAwaiter().GetResult();

            if (Counter <= 1)
            {
                throw new ApplicationException("Connection strings seems wrong");
            }

            stopwatch.Stop();
            Console.WriteLine($"{Counter} transactions in {stopwatch.Elapsed.TotalSeconds} seconds, {Counter / stopwatch.Elapsed.TotalSeconds} tps");
        }
    }
}
