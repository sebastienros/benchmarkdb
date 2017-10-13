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
        static volatile int Counter = 0;

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
                    Console.WriteLine($"Accepted database values: {SqlServer}, {MySql}, {PostgreSql}");
                    Environment.Exit(2);
                    break;
            }

            Console.WriteLine($"Running with {args[0]} on {connectionString}");

            var stopwatch = new Stopwatch();
            var tasks = new List<Task>();
            var stopping = false;
            var startTime = DateTime.UtcNow;
            var lastDisplay = DateTime.UtcNow;
            var lastNewTask = DateTime.UtcNow;

            while (!stopping)
            {
                Thread.Sleep(200);
                var now = DateTime.UtcNow;

                if ((now - lastDisplay) > TimeSpan.FromMilliseconds(200))
                {
                    Console.Write($"{tasks.Count} Threads, {Counter / (now - lastDisplay).TotalSeconds} tps                                   ");
                    Console.CursorLeft = 0;
                    lastDisplay = now;
                    Counter = 0;

                }

                if ((now - lastNewTask) > TimeSpan.FromMilliseconds(2000))
                {
                    tasks.Add(Task.Run(Thing));

                    async Task Thing()
                    {
                        while (!stopping)
                        {
                            Interlocked.Add(ref Counter, 1);

                            try
                            {
                                using (var connection = factory.CreateConnection())
                                {
                                    connection.ConnectionString = connectionString;
                                    var results = await connection.QueryAsync("SELECT id,message FROM fortune");

                                    if (results.Count() != 12)
                                    {
                                        throw new ApplicationException("Not 12");
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }
                        }
                    }

                    lastNewTask = now;
                }
            }

            Task.WhenAll(tasks).GetAwaiter().GetResult();
        }
    }
}
