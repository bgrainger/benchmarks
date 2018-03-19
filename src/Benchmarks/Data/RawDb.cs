// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Benchmarks.Configuration;
using Microsoft.Extensions.Options;
using MySql.Data.MySqlClient;
using MySqlConnector.Direct;

namespace Benchmarks.Data
{
    public class RawDb : IDb
    {
        private readonly IRandom _random;
        private readonly DbProviderFactory _dbProviderFactory;
        private readonly string _connectionString;
        private readonly ThreadLocal<MySqlSession> _session;

        public RawDb(IRandom random, DbProviderFactory dbProviderFactory, IOptions<AppSettings> appSettings)
        {
            _random = random;
            _dbProviderFactory = dbProviderFactory;
            _connectionString = appSettings.Value.ConnectionString;
            var csb = new MySqlConnectionStringBuilder(_connectionString);
            _session = new ThreadLocal<MySqlSession>(() => {
                var session = new MySqlSession(csb.Server, (int) csb.Port, csb.UserID, csb.Password, csb.Database);
                session.ConnectAsync().GetAwaiter().GetResult();
                return session;
            });
        }

        public async Task<World> LoadSingleQueryRow()
        {
            using (var db = _dbProviderFactory.CreateConnection())
            {
                db.ConnectionString = _connectionString;
                await db.OpenAsync();

                using (var cmd = CreateReadCommand(db))
                {
                    return await ReadSingleRow(db, cmd);
                }
            }
        }

        async Task<World> ReadSingleRow(DbConnection connection, DbCommand cmd)
        {
            using (var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow))
            {
                await rdr.ReadAsync();

                return new World
                {
                    Id = rdr.GetInt32(0),
                    RandomNumber = rdr.GetInt32(1)
                };
            }
        }

        DbCommand CreateReadCommand(DbConnection connection)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT id, randomnumber FROM world WHERE id = @Id";
            var id = cmd.CreateParameter();
            id.ParameterName = "@Id";
            id.DbType = DbType.Int32;
            id.Value = _random.Next(1, 10001);
            cmd.Parameters.Add(id);

            return cmd;
        }

        public async Task<World[]> LoadMultipleQueriesRows(int count)
        {
            var result = new World[count];

            using (var db = _dbProviderFactory.CreateConnection())
            {
                db.ConnectionString = _connectionString;
                await db.OpenAsync();
                using (var cmd = CreateReadCommand(db))
                {
                    for (int i = 0; i < count; i++)
                    {
                        result[i] = await ReadSingleRow(db, cmd);
                        cmd.Parameters["@Id"].Value = _random.Next(1, 10001);
                    }
                }
            }

            return result;
        }

        public async Task<World[]> LoadMultipleUpdatesRows(int count)
        {
            var results = new World[count];

            var updateCommand = new StringBuilder(count);

            using (var db = _dbProviderFactory.CreateConnection())
            {
                db.ConnectionString = _connectionString;
                await db.OpenAsync();

                using (var updateCmd = db.CreateCommand())
                using (var queryCmd = CreateReadCommand(db))
                {
                    for (int i = 0; i < count; i++)
                    {
                        results[i] = await ReadSingleRow(db, queryCmd);
                        queryCmd.Parameters["@Id"].Value = _random.Next(1, 10001);
                    }

                    // Postgres has problems with deadlocks when these aren't sorted
                    Array.Sort<World>(results, (a, b) => a.Id.CompareTo(b.Id));

                    for(int i = 0; i < count; i++)
                    {
                        var id = updateCmd.CreateParameter();
                        id.ParameterName = BatchUpdateString.Strings[i].Id;
                        id.DbType = DbType.Int32;
                        updateCmd.Parameters.Add(id);

                        var random = updateCmd.CreateParameter();
                        random.ParameterName = BatchUpdateString.Strings[i].Random;
                        random.DbType = DbType.Int32;
                        updateCmd.Parameters.Add(random);

                        var randomNumber = _random.Next(1, 10001);
                        id.Value = results[i].Id;
                        random.Value = randomNumber;
                        results[i].RandomNumber = randomNumber;

                        updateCommand.Append(BatchUpdateString.Strings[i].UpdateQuery);
                    }

                    updateCmd.CommandText = updateCommand.ToString();

                    await updateCmd.ExecuteNonQueryAsync();
                }
            }

            return results;
        }

        public async Task<IEnumerable<Fortune>> LoadFortunesRows()
        {
            var result = new List<Fortune>(13);
            var session = _session.Value;
            await session.ExecuteAsync("SELECT * FROM fortune;");
            while (await session.ReadAsync())
            {
                result.Add(new Fortune
                {
                    Id = session.ReadInt32(),
                    Message = session.ReadString(),
                });
            }

            result.Add(new Fortune { Message = "Additional fortune added at request time." });
            result.Sort();

            return result;
        }

        public IEnumerable<Fortune> LoadFortunesRowsSync()
        {
            var result = new List<Fortune>();

            using (var db = _dbProviderFactory.CreateConnection())
            using (var cmd = db.CreateCommand())
            {
                cmd.CommandText = "SELECT id, message FROM fortune";

                db.ConnectionString = _connectionString;
                db.Open();

                // Prepared statements improve PostgreSQL performance by 10-15%
                cmd.Prepare();

                using (var rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                {
                    while (rdr.Read())
                    {
                        result.Add(new Fortune
                        {
                            Id = rdr.GetInt32(0),
                            Message = rdr.GetString(1)
                        });
                    }
                }
            }

            result.Add(new Fortune { Message = "Additional fortune added at request time." });
            result.Sort();

            return result;
        }
    }
}
