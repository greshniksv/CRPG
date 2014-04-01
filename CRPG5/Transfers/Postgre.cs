using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using Npgsql;

namespace CRPG5.Transfers
{
	public static class Postgre
	{
		private static Stopwatch stopwatch = new Stopwatch();

		public class TransferedInfo
		{
			public int RowCount { get; set; }
			public long Time { get; set; }
			public string Table { get; set; }
		}

		public delegate void CallBackDelegate(ref string data, List<string> param, int progressPersent);

		public static TransferedInfo ToPostrgeDb(MpRajah mpRajah, FbCommand fbCmd, string fbSql, NpgsqlConnection pgConn, string table, string pgSql, CallBackDelegate callback)
		{
			return ToPostrgeDb(fbCmd, mpRajah.FixQuery(fbSql), pgConn, table, pgSql, callback);
		}

		public static TransferedInfo ToPostrgeDb(FbCommand fbCmd, string fbSql, NpgsqlConnection pgConn, string table, string pgSql, CallBackDelegate callback)
		{
			Func.Log("Transfer table [ "+table+" ]\t",Func.LogType.Information,false);

			string dataInfo2Error = string.Empty;
			var transferedInfo = new TransferedInfo();
			int rowCount = 0;
			int curRow = 0;
			FbDataReader re = null;
			stopwatch.Reset();
			stopwatch.Start();

#if !DEBUG
			try
			{
#endif
				// get row count
				fbCmd.CommandText = fbSql;
				re = fbCmd.ExecuteReader();
				while (re.Read())
					rowCount++;
				re.Close();
				transferedInfo.RowCount = rowCount;
				transferedInfo.Table = table;
#if !DEBUG
			}
			catch (Exception ex)
			{
				Func.Log("Error get row count. Table:"+table+". fbSql:"+fbSql+". Exception:"+ex, Func.LogType.Error);
				return null;
			}
#endif


			NpgsqlCommand pgCmd = pgConn.CreateCommand();
			pgCmd.CommandTimeout = 9999999;

#if !DEBUG
			try
			{
#endif
			pgCmd.CommandText = "truncate \"" + table + "\" ";
			pgCmd.ExecuteNonQuery();
#if !DEBUG
			}
			catch (Exception ex)
			{
				Func.Log("Error trancate postgre table: "+table, Func.LogType.Error);
				return null;
			}
#endif


			var command = new NpgsqlCommand(pgSql, pgConn);
			var cin = new NpgsqlCopyIn(command, pgConn);
			cin.Start();
			Stream copyInStream = cin.CopyStream;
			Encoding inEncoding = Encoding.UTF8;

			fbCmd.CommandText = fbSql;
			re = fbCmd.ExecuteReader();
			while (re.Read())
			{
#if !DEBUG
				try
				{
#endif
					dataInfo2Error = string.Empty;
					string data = string.Empty;
					
					var paramList = new List<string>();
					for (int i = 0; i < re.FieldCount; i++)
					{
						paramList.Add(re[i].ToString().Trim());
						dataInfo2Error += re.GetName(i)+"-["+re[i].ToString().Trim()+"] ";
					}

					callback(ref data, paramList,(curRow/100*rowCount));
					var buf = inEncoding.GetBytes(data);
					copyInStream.Write(buf, 0, buf.Length);
#if !DEBUG
				}
				catch (Exception ex)
				{
					Func.Log("Error tarnsfer data. Table:" + table + ". FbSql:" + fbSql + ". Data:[" + dataInfo2Error + "] Exception:" + ex, Func.LogType.Error); 
					return null;
				}
#endif
					curRow++;
			}
			re.Close();
			copyInStream.Close();
			cin.End();
			stopwatch.Stop();
			transferedInfo.Time = stopwatch.ElapsedMilliseconds;

			Console.Write(" Time: " + stopwatch.Elapsed.Hours + ":" + stopwatch.Elapsed.Minutes + ":" + stopwatch.Elapsed.Seconds + "." + stopwatch.Elapsed.Milliseconds + " Rows: " + transferedInfo.RowCount + " Speed: " + (stopwatch.Elapsed.Seconds!=0?(transferedInfo.RowCount/stopwatch.Elapsed.Seconds).ToString():"0/0") + " \n");
			return transferedInfo;
		}


		public static TransferedInfo MsToPostrgeDb(SqlCommand msCmd, string msSql, NpgsqlConnection pgConn, string table, string pgSql, CallBackDelegate callback)
		{
			Func.Log("Transfer table [ " + table + " ]\t", Func.LogType.Information, false);

			string dataInfo2Error = string.Empty;
			var transferedInfo = new TransferedInfo();
			int rowCount = 0;
			int curRow = 0;
			SqlDataReader re = null;
			var stopwatch = new Stopwatch();
			stopwatch.Start();

#if !DEBUG
			try
			{
#endif
			// get row count
			msCmd.CommandText = msSql;
			re = msCmd.ExecuteReader();
			while (re.Read())
				rowCount++;
			re.Close();
			transferedInfo.RowCount = rowCount;
			transferedInfo.Table = table;
#if !DEBUG
			}
			catch (Exception ex)
			{
				Func.Log("Error get row count. Table:"+table+". msSql:"+msSql+". Exception:"+ex, Func.LogType.Error);
				return null;
			}
#endif


			NpgsqlCommand pgCmd = pgConn.CreateCommand();
			pgCmd.CommandTimeout = 9999999;

#if !DEBUG
			try
			{
#endif
			pgCmd.CommandText = "truncate \"" + table + "\" ";
			pgCmd.ExecuteNonQuery();
#if !DEBUG
			}
			catch (Exception ex)
			{
				Func.Log("Error trancate postgre table: "+table, Func.LogType.Error);
				return null;
			}
#endif


			var command = new NpgsqlCommand(pgSql, pgConn);
			var cin = new NpgsqlCopyIn(command, pgConn);
			cin.Start();
			Stream copyInStream = cin.CopyStream;
			Encoding inEncoding = Encoding.UTF8;

			msCmd.CommandText = msSql;
			re = msCmd.ExecuteReader();
			while (re.Read())
			{
#if !DEBUG
				try
				{
#endif
				dataInfo2Error = string.Empty;
				string data = string.Empty;

				var paramList = new List<string>();
				for (int i = 0; i < re.FieldCount; i++)
				{
					paramList.Add(re[i].ToString().Trim());
					dataInfo2Error += re.GetName(i) + "-[" + re[i].ToString().Trim() + "] ";
				}

				callback(ref data, paramList, (curRow / 100 * rowCount));
				var buf = inEncoding.GetBytes(data);
				copyInStream.Write(buf, 0, buf.Length);
#if !DEBUG
				}
				catch (Exception ex)
				{
					Func.Log("Error tarnsfer data. Table:" + table + ". MsSql:" + msSql + ". Data:[" + dataInfo2Error + "] Exception:" + ex, Func.LogType.Error); 
					return null;
				}
#endif
				curRow++;
			}
			re.Close();
			copyInStream.Close();
			cin.End();
			stopwatch.Stop();
			transferedInfo.Time = stopwatch.ElapsedMilliseconds;

			Console.Write(" Time: " + stopwatch.Elapsed.Hours + ":" + stopwatch.Elapsed.Minutes + ":" + stopwatch.Elapsed.Seconds + "." + stopwatch.Elapsed.Milliseconds + " Rows: " + transferedInfo.RowCount + "\n");
			return transferedInfo;
		}

		public static void Lock(NpgsqlConnection pgConn, bool turn)
		{
			NpgsqlCommand pgCmd = pgConn.CreateCommand();
			pgCmd.CommandTimeout = 9999999;

			if (turn)
			{
				pgCmd.CommandText = "update \"tblDBState\" set \"Value\"='1' where \"ParamName\"='CrpgOn'";
				pgCmd.ExecuteNonQuery();
			}
			else
			{
				pgCmd.CommandText = "update \"tblDBState\" set \"Value\"='0' where \"ParamName\"='CrpgOn'";
				pgCmd.ExecuteNonQuery();
			}
		}

		public static void Maintenans(NpgsqlConnection pgConn)
		{
			NpgsqlCommand pgCmd = pgConn.CreateCommand();
			pgCmd.CommandTimeout = 9999999;

			var tableList = new List<string>();
			pgCmd.CommandText = "select tablename from \"pg_tables\" where schemaname='public'";
			NpgsqlDataReader re2 = pgCmd.ExecuteReader();
			while (re2.Read())
			{
				tableList.Add(re2[0].ToString());
			}
			re2.Close();

			var indexList = new List<string>();
			pgCmd.CommandText = "SELECT " +
					" c.relname as \"Name\" " +
					" FROM pg_catalog.pg_class c " +
					"    JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid " +
					"    JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid " +
					"    LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner " +
					"    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
					" WHERE c.relkind IN ('i','') " +
					"     AND n.nspname NOT IN ('pg_catalog', 'pg_toast') " +
					"     AND pg_catalog.pg_table_is_visible(c.oid) " +
					" ORDER BY 1;";
			re2 = pgCmd.ExecuteReader();
			while (re2.Read())
			{
				indexList.Add(re2[0].ToString());
			}
			re2.Close();

			pgCmd.CommandTimeout = 800000;
			for (int i = 0; i < tableList.Count; i++)
			{
				pgCmd.CommandText = string.Format("VACUUM VERBOSE ANALYZE \"{0}\"", tableList[i]);
				pgCmd.ExecuteNonQuery();
				Func.Log("Vacuum table [" + tableList[i] + "]", Func.LogType.Information);
			}

			for (int i = 0; i < indexList.Count; i++)
			{
				pgCmd.CommandText = string.Format("REINDEX INDEX \"{0}\"", indexList[i]);
				pgCmd.ExecuteNonQuery();
				Func.Log("Reindex index [" + indexList[i] + "]", Func.LogType.Information);
			}
		}



	}
}
