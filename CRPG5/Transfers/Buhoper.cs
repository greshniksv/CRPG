using System;
using System.Collections.Generic;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using Npgsql;

namespace CRPG5.Transfers
{
	public static class Buhoper
	{
		public static bool Transfer(FbCommand fbCmd, NpgsqlConnection pgConn)
		{
			Func.Log(" * Start transfer BUHOPER", Func.LogType.Information);

			var info = Postgre.ToPostrgeDb(fbCmd, "select ID,LOG,PAS,PERM from AUTH", pgConn,
				"AUTH_buh",
				"COPY \"AUTH_buh\" (\"ID\",\"LOG\",\"PAS\",\"PERM\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}\n", dataList[0], dataList[1], dataList[2], dataList[3]);
				});
			if (info == null) return false;
			Func.HtmlReportAdd(info);
			info.Table = " - BUHOPER";


			var infoAdd = Postgre.ToPostrgeDb(fbCmd, "select ID,NAME,ADDRESS,PERSON,PHONE,MFO,OKPO,SCHET,INFO from CLIENT", pgConn,
				"CLIENT_buh",
				"COPY \"CLIENT_buh\" (\"ID\",\"NAME\",\"ADDRESS\",\"PERSON\",\"PHONE\",\"MFO\",\"OKPO\",\"SCHET\",\"INFO\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}	{7}	{8}\n",
						dataList[0], dataList[1], dataList[2], dataList[3], dataList[4],
						dataList[5], dataList[6], dataList[7], dataList[8]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select ID,ID_CLIENT,ID_TP,ID_SKLAD,ID_OPER,DAT,DAT_INP,NUM,VAL from OPER", pgConn,
				"OPER_buh",
				"COPY \"OPER_buh\" (\"ID\",\"ID_CLIENT\",\"ID_TP\",\"ID_SKLAD\",\"ID_OPER\",\"DAT\",\"DAT_INP\",\"NUM\",\"VAL\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}	{7}	{8}\n",
						dataList[0], dataList[1], dataList[2], dataList[3], dataList[4],
						DateTime.Parse(dataList[5]).ToString("yyyy-MM-dd"),
						DateTime.Parse(dataList[6]).ToString("yyyy-MM-dd"), dataList[7],
						dataList[8].Replace(',', '.'));
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select ID,ID_CLIENT,ID_TP,ID_SKLAD,SALDO,SALDOF,SALDOT,SALDOFT,OTSR,CRED from SALDO", pgConn,
				"SALDO_buh",
				"COPY \"SALDO_buh\" (\"ID\",\"ID_CLIENT\",\"ID_TP\",\"ID_SKLAD\",\"SALDO\",\"SALDOF\",\"SALDOT\",\"SALDOFT\",\"OTSR\",\"CRED\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}	{7}	{8}	{9}\n",
						dataList[0], dataList[1], dataList[2], dataList[3], dataList[4].Replace(',', '.'),
						dataList[5].Replace(',', '.'), dataList[6].Replace(',', '.'),
						dataList[7].Replace(',', '.'), dataList[8], dataList[9]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select ID,NAME from SKLAD", pgConn,
				"SKLAD_buh",
				"COPY \"SKLAD_buh\"(\"ID\",\"NAME\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select ID,NAME from TP", pgConn,
				"TP_buh",
				"COPY \"TP_buh\" (\"ID\",\"NAME\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			Func.HtmlReportAdd(info);

			return true;
		}

	}
}
