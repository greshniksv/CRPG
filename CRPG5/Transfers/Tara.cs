using System;
using System.Collections.Generic;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using Npgsql;

namespace CRPG5.Transfers
{
	public static class Tara
	{
		public static bool Transfer(FbCommand fbCmd, NpgsqlConnection pgConn)
		{
			Func.Log(" * Start transfer TARADB", Func.LogType.Information);

			var info = Postgre.ToPostrgeDb(fbCmd, "select IO_ID,IOKM_ID,IOT_ID,ISHOST from ISHOST", pgConn, 
				"ISHOST_tara", 
				"COPY \"ISHOST_tara\" (\"IO_ID\",\"IOKM_ID\",\"IOT_ID\",\"ISHOST\") FROM STDIN", 
				(ref string data,List<string> dataList, int progres) =>
			{
				data = string.Format("{0}	{1}	{2}	{3}\n", dataList[0], dataList[1], dataList[2], dataList[3]);
			});
			if (info == null) return false;
			Func.HtmlReportAdd(info);
			info.Table = " - TARADB";

			var infoAdd = Postgre.ToPostrgeDb(fbCmd, "select K_ID, KLIENT from KLIENT", pgConn,
				"KLIENT_tara",
				"COPY \"KLIENT_tara\" (\"K_ID\",\"KLIENT\") FROM STDIN", 
				(ref string data,List<string> dataList, int progres) =>
			{
				data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
			});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select KM_ID, KMK_ID, KMM_ID, KMS_ID from KLIMAN", pgConn,
				"KLIMAN_tara",
				"COPY \"KLIMAN_tara\" (\"KM_ID\",\"KMK_ID\",\"KMM_ID\",\"KMS_ID\") FROM STDIN", 
				(ref string data,List<string> dataList, int progres) =>
			{
				data = string.Format("{0}	{1}	{2}	{3}\n", dataList[0], dataList[1], dataList[2], dataList[3]);
			});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select M_ID,MANAGER from MANAGER", pgConn,
				"MANAGER_tara",
				"COPY \"MANAGER_tara\" (\"M_ID\",\"MANAGER\") FROM STDIN", 
				(ref string data,List<string> dataList, int progres) =>
			{
				data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
			});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select D_ID, DN_ID, DKM_ID, DT_ID, DRASH, DPRIH, DTSUMR, DTSUMP from MOVE", pgConn,
				"MOVE_tara",
				"COPY \"MOVE_tara\" (\"D_ID\",\"DN_ID\",\"DKM_ID\",\"DT_ID\",\"DRASH\",\"DPRIH\",\"DTSUMR\",\"DTSUMP\") FROM STDIN", 
				(ref string data,List<string> dataList, int progres) =>
			{
				data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}	{7}\n", 
					dataList[0], dataList[1], dataList[2], dataList[3], 
					dataList[4], dataList[5], dataList[6].Replace(',', '.'), dataList[7].Replace(',', '.'));
			});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select N_ID,NDATE,NNUMBER,NKM_ID,NSUMR,NSUMP,NNOTE from NAKLADNA", pgConn,
				"NAKLADNA_tara",
				"COPY \"NAKLADNA_tara\" (\"N_ID\",\"NDATE\",\"NNUMBER\",\"NKM_ID\",\"NSUMR\",\"NSUMP\",\"NNOTE\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					DateTime dt = DateTime.Parse(dataList[1]);

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}\n",
						dataList[0], dt.ToString("yyyy-MM-dd"), dataList[2], dataList[3],
						dataList[4].Replace(',', '.'), dataList[5].Replace(',', '.'), dataList[6]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select S_ID,SKLAD from sklad", pgConn,
				"SKLAD_tara",
				"COPY \"SKLAD_tara\" (\"S_ID\",\"SKLAD\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(fbCmd, "select T_ID,TOVAR,TPRICE from TOVAR", pgConn,
				"TOVAR_tara",
				"COPY \"TOVAR_tara\"(\"T_ID\",\"TOVAR\",\"TPRICE\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}\n", dataList[0], dataList[1], dataList[2].Replace(',', '.'));
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
