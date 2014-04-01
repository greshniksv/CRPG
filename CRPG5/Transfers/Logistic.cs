using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using Npgsql;

namespace CRPG5.Transfers
{
	class Logistic
	{

		public static bool Transfer(SqlCommand msCmd, NpgsqlConnection pgConn)
		{
			Func.Log(" * Start transfer LOGISTIC", Func.LogType.Information);

			var info = Postgre.MsToPostrgeDb(msCmd, "SELECT urId, crId FROM tblCustomerBindings join tblCustomers c on c.id = cbCustomerId join tblUsers s on s.id = cbUserId", pgConn,
				"tblCustomerBindings",
				"COPY \"tblCustomerBindings\"(\"cbUserId\",\"cbCustomerId\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			if (info == null) return false;
			Func.HtmlReportAdd(info);
			info.Table = " - LOGISTIC";

			var infoAdd = Postgre.MsToPostrgeDb(msCmd, "SELECT sbShopId,urId FROM tblShopBindings sb, tblUsers u where sbUserId=u.id", pgConn,
				"tblShopBindings",
				"COPY \"tblShopBindings\"(\"sbShopId\",\"sbUserId\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.MsToPostrgeDb(msCmd, "SELECT u.urId ,c.crId FROM tblProtect join tblUsers u on u.id = prUserId join tblCustomers c on c.id = prCustomerId", pgConn,
				"tblProtect",
				"COPY \"tblProtect\"(\"prUserId\",\"prCustomerId\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.MsToPostrgeDb(msCmd, "select s.id,crId,sName,dbo.GetShopFullAddress(s.id),coalesce(sActive,'0') as sActive from tblShops s, tblCustomers c where sCustomerId = c.id", pgConn,
				"tblShops",
				"COPY \"tblShops\"(\"id\",\"sCustomerId\",\"sName\",\"sAddress\",\"sActive\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
					{
						var dl3 = dataList[3];
						string dl3Out = string.Empty;
						for (int i = 0; i < dl3.Length; i++)
						{
							if (dl3[i] == '\'')
							{
								dl3Out += @"\" + @"'";
							}
							else dl3Out += dl3[i];
						}

						var dl4 = dataList[4];
						string dl4Out = string.Empty;
						for (int i = 0; i < dl4.Length; i++)
						{
							if (dl4[i] == '\'')
							{
								dl4Out += @"\" + @"'";
							}
							else dl4Out += dl4[i];
						}

					data = string.Format("{0}	{1}	{2}	{3}	{4}\n", 
						dataList[0], dataList[1], dataList[2],
						dl3Out, dl4Out);
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
