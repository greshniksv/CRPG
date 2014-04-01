using System;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.Threading;
using CRPG5.Transfers;
using FirebirdSql.Data.FirebirdClient;
using Npgsql;

namespace CRPG5
{
	class Program
	{
		private static int _rajahInitialiger;
		private static int _returnCode;

		static int Main(string[] args)
		{
			Func.WorkDir = Directory.GetCurrentDirectory();
			if(!File.Exists(Func.WorkDir+@"/CRPG5.cfg"))
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("ERROR find CRPG5.cfg");
				Thread.Sleep(1000);
				Console.ResetColor();
				return 101;
			}

			fnConfig3.Config.Initialization(Func.WorkDir+@"/CRPG5.cfg");
			Func.Log(" -= CRPG 5 =- v." + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version,Func.LogType.Information);

			Func.WorkLocation = Dns.GetHostName();
			Func.Log("Detect hostname: "+Func.WorkLocation,Func.LogType.Information);

			// Rajah
			Func.RCharset = fnConfig3.Config.GetValue("charset", "WIN1251", "Rajah");
			Func.RUserID = fnConfig3.Config.GetValue("user", "SYSDBA", "Rajah");
			Func.RPassword = fnConfig3.Config.GetValue("password", "masterkey", "Rajah");
			Func.RDatabase = fnConfig3.Config.GetValue("database", "OBOLON", "Rajah");
			Func.RDataSource = fnConfig3.Config.GetValue("host", "db2.poisk.lg.ua", "Rajah");
			Func.RTransfer = fnConfig3.Config.GetValue("transfer", "yes", "Rajah").ToLower() == "yes" ? true : false;

			// Buhoper
			Func.BCharset = fnConfig3.Config.GetValue("charset", "WIN1251", "Buhoper");
			Func.BUserID = fnConfig3.Config.GetValue("user", "SYSDBA", "Buhoper");
			Func.BPassword = fnConfig3.Config.GetValue("password", "masterkey", "Buhoper");
			Func.BDatabase = fnConfig3.Config.GetValue("database", "BUHOPER", "Buhoper");
			Func.BDataSource = fnConfig3.Config.GetValue("host", "tdb.poisk.lg.ua", "Buhoper");
			Func.BTransfer = fnConfig3.Config.GetValue("transfer", "yes", "Buhoper").ToLower() == "yes" ? true : false;

			// Tara
			Func.TCharset = fnConfig3.Config.GetValue("charset", "WIN1251", "Tara");
			Func.TUserID = fnConfig3.Config.GetValue("user", "SYSDBA", "Tara");
			Func.TPassword = fnConfig3.Config.GetValue("password", "masterkey", "Tara");
			Func.TDatabase = fnConfig3.Config.GetValue("database", "TARA", "Tara");
			Func.TDataSource = fnConfig3.Config.GetValue("host", "tdb.poisk.lg.ua", "Tara");
			Func.TTransfer = fnConfig3.Config.GetValue("transfer", "yes", "Tara").ToLower() == "yes" ? true : false;

			// MsSql
			Func.MUserID = fnConfig3.Config.GetValue("user", "user", "MsSql");
			Func.MPassword = fnConfig3.Config.GetValue("password", "1", "MsSql");
			Func.MDatabase = fnConfig3.Config.GetValue("database", "itdb.server", "MsSql");
			Func.MDataSource = fnConfig3.Config.GetValue("host", "sql2.poisk.lg.ua", "MsSql");
			Func.MTransfer = fnConfig3.Config.GetValue("transfer", "yes", "MsSql").ToLower() == "yes" ? true : false;

			// Postre
			Func.PUserID = fnConfig3.Config.GetValue("user", "postgres", "PostgreSql");
			Func.PPassword = fnConfig3.Config.GetValue("password", "ldyqfx", "PostgreSql");
			Func.PDatabase = fnConfig3.Config.GetValue("database", "ALLDB", "PostgreSql");
			Func.PDataSource = fnConfig3.Config.GetValue("host", "reports.poisk.lg.ua", "PostgreSql");
			Func.PDataSourcePort = fnConfig3.Config.GetValue("port", "5432", "PostgreSql");
			Func.PTransfer = fnConfig3.Config.GetValue("transfer", "yes", "PostgreSql").ToLower() == "yes" ? true : false;

			// Mail
			Func.MailFrom = fnConfig3.Config.GetValue("mailFrom", "alerts.poisk.lg.ua", "Mail");
			Func.MailHost = fnConfig3.Config.GetValue("host", "mail.iteam.net.ua", "Mail");
			Func.MailPort = Int32.Parse(fnConfig3.Config.GetValue("port", "25", "Mail"));
			Func.MailUser = fnConfig3.Config.GetValue("user", "user", "Mail");
			Func.MailPassword = fnConfig3.Config.GetValue("password", "1", "Mail");
			Func.MailSsl = fnConfig3.Config.GetValue("ssl", "no", "Mail").ToLower() == "yes" ? true : false;
			Func.MailTo = fnConfig3.Config.GetValue("mailTo", "greshnik-sv@yandex.ru", "Mail");
			Func.MailToAdmin = fnConfig3.Config.GetValue("mailToAdmin", "greshnik-sv@yandex.ru", "Mail");


			FbConnection fbRajahConn;
			FbCommand fbRajahCmd;

			FbConnection fbTaraConn;
			FbCommand fbTaraCmd;

			FbConnection fbBuhConn;
			FbCommand fbBuhCmd;

			SqlConnection msConn;
			SqlCommand msCmd;

			NpgsqlConnection pgConn;

			Func.Log("Connect to Firedird Rajah database [ " + Func.RDataSource + " : " + Func.RDatabase + " ]", Func.LogType.Information);
			try
			{
				var fbCsb = new FbConnectionStringBuilder
				{
					Dialect = 3,
					Charset = Func.RCharset,
					UserID = Func.RUserID,
					Password = Func.RPassword,
					Database = Func.RDatabase,
					DataSource = Func.RDataSource
				};

				fbRajahConn = new FbConnection(fbCsb.ConnectionString);

				fbRajahConn.Open();
				fbRajahCmd = fbRajahConn.CreateCommand();
				fbRajahCmd.CommandTimeout = Int32.Parse("99999");
			}
			catch (Exception ex)
			{
				Func.Log("Error connect to Filrebird RAJAH. Ex:" + ex, Func.LogType.Error); return 1; 
			}

			Func.Log("Connect to Firedird Tara database [ " + Func.TDataSource + " : " + Func.TDatabase + " ]", Func.LogType.Information);
			try
			{
				var fbCsb = new FbConnectionStringBuilder
				{
					Dialect = 3,
					Charset = Func.TCharset,
					UserID = Func.TUserID,
					Password = Func.TPassword,
					Database = Func.TDatabase,
					DataSource = Func.TDataSource
				};

				fbTaraConn = new FbConnection(fbCsb.ConnectionString);

				fbTaraConn.Open();
				fbTaraCmd = fbTaraConn.CreateCommand();
				fbTaraCmd.CommandTimeout = Int32.Parse("99999");
			}
			catch (Exception ex)
			{
				Func.Log("Error connect to Filrebird TARA. Ex:" + ex, Func.LogType.Error); return 1;
			}

			Func.Log("Connect to Firedird Buhoper database [ " + Func.BDataSource + " : " + Func.BDatabase + " ]", Func.LogType.Information);
			try
			{
				var fbCsb = new FbConnectionStringBuilder
				{
					Dialect = 3,
					Charset = Func.BCharset,
					UserID = Func.BUserID,
					Password = Func.BPassword,
					Database = Func.BDatabase,
					DataSource = Func.BDataSource
				};

				fbBuhConn = new FbConnection(fbCsb.ConnectionString);

				fbBuhConn.Open();
				fbBuhCmd = fbBuhConn.CreateCommand();
				fbBuhCmd.CommandTimeout = Int32.Parse("99999");
			}
			catch (Exception ex)
			{
				Func.Log("Error connect to Filrebird BUHOPER. Ex:" + ex, Func.LogType.Error); return 1;
			}

			Func.Log("Connect to MsSql database [ " + Func.MDataSource + " : " + Func.MDatabase + " ]", Func.LogType.Information);
			try
			{
				var sqlCsb = new SqlConnectionStringBuilder
				{
					UserID = Func.MUserID,
					Password = Func.MPassword,
					InitialCatalog = Func.MDatabase,
					DataSource = Func.MDataSource
				};

				msConn = new SqlConnection(sqlCsb.ConnectionString);

				msConn.Open();
				msCmd = msConn.CreateCommand();
				msCmd.CommandTimeout = 9999999;
			}
			catch (Exception ex)
			{
				Func.Log("Error connect to MsSql. Ex:" + ex, Func.LogType.Error); return 1;
			}

			Func.Log("Connect to PostgreSql database [ " + Func.PDataSource + " : " + Func.PDatabase + " ]", Func.LogType.Information);
			
			try
			{
				string connstring = String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};",
									Func.PDataSource, Func.PDataSourcePort, Func.PUserID, Func.PPassword, Func.PDatabase);
				pgConn = new NpgsqlConnection(connstring);
				pgConn.Open();
			}
			catch (Exception ex) { Func.Log("Error connect to PostgreSql. Ex:" + ex, Func.LogType.Error); return 1; }
			

			var mpRajah = new MpRajah();
			
			Func.Log("Initialize database.",Func.LogType.Information,false);
			mpRajah.InitialProgress += (MpRajahInitialProgress);
			mpRajah.Initialization(fbRajahCmd);
			Console.Write(".\n");

			Postgre.Lock(pgConn, true);

			if(Func.TTransfer)
			    if(!Tara.Transfer(fbTaraCmd,pgConn))
				{ _returnCode = 2; }

			if (Func.BTransfer)
			    if(!Buhoper.Transfer(fbBuhCmd, pgConn))
				{ _returnCode = 3; }

			if (Func.RTransfer)
				if(!Rajah.Transfer(fbRajahCmd, pgConn, mpRajah))
				{ _returnCode = 4; }

			if (Func.MTransfer)
				if(!Logistic.Transfer(msCmd, pgConn))
				{ _returnCode = 5; }
				
			Postgre.Maintenans(pgConn);

			Postgre.Lock(pgConn, false);

			fbBuhConn.Close();
			fbRajahConn.Close();
			fbTaraConn.Close();
			pgConn.Close();
			msConn.Close();

			var report = Func.CreateHtmlReport();
			Func.SendMail(Func.MailTo, Func.MailFrom, Func.MailFrom, null, _returnCode != 0 ? "Ошибка регламентной отчетности" : "Обновление данных регламентной отчетности завершено", _returnCode != 0 ? "Регламентная отчетность не работает в связи с возникшими проблемами. Администраторы разбираются с проблемой." : "В регламентной отчетности свежие данные. ");
			Func.SendMail(Func.MailToAdmin, Func.MailFrom, Func.MailFrom, null, Func.HtmlReportHead + ". @: " + Func.WorkLocation, report);

			Func.Log("Finish",Func.LogType.Information);
			//Console.ReadKey();

			return _returnCode;
		}

		static void MpRajahInitialProgress(int persent)
		{
			//Console.SetCursorPosition(0,Console.CursorTop);
			//Console.Write("MPRajah initialization "+persent+"%"          );

			if (Int32.Parse((persent / 10).ToString()) != _rajahInitialiger)
			{
				_rajahInitialiger = Int32.Parse((persent / 10).ToString());
				Console.Write(".");
			}
		}



		//static void UnusesFunc()
		//{

			//    private class Payer
			//{
			//    public string Id { get; set; }
			//    public string Name { get; set; }
			//    public string NaklNum { get; set; }
			//    public string Date { get; set; }
			//}

			//private class ShopInfo
			//{
			//    public List<Payer> Payers { get; set; }
			//    public string Shop { get; set; }
			//    public string ShopName { get; set; }
			//}

			//Console.WriteLine("\nGet count of data");
			//int count = 0;
			////fbCmd.CommandText = mpRajah.FixQuery("select count(nn.payer0)" +
			////                    " from nakladna0 nn,nakldetail0 nnd, legal0 ll " +
			////                    " where ll.id = nn.payer0 and nnd.sklad0 = '0000017F' and nn.id = nnd.invoce0 and nn.DELETED=0"+
			////                    " and nn.deleted=0 and nnd.deleted=0 and nn.name0%i = 'РАСХОДНАЯ НАКЛАДНАЯ'" + 
			////                    " group by nn.payer0");

			////FbDataReader dataReader = fbCmd.ExecuteReader();

			////if (dataReader.Read())
			////    count = Int32.Parse(dataReader[0].ToString());
			////dataReader.Close();

			//Console.WriteLine("Extract data"); //nn.comment0
			//fbCmd.CommandText = mpRajah.FixQuery("select nn.payer0, ll.name0%i, nn.osnov0, nn.osnov0 ,nn.number0%i, date_to_str( nn.date0 )" +
			//                    " from nakladna0 nn,nakldetail0 nnd, legal0 ll " +
			//                    " where ll.id = nn.payer0 and nnd.sklad0 = '0000017H' and nn.id = nnd.invoce0 and nn.DELETED=0" +
			//                    " and nn.deleted=0 and nnd.deleted=0 and nn.name0%i = 'РАСХОДНАЯ НАКЛАДНАЯ'" +
			//                    " and cast(date_to_str(nn.date0) as date) >='01.05.2011' " +
			//                    " and cast(date_to_str(nn.date0) as date) <='20.10.2011' " +
			//                    " group by nn.payer0, ll.name0%i, nn.comment0, nn.osnov0 ,nn.number0%i, date_to_str( nn.date0 ) " +
			//                    " order by nn.comment0,ll.name0%i ");
			//// obolonLug = '0000017F' Sever = '0000017H'

			//FbDataReader dataReader = fbCmd.ExecuteReader();
			//int counter = 0;
			//while (dataReader.Read())
			//{
			//    counter++;

			//    Console.SetCursorPosition(0, Console.CursorTop);
			//    Console.Write("[" + counter + "/" + count + "]         ");

			//    int shopId = 0;
			//    try
			//    {
			//        string sh = dataReader[2].ToString();
			//        shopId = Int32.Parse(sh.Substring(sh.IndexOf("[") + 1, sh.IndexOf("]") - sh.IndexOf("[") - 1));
			//    }
			//    catch (Exception) { }

			//    if (shopId <= 0)
			//        continue;


			//    bool shopExist = false;
			//    foreach (var info in shopInfo)
			//    {
			//        if (info.Shop == shopId.ToString())
			//        {
			//            shopExist = true;
			//            bool payerExist = false;
			//            foreach (var payer in info.Payers)
			//            {
			//                if (payer.Id == dataReader[0].ToString())
			//                    payerExist = true;
			//            }

			//            if (!payerExist)
			//            {
			//                info.Payers.Add(new Payer() { Id = dataReader[0].ToString(), Name = dataReader[1].ToString(), NaklNum = dataReader[4].ToString(), Date = dataReader[5].ToString() });
			//                Console.WriteLine("\nFound customer for same shop: " + info.Shop + " Cus: " + info.Payers.Count);
			//            }
			//        }
			//    }

			//    if (!shopExist)
			//    {
			//        var payer = new List<Payer>
			//        {
			//            new Payer()
			//                {Id = dataReader[0].ToString(), Name = dataReader[1].ToString(), NaklNum = dataReader[4].ToString(), Date = dataReader[5].ToString()}
			//        };

			//        shopInfo.Add(new ShopInfo() { Shop = shopId.ToString(), ShopName = dataReader[3].ToString(), Payers = payer });
			//    }

			//}
			//dataReader.Close();

			//TextWriter textWriter = new StreamWriter(@"C:\report.html", false, Encoding.UTF8);

			//textWriter.WriteLine("<table border='1'>");
			//foreach (var info in shopInfo)
			//{
			//    foreach (var payer in info.Payers)
			//    {
			//        string date = DateTime.Parse(payer.Date).ToString("dd.MM.yyyy");
			//        if (info.Payers.Count > 1)
			//            textWriter.WriteLine("<tr><td> " + info.ShopName + "</td><td>" + payer.Name + "</td><td>" + payer.NaklNum + "</td><td>" + date + "</td></tr>");
			//    }
			//}
			//textWriter.WriteLine("</table>");
			//textWriter.Close();
			//
		//}

	}
}
