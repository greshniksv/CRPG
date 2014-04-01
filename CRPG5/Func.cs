using System;
using System.Net;
using System.Net.Mail;
using System.Text;
using CRPG5.Transfers;
using fnLog2;

namespace CRPG5
{
	public static class Func
	{
		private static bool _mainLogInited=false;
		private static fnLog2.Log _mainLog = new Log();
		public static string HtmlReportHead = string.Empty;
		private static StringBuilder _logList = new StringBuilder();
		private static StringBuilder _htmlLogList = new StringBuilder();



		// Global variable
		public static string WorkLocation { get; set; }
		public static string WorkDir { get; set; }

		// Rajah
		public static string RCharset { get; set; }
		public static string RUserID { get; set; }
		public static string RPassword { get; set; }
		public static string RDatabase { get; set; }
		public static string RDataSource { get; set; }
		public static bool RTransfer { get; set; }

		// Buhoper
		public static string BCharset { get; set; }
		public static string BUserID { get; set; }
		public static string BPassword { get; set; }
		public static string BDatabase { get; set; }
		public static string BDataSource { get; set; }
		public static bool BTransfer { get; set; }

		// Tara
		public static string TCharset { get; set; }
		public static string TUserID { get; set; }
		public static string TPassword { get; set; }
		public static string TDatabase { get; set; }
		public static string TDataSource { get; set; }
		public static bool TTransfer { get; set; }
		//MsSql
		public static string MUserID { get; set; }
		public static string MPassword { get; set; }
		public static string MDatabase { get; set; }
		public static string MDataSource { get; set; }
		public static bool MTransfer { get; set; }
		//Postgre
		public static string PUserID { get; set; }
		public static string PPassword { get; set; }
		public static string PDatabase { get; set; }
		public static string PDataSource { get; set; }
		public static string PDataSourcePort { get; set; }
		public static bool PTransfer { get; set; }

		//Mail
		public static string MailFrom { get; set; }
		public static string MailHost { get; set; }
		public static int MailPort { get; set; }
		public static string MailPassword { get; set; }
		public static string MailUser { get; set; }
		public static bool MailSsl { get; set; }
		public static string MailTo { get; set; }
		public static string MailToAdmin { get; set; }


		public enum LogType
		{
			Information, Error, Warning
		}

		public static void Log(string text, LogType type)
		{
			// Write to console
			if (type==LogType.Information)
				Console.ForegroundColor = ConsoleColor.Green;

			if (type == LogType.Warning)
				Console.ForegroundColor = ConsoleColor.DarkYellow;

			if (type == LogType.Error)
				Console.ForegroundColor = ConsoleColor.Red;

			Console.Write(" >> ");
			Console.ResetColor();
			Console.Write(text+"\n");

			// write to file

			if (!_mainLogInited)
				_mainLog.Initialization(WorkDir+"/crpg5.log",99999,false);

			_mainLog.Write(text, type == LogType.Information ? MessageType.Information : 
				(type == LogType.Warning ? MessageType.Warning : MessageType.Error));

			// ----------------  write to SysLog (UDP) ---------------------------
			//CultureInfo us = new CultureInfo("en-US");
			//var udp = new UdpClient("192.168.2.8", 514);
			//string ss = "<150>2003-09-03 21:00:39 demo[1604]: syslog client at 10.0.0.6 started.";

			//string data = "<160>MyHost1 CRPG: " + DateTime.Now.ToString("MMM dd HH:mm:ss",us) + " MyHost  " + text;
			//var rawMsg = Encoding.UTF8.GetBytes(data);
			//udp.Send(rawMsg, rawMsg.Length);
			//udp.Close();
			//udp = null;

		}

		public static void Log(string text, LogType type, bool newLine)
		{
			// Write to console
			if (type == LogType.Information)
				Console.ForegroundColor = ConsoleColor.Green;

			if (type == LogType.Warning)
				Console.ForegroundColor = ConsoleColor.DarkYellow;

			if (type == LogType.Error)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				HtmlReportHead = "CRPG5 - ERROR finished !!!!!! ERROR !!!!!! ERROR !!!!!! ERROR";
			}

			Console.Write(" >> ");
			Console.ResetColor();
			Console.Write(text + (newLine?"\n":""));

			_logList.Append("<tr><td>"+text+"</td></tr>");
			//HtmlReportAdd(text);

			// write to file)

			if (!_mainLogInited)
				_mainLog.Initialization(WorkDir + "/crpg5.log", 99999, false,Encoding.UTF8);

			_mainLog.Write(text, type == LogType.Information ? MessageType.Information :
				(type == LogType.Warning ? MessageType.Warning : MessageType.Error));

			// ----------------  write to SysLog (UDP) ---------------------------
			//CultureInfo us = new CultureInfo("en-US");
			//var udp = new UdpClient("192.168.2.8", 514);
			//string ss = "<150>2003-09-03 21:00:39 demo[1604]: syslog client at 10.0.0.6 started.";

			//string data = "<160>MyHost1 CRPG: " + DateTime.Now.ToString("MMM dd HH:mm:ss",us) + " MyHost  " + text;
			//var rawMsg = Encoding.UTF8.GetBytes(data);
			//udp.Send(rawMsg, rawMsg.Length);
			//udp.Close();
			//udp = null;

		}

		public static string CreateHtmlReport()
		{
			var htmlReport = new StringBuilder();

			if (HtmlReportHead.Length<3)
					HtmlReportHead = "CRPG5 - Successfuly finished";

			htmlReport.Append("<table border='1' cellpadding='6' cellspacing='1' ><tr><td valign='top'>");
			htmlReport.Append(" <table> <tr><th bgcolor='#CCCCCC' colspan='3'> Transfer table info </th></tr> ");
			htmlReport.Append(" <tr bgcolor='#CCCCCC'><th> Table </th><th> Time </th><th> Rows </th></tr> ");
			htmlReport.Append(_htmlLogList); htmlReport.Append("</table>");
			htmlReport.Append("</td><td valign='top'>");
			htmlReport.Append(" <table> <tr><th bgcolor='#CCCCCC'> Transfer log </th></tr> ");
			htmlReport.Append(_logList); htmlReport.Append("</table>");
			htmlReport.Append("</tr></td></table>");
			return htmlReport.ToString();
		}

		public static void HtmlReportAdd(Postgre.TransferedInfo info) 
		{
			if (info.Table.IndexOf('-') != -1)
			{
				_htmlLogList.Append("<tr bgcolor='#CCCCCC'><td> " + info.Table + " </td><td> " + info.Time + " </td><td> " + info.RowCount + " </td></tr>");
			}
			else
			{
				_htmlLogList.Append("<tr><td> " + info.Table + " </td><td> " + info.Time + " </td><td> " + info.RowCount + " </td></tr>");
			}
		}

		public static void SendMail(string mailTo, string replyTo, string mailFrom, string attach, string head, string body)
		{
			Log("Send Mailmessage to " + mailTo + " from " + mailFrom + " replayTo " + replyTo,LogType.Information);

			string[] strArray = mailTo.Split(';');
			foreach (string str in strArray)
			{
				if (str.Length >= 3)
				{
					string smtpHost = MailHost;
					int smtpPort = MailPort;
					string smtpUser = MailUser;
					string smtpPassword = MailPassword;
					SmtpClient client = new SmtpClient(smtpHost, smtpPort);
					if (MailSsl)
					{
						client.EnableSsl = true;
					}
					client.Credentials = new NetworkCredential(smtpUser, smtpPassword);
					string from = mailFrom;
					string to = str;
					string subject = head;
					string str8 = body;
					MailMessage message2 = new MailMessage(from, to, subject, str8)
					{
						IsBodyHtml = true,
						ReplyTo = new MailAddress(replyTo)
					};
					MailMessage message = message2;
					if (attach != null)
					{
						Attachment item = new Attachment(attach);
						message.Attachments.Add(item);
					}
					try
					{
						client.Send(message);
					}
					catch (SmtpException exception)
					{
						Log("Error send mail: Detail:" + exception,LogType.Error);
					}
				}
				else
				{
					if (str.Trim().Length > 1)
					{
						Log("Error send message to \"" + str + "\". Few sumbol.", LogType.Error);
					}
				}
			}
		}

	}
}
