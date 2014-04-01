using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using Npgsql;

namespace CRPG5.Transfers
{
	public static class Rajah
	{
		public static bool Transfer(FbCommand fbCmd, NpgsqlConnection pgConn, MpRajah mpRajah)
		{
			var multipackList = GetMultipack(fbCmd, mpRajah);

			Func.Log(" * Start transfer RAJAH", Func.LogType.Information);

			var info = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT le.ID, le.name0%i, le.MANAGER0, le.GROUPDISCOUNT0, le.OKPO0 \n" +
				"FROM LEGAL0 le \n WHERE (le.ID <> '00000000') AND (le.DELETED = 0)", pgConn,
				"tblCustomers",
				"COPY \"tblCustomers\" (\"id\",\"cName\",\"cDiscountGroupId\",\"cOKPO\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}\n", dataList[0], dataList[1], dataList[2], dataList[3]);
				});
			if (info == null) return false;
			Func.HtmlReportAdd(info);
			info.Table = " - RAJAH";

			var infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT w.ID, w.NAME0%i, w.WEIGHT0, w.GROUPREF0, w.KINDREF0, w.TYPEREF0, w.VOLUME0, w.GOODCHARSVALUES10, \n" +
						" w.GOODCHARSVALUES20, w.GOODCHARSVALUES30, w.GOODCHARSVALUES40, w.TARA0, w.NUMBER0, w.SYNONYM0, w.PREFIX0, w.SERVICE0, \n" +
						" w.LENGTH0, w.WIDTH0, w.PRINTNAME0 \n" +
						" FROM WAREHOUS0 w \n" +
						" WHERE (w.ID <> '00000000') AND (w.DELETED = 0)", pgConn,
				"tblProducts",
				"COPY \"tblProducts\" (\"id\", \"pName\", \"pMass\", \"pGroupId1\", \"pGroupId2\", \"pGroupId3\", \"pCapacity\", \"pCapacity2\", \"pPackType\", \"pSrokHran\", \"pNotUse\", \"pTara\", \"pKodObolon\", \"pKodProizvod\", \"pPrefix\", \"pService\", \"pKodSort\", \"pKod\", \"pPrintName\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					var dl7 = dataList[7].Length < 2 ? "0" : dataList[7];
					dl7 = dl7.Substring(0, dl7.IndexOf('/') == -1 ? dl7.Length : dl7.IndexOf('/'));

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}	{7}	{8}	{9}	{10}	{11}	{12}	{13}	{14}	{15}	{16}	{17}	{18}\n",
						dataList[0], dataList[1], dataList[2].Replace(',', '.'), dataList[3], dataList[4],
						dataList[5], dataList[6].Replace(',', '.'), dl7,
						dataList[8].Length < 2 ? "0" : dataList[8],
						dataList[9].Length < 2 ? "0" : dataList[9],
						dataList[10].Length < 2 ? "0" : dataList[10],
						dataList[11],
						dataList[12].Length < 2 ? "0" : dataList[12],
						dataList[13].Length < 2 ? "0" : dataList[13],
						dataList[14], dataList[15], dataList[16],
						dataList[17].Replace(',', '.'),
						dataList[18].Length < 2 ? "0" : dataList[18]);
				});
			if (infoAdd == null) return false;
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT sk.GOODS0, sk.SKLAD0, date_to_str( sk.DATE_ ), sk.QUANREAL0%p, sk.QUANREAL0%m, sk.DOC " +
						" FROM SKLADREG0%m sk ", pgConn,
				"tblSkladReg",
				"COPY \"tblSkladReg\" (\"srGoods\", \"srSklad\", \"srDate\", \"srQuanRealP\", \"srQuanRealM\",\"srDOC\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					int quanP = Int32.Parse(dataList[3].Replace(',', '.'));
					int quanM = Int32.Parse(dataList[4].Replace(',', '.'));

					foreach (var multipackItem in multipackList)
					{
						if (dataList[0] == multipackItem.ProductId)
						{
							quanP *= multipackItem.MultipackCount;
							quanM *= multipackItem.MultipackCount;
						}
					}

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}\n",
						dataList[0], dataList[1],
						DateTime.Parse(dataList[2], new CultureInfo("ru-RU", false)).ToString("yyyy-MM-dd"),
						quanP.ToString().Replace(',', '.'),
						quanM.ToString().Replace(',', '.'), dataList[5]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT pd.ID, pl.SKLAD0, pl.PRICE10, pl.PRICE20, pl.PRICE30, pl.PRICE40, pl.PRICE50,\n" +
						"	(\n" +
						"		SELECT SUM( od.PARTYQUANREAL0 ) \n" +
						"		FROM ORDDETAIL0 od \n" +
						"		WHERE (od.ID <> '00000000') \n" +
						"			AND (od.DELETED <> 1) \n" +
						"			AND (od.SKLAD0 = pl.SKLAD0) \n" +
						"			AND (od.GOODS0 = pd.ID) \n" +
						"	) AS AvailCount\n" +
						"FROM WAREHOUS0 pd, PRICELIST0 pl \n" +
						"WHERE (pl.GOODS0 = pd.ID) \n" +
						"      AND (pl.PRICE10 > 0) AND (pd.DELETED = 0)", pgConn,
				"tblReserves",
				"COPY \"tblReserves\" (\"rProductId\",\"rStoreId\",\"rProductPrice1\",\"rProductPrice2\",\"rProductPrice3\",\"rProductPrice4\",\"rProductPrice5\",\"rProductAvailCount\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					int availCount = 0;

					availCount = dataList[7].Length>0 ? Int32.Parse(dataList[7]) : 0;

					foreach (var multipackItem in multipackList)
					{
						if (dataList[0] == multipackItem.ProductId)
						{
							availCount *= multipackItem.MultipackCount;
						}
					}

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}	{7}\n",
						dataList[0], dataList[1], dataList[2].Replace(',', '.'),
						dataList[3].Replace(',', '.'), dataList[4].Replace(',', '.'),
						dataList[5].Replace(',', '.'), dataList[6].Replace(',', '.'),
						availCount.ToString().Replace(',', '.'));
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT sg.ID, sg.NAME0%i\n" +
						"FROM SIMPLEWARE__GROUP0 sg \n" +
						"WHERE (sg.ID <> '00000000') AND (sg.DELETED = 0)", pgConn,
				"tblProductGroup",
				"COPY \"tblProductGroup\" (\"id\",\"pgName\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT dg.ID, dg.NAME0%i\n" +
						"FROM GROUPDISCOUNT0 dg, GROUPDISCOUNTDET0 dgd\n" +
						"WHERE (dg.ID <> '00000000') AND\n" +
						"      (dg.DELETED = 0) AND\n" +
						"      (dgd.ID <> '00000000') AND\n" +
						"      (dgd.DELETED = 0) AND\n" +
						"      NOT ((dgd.DISCOUNT0 = 0) AND (dgd.PRICENUM0 = 1)) AND\n" +
						"      (dg.ID = dgd.INVOCE0)", pgConn,
				"tblDiscountGroups",
				"COPY \"tblDiscountGroups\" (\"id\",\"dgName\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT ID, gd.GROUPREF0, gd.INVOCE0, gd.DISCOUNT0, gd.PRICENUM0\n" +
						" FROM GROUPDISCOUNTDET0 gd\n" +
						" WHERE (ID <> '00000000') AND (DELETED = 0) AND " +
						" NOT ((gd.DISCOUNT0 = 0) AND (gd.PRICENUM0 = 1))", pgConn,
				"tblDiscountGroupDetails",
				"COPY \"tblDiscountGroupDetails\" (\"id\",\"dgdDiscountGroupId\",\"dgdProductGroupId\",\"dgdDiscount\",\"dgdPriceNum\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}	{4}\n",
						dataList[0], dataList[1], dataList[2], dataList[3].Replace(',', '.'), dataList[4].Replace(',', '.'));
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;


			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "select nk.id, date_to_str(nk.DATE0), nk.NUMBER0, nk.OSNOV0, nk.NOTE0, nk.MANAGER0 " +
									" from NAKLADNA0 nk " +
									" where nk.deleted=0 and nk.NAME0%i = 'РАСХОДНАЯ НАКЛАДНАЯ' ", pgConn,
				"tblOrders",
				"COPY \"tblOrders\" (\"id\",\"oDate\",\"oNumber\",\"oShopId\",\"oComment\",\"oCreatorId\",\"oIsOfficial\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					string shopid = "0";

					if (dataList[3].IndexOf('[') != -1)
						shopid = dataList[3].Substring(dataList[3].IndexOf('[') + 1,
															  dataList[3].IndexOf(']') - (dataList[3].IndexOf('[') + 1));

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}\n",
						dataList[0],
						DateTime.Parse(dataList[1], new CultureInfo("ru-RU", false)).ToString("yyyy-MM-dd"),
						dataList[2], shopid,
						dataList[4].Replace('\\', '_'), dataList[5].Replace(',', '.'),
						dataList[2].IndexOf('/') != -1 ? "1" : "0");

					data = data.Replace("\n", "");
					data = data.Replace("\r", "");
					data += '\r';

				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "select nk.id, date_to_str(nk.DATE0), nk.NUMBER0, nk.OSNOV0, nk.NOTE0, nk.MANAGER0 " +
									" from NAKLADNA0 nk " +
									" where nk.deleted=0 and nk.NAME0%i = 'ВОЗВРАТНАЯ НАКЛАДНАЯ' ", pgConn,
				"tblOrdersBack",
				"COPY \"tblOrdersBack\" (\"id\",\"oDate\",\"oNumber\",\"oShopId\",\"oComment\",\"oCreatorId\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{

					string shopid = "0";

					if (dataList[3].IndexOf('[') != -1)
						shopid = dataList[3].Substring(dataList[3].IndexOf('[') + 1,
															  dataList[3].IndexOf(']') - (dataList[3].IndexOf('[') + 1));

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}\n",
						dataList[0],
						DateTime.Parse(dataList[1], new CultureInfo("ru-RU", false)).ToString("yyyy-MM-dd"),
						dataList[2], shopid,
						dataList[4].Replace('\\', '_'), dataList[5].Replace(',', '.'));

					data = data.Replace("\n", "");
					data = data.Replace("\r", "");
					data += '\r';

				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "select nk.ID,nk.INVOCE0,nk.GOODS0, max(nk.PRICE0), sum(nk.QUAN0), nk.SKLAD0, nk.DISCOUNT0 " +
								  " from NAKLDETAIL0 nk " +
								  " where nk.deleted=0 " +
								  " group by nk.ID,nk.INVOCE0,nk.GOODS0, nk.SKLAD0, nk.DISCOUNT0 ", pgConn,
				"tblOrderDetails",
				"COPY \"tblOrderDetails\" (\"rjID\", \"odOrderId\",\"odProductId\",\"odProductPrice\",\"odProductCount\",\"odStoreId\",\"odDiscount\",\"odRest\",\"odIsPersonalPrice\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					string productId = dataList[2];
					double price = double.Parse(dataList[3].IndexOf('E') != -1 ? "0" : dataList[3]);
					int quan = Int32.Parse(dataList[4]);

					foreach (var multipackItem in multipackList)
					{
						if(multipackItem.ProductId == productId)
						{
							price /= multipackItem.MultipackCount;
							quan *= multipackItem.MultipackCount;
						}
					}

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}	{7}	{8}\n",
						dataList[0], dataList[1], dataList[2],
						price.ToString().Replace(',', '.'),
						quan, dataList[5], dataList[6], "0", "0");
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "select vo.id, date_to_str( vo.DATE0 ) " +
									" from VOZVRATORDER0 vo " +
									" where vo.deleted=0 and vo.id <> '00000000' ", pgConn,
				"tblVozvratOrders",
				"COPY \"tblVozvratOrders\" (\"rjID\",\"voDate\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n",
					dataList[0],
					DateTime.Parse(dataList[1], new CultureInfo("ru-RU", false)).ToString("yyyy-MM-dd"));
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "select vd.PARTY0, vd.GOODS0, max(vd.PRICE0), sum(vd.QUAN0), " +
								  " vd.SKLAD0, vd.DISCOUNT0 ,vd.INVOCE0 " +
								  " from VOZVRATORDDETAIL0 vd where vd.deleted=0 " +
								  " group by vd.PARTY0, vd.GOODS0, vd.SKLAD0, vd.DISCOUNT0 ,vd.INVOCE0 ", pgConn,
				"tblVozvratOrderDetails",
				"COPY \"tblVozvratOrderDetails\" (\"vodOrderId\",\"vodProductId\",\"vodProductPrice\",\"vodProductCount\",\"vodStoreId\",\"vodDiscount\", \"vodVozvratOrderID\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					//if (decimal.Parse(dataList[2]) > 0)
					//{
					//    dataList[2] = decimal.Round(decimal.Parse(dataList[2]), 2, MidpointRounding.AwayFromZero).ToString();
					//}

					string productId = dataList[1];
					double price = double.Parse(dataList[2].IndexOf('E') != -1 ? "0" : dataList[2]);
					int quan = Int32.Parse(dataList[3]);

					foreach (var multipackItem in multipackList)
					{
						if (multipackItem.ProductId == productId)
						{
							price /= multipackItem.MultipackCount;
							quan *= multipackItem.MultipackCount;
						}
					}

					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}	{6}\n",
						dataList[0], dataList[1],
						price.ToString().Replace(',', '.'),
						quan, dataList[4], dataList[5], dataList[6]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT sk.ID, sk.name0%i \n" +
						"FROM SKLAD0 sk \n" +
						"WHERE (sk.ID <> '00000000') AND (sk.DELETED = 0)", pgConn,
				"tblStores",
				"COPY \"tblStores\" (\"id\",\"sName\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT ID, wr.NAME0, wr.PHONE0, wr.POSTSTR0, wr.INDEX0, wr.PAYERGR0 " +
						"FROM WORKERS0 wr \n" +
						"WHERE (wr.ID <> '00000000') AND (wr.DELETED = 0) ", pgConn,
				"tblUsers",
				"COPY \"tblUsers\" (\"id\",\"uFullName\",\"uPhone\", \"uType\", \"uIndex\", \"uPayerGr\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}	{4}	{5}\n", dataList[0], dataList[1], dataList[2], dataList[3], dataList[4], dataList[5]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT pg.ID, pg.name0%i " +
						"FROM PAYERGR0 pg \n" +
						"WHERE (pg.ID <> '00000000') AND (pg.DELETED = 0)", pgConn,
				"tblPayerGr",
				"COPY \"tblPayerGr\" (\"id\",\"Name\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}\n", dataList[0], dataList[1]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "SELECT cn.ID, cn.PAYER0 , date_to_str(cn.DATE0), cn.NUMBER0, date_to_str(cn.EDATE0) " +
						"FROM CONTRACTS0 cn \n" +
						"WHERE (cn.ID <> '00000000') AND (cn.DELETED = 0)", pgConn,
				"tblContracts",
				"COPY \"tblContracts\" (\"id\",\"Payer\",\"Date\",\"Number\",\"EDate\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}	{4}\n",
						dataList[0], dataList[1],
						DateTime.Parse(dataList[2], new CultureInfo("ru-RU", false)).ToString("yyyy-MM-dd")
						, dataList[3],
						DateTime.Parse(dataList[4], new CultureInfo("ru-RU", false)).ToString("yyyy-MM-dd"));
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "select ac.id, date_to_str(ac.date0), ac.number0%i, ac.payer0, ac.operationname0 \n" +
					 "  from account0 ac \n" +
					 " where ac.deleted=0 and ac.skl_prov0='Да'", pgConn,
				"Account",
				"COPY \"Account\" (\"id\",\"Date\",\"Number\",\"Payer\",\"OperationName\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					data = string.Format("{0}	{1}	{2}	{3}	{4}\n", dataList[0],
						DateTime.Parse(dataList[1], new CultureInfo("ru-RU", false)).ToString("yyyy-MM-dd"),
						dataList[2], dataList[3], dataList[4]);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;

			infoAdd = Postgre.ToPostrgeDb(mpRajah, fbCmd, "select ad.id, ad.invoce0, ad.goods0, ad.sklad0, ad.quan0 \n" +
					 " from accdetail0 ad \n" +
					 " where ad.deleted=0", pgConn,
				"AccDetail",
				"COPY \"AccDetail\" (\"id\",\"Invoce\",\"Goods\",\"Sklad\",\"Quan\") FROM STDIN",
				(ref string data, List<string> dataList, int progres) =>
				{
					string productId = dataList[2];
					int quan = Int32.Parse(dataList[4]);

					foreach (var multipackItem in multipackList)
					{
						if (multipackItem.ProductId == productId)
						{
							quan *= multipackItem.MultipackCount;
						}
					}

					data = string.Format("{0}	{1}	{2}	{3}	{4}\n",
						dataList[0], dataList[1], dataList[2], dataList[3], quan);
				});
			info.RowCount += infoAdd.RowCount;
			info.Time += infoAdd.Time;
			Func.HtmlReportAdd(infoAdd);
			if (infoAdd == null) return false;


			Func.HtmlReportAdd(info);

			return true;
		}

		private class MultipackItem
		{
			public string ProductId { get; set; }
			public int MultipackCount { get; set; }
		}


		private static List<MultipackItem> GetMultipack(FbCommand fbCmd, MpRajah mpRajah)
		{
			var multipackList = new List<MultipackItem>();

#if !DEBUG
			try
			{
#endif
				// get row count
				fbCmd.CommandText =mpRajah.FixQuery(" select wrh.id from warehous0 wrh where wrh.name0%i like '%(4 % 3)%'");
				var re = fbCmd.ExecuteReader();
				while (re.Read())
				{
					multipackList.Add(new MultipackItem(){ProductId = re[0].ToString(),MultipackCount = 4});
				}
				re.Close();
#if !DEBUG
			}
			catch (Exception ex)
			{
				Func.Log("Error extract Multipack product. Exception:" + ex, Func.LogType.Error);
				return null;
			}
#endif

			return multipackList;
		}



	}
}
