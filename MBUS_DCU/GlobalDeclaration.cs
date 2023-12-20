using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace MBUS_DCU;

internal class GlobalDeclaration
{
	public class MMId
	{
		public ushort MeterId { get; set; }

		public ushort ModelId { get; set; }
	}

	public class ValDetails
	{
		public int ScheduleID { get; set; }

		public int MeterID { get; set; }

		public int ValveID { get; set; }

		public bool Status { get; set; }

		public TimeSpan SchTime { get; set; }

		public DateTime LastUpdated { get; set; }

		public int TransmitId { get; set; }
	}

	public class MDetails
	{
		public short TransmitId { get; set; }

		public string MBUSID { get; set; }

		public int MeterId { get; set; }

		public short MModelId { get; set; }

		public short MeterType { get; set; }

		public short TagsCount { get; set; }

		public byte DMeterId { get; set; }

		public string MeterName { get; set; }

		public int ValveId { get; set; }
	}

	public class TransmitDetails
	{
		public int PollTime { get; set; }

		public string TransmitMode { get; set; }

		public string GatewayType { get; set; }

		public int TransmitId { get; set; }

		public string TransmitName { get; set; }

		public int TypeId { get; set; }

		public int BaudRate { get; set; }

		public string Parity { get; set; }

		public ushort Stopbit { get; set; }

		public string PortNo { get; set; }

		public ushort ModelId { get; set; }

		public bool ScheduledPoll { get; set; }

		public bool GatewayTelegram { get; set; }

		public string DeviceId { get; set; }

		public int PollDate { get; set; }
	}

	public class ScheduleDetails
	{
		public int TransmitId { get; set; }

		public int DayInterval { get; set; }

		public TimeSpan TransSch { get; set; }
	}

	public class GatwayTelDetails
	{
		public ushort TransmitId { get; set; }

		public ushort TelNo { get; set; }

		public string Telegram { get; set; }
	}

	public class ImportDetails
	{
		public ushort TemplateId { get; set; }

		public TimeSpan SchTime { get; set; }
	}

	public class MSDetails
	{
		public int MeterId { get; set; }

		public int TransmitId { get; set; }

		public int DayInterval { get; set; }

		public TimeSpan ScheduleTime { get; set; }
	}

	public static SqlConnection _ASIMBUSSETTINGSCnx = null;

	public static SqlConnection _ASIMBUSDATA = null;

	public static string CustomerId = "";

	public static string _SQLServerName;

	public static string _SQLPwd;

	public static string _SQLDBName;

	public static string _SQLUName;

	public static string _SQLClientDBName;

	public static int _portIDforEdit = 0;

	public static byte _ManagePortEditFlag = 0;

	public static bool ErrorTest = false;

	public static byte _PollingID = 0;

	public static byte _MID = 0;

	public static byte _MFID = 0;

	public static byte _Version = 0;

	public static byte _Medium = 0;

	public static byte _ThirdParty = 0;

	public static byte _ThirdPartyGateway = 0;

	public static byte _PushDatatoCloud = 0;

	public static string _CloudIP = "";

	public static byte _NoofRetries = 0;

	public static int _TotlaSlaveCount = 0;

	public static bool ISRepeatPoll = false;

	public static bool InitialReset = false;

	public static bool IsSaveIntegrated = false;

	public static string _PollingTime = "";

	public static string _TransmitMode = "";

	public static List<TransmitDetails> TransDetails = new List<TransmitDetails>();

	public static string strLogPath = "";

	public static List<MDetails> MeterDetails = new List<MDetails>();

	public static List<ValDetails> ValveDetails = new List<ValDetails>();

	public static List<MMId> MeterModelID = new List<MMId>();

	public static List<ScheduleDetails> TransmitSchedule = new List<ScheduleDetails>();

	public static List<GatwayTelDetails> GatewayTelegram = new List<GatwayTelDetails>();

	public static List<ImportDetails> ImportSchedule = new List<ImportDetails>();

	public static List<MSDetails> MeterSchedule = new List<MSDetails>();

	public static int TotalMeterCount = 0;

	public static int TotalSerial = 0;

	public static int TotalTCP = 0;

	public static int ValveStatusTag = 1;

	public static int ModTransmitTotal = 0;

	public static string[,] Telegram = new string[150, 5];

	public static int[] IntegratedTag = new int[21];

	public static ArrayList CommonArraylist = new ArrayList();

	public static void GetCustomerId()
	{
		SqlDataReader sqlDataReader = null;
		try
		{
			string cmdText = "  Select [CustomerID] From [SETTINGS_CustomerMaster] Where [CustomerDB]='" + _SQLClientDBName + "'";
			SqlCommand sqlCommand = new SqlCommand(cmdText, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			if (sqlDataReader.Read())
			{
				CustomerId = sqlDataReader["CustomerID"].ToString();
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU GlobalDeclarations", "GetCustomerId", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
	}

	public static List<TransmitDetails> GetTransmitDetails()
	{
		List<TransmitDetails> list = new List<TransmitDetails>();
		SqlDataReader sqlDataReader = null;
		try
		{
			int num;
			int num2 = (num = 0);
			int num3 = 0;
			int typeId = 0;
			string text = "";
			text = "select [TransmitID],[TransmitMode],[TransmitName],[PortNo],[GatewayTypeId],[PollingTime],[BaudRate],[Parity],[StopBit],[ModelID],ISNULL([ScheduledPolling],0) ScheduledPolling,ISNULL([GatewayTelegram],0) GatewayTelegram,ISNULL([DEV_ID],0) DEV_ID,(Select [GatewayTypeName] FROM [dbo].[SETTINGS_GATEWAYTYPEMASTER] where [GatewayTypeId]=ST.[GatewayTypeId]) GatewayName from [SETTINGS_Transmit] ST Where [Active] = 1 AND ISNULL([CustomerID],1)=" + CustomerId + " Order by TransmitID";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				int pollTime = 0;
				switch (sqlDataReader["PollingTime"].ToString())
				{
				case "0":
					pollTime = 1;
					break;
				case "1":
					pollTime = 5;
					break;
				case "2":
					pollTime = 15;
					break;
				case "3":
					pollTime = 60;
					break;
				case "4":
					pollTime = 100;
					break;
				case "5":
					pollTime = 101;
					break;
				case "6":
					pollTime = 102;
					break;
				case "7":
					pollTime = 103;
					break;
				case "8":
					pollTime = 104;
					break;
				case "9":
					pollTime = 105;
					break;
				}
				string text2 = sqlDataReader["GatewayName"].ToString();
				int num4 = Convert.ToInt16(sqlDataReader["TransmitMode"].ToString());
				string transmitMode = "";
				if (text2 == "MBUS")
				{
					switch (num4)
					{
					case 1:
						typeId = num2;
						transmitMode = "Serial";
						num2++;
						break;
					case 2:
						typeId = num;
						transmitMode = "TCP";
						num++;
						break;
					}
				}
				else if (text2 == "MODBUS" || text2 == "BMS MODBUS")
				{
					switch (num4)
					{
					case 1:
						typeId = num3;
						transmitMode = "Serial";
						num3++;
						break;
					case 2:
						typeId = num3;
						transmitMode = "TCP";
						num3++;
						break;
					}
				}
				list.Add(new TransmitDetails
				{
					PollTime = pollTime,
					TransmitMode = transmitMode,
					GatewayType = text2,
					TransmitId = Convert.ToInt16(sqlDataReader["TransmitID"].ToString()),
					TransmitName = sqlDataReader["TransmitName"].ToString(),
					TypeId = typeId,
					BaudRate = ((!(sqlDataReader["BaudRate"].ToString() == "")) ? Convert.ToInt32(sqlDataReader["BaudRate"].ToString()) : 0),
					Parity = sqlDataReader["Parity"].ToString(),
					Stopbit = ((sqlDataReader["StopBit"].ToString() == "") ? Convert.ToUInt16(0) : Convert.ToUInt16(sqlDataReader["StopBit"].ToString())),
					PortNo = sqlDataReader["PortNo"].ToString(),
					ModelId = ((sqlDataReader["ModelID"].ToString() == "") ? Convert.ToUInt16(0) : Convert.ToUInt16(sqlDataReader["ModelID"].ToString())),
					ScheduledPoll = Convert.ToBoolean(sqlDataReader["ScheduledPolling"]),
					GatewayTelegram = Convert.ToBoolean(sqlDataReader["GatewayTelegram"]),
					DeviceId = sqlDataReader["DEV_ID"].ToString()
				});
			}
			ModTransmitTotal = num3;
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU GlobalDeclarations", "GetTransmitDetails", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
		return list;
	}

	public static List<ScheduleDetails> GetScheduleDetails()
	{
		List<ScheduleDetails> list = new List<ScheduleDetails>();
		SqlDataReader sqlDataReader = null;
		try
		{
			string cmdText = "Select [TransmitID],[DayInterval],[ScheduleTime] FROM [dbo].[SETTINGS_TransmitSchedule] where [TransmitID] in (Select [TransmitID] From [dbo].[SETTINGS_Transmit] where ISNULL([ScheduledPolling],0)=1 AND [Active]=1 AND [CustomerID]=" + CustomerId + ")";
			SqlCommand sqlCommand = new SqlCommand(cmdText, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				list.Add(new ScheduleDetails
				{
					TransmitId = Convert.ToInt16(sqlDataReader["TransmitID"].ToString()),
					DayInterval = Convert.ToInt16(sqlDataReader["DayInterval"].ToString()),
					TransSch = TimeSpan.Parse(sqlDataReader["ScheduleTime"].ToString())
				});
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU GlobalDeclarations", "GetScheduleDetails", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
		return list;
	}

	public static int GetTotalSlaveCount()
	{
		int result = 0;
		SqlDataReader sqlDataReader = null;
		try
		{
			string text = "";
			text = "Select Count(MeterID) from SETTINGS_MeterList";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			sqlDataReader.Read();
			result = ((sqlDataReader[0] == DBNull.Value) ? 1 : Convert.ToInt32(sqlDataReader[0]));
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU, GlobalDeclaration", "GetTotalSlaveCount", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
		return result;
	}

	public static short GetTotalPorts(short PortType)
	{
		short result = 0;
		SqlDataReader sqlDataReader = null;
		try
		{
			string text = "";
			text = ("select Count(TransmitID) Total from SETTINGS_Transmit Where TransmitMode = " + PortType) ?? "";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			sqlDataReader.Read();
			result = (short)((sqlDataReader[0] != DBNull.Value) ? Convert.ToInt16(sqlDataReader[0]) : 0);
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU, GlobalDeclaration", "GetTotalSlaveCount", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
		return result;
	}

	public static List<MDetails> GetMeterDetails()
	{
		List<MDetails> list = new List<MDetails>();
		try
		{
			DataTable dataTable = new DataTable();
			string text = "";
			text = "Select [MeterID],[MeterName],[DMeterID],[MeterTransmitID],[MeterModelID],[MeterType],(select [NoOfTags] From  [SETTINGS_Model] where [ModelID]= SM.MeterModelID) Tags,ISNULL([ValveID],0) ValveID From [SETTINGS_MeterList] SM where CustomerID=" + CustomerId + " AND MeterID<100000 order by MeterID";
			SqlCommand selectCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(selectCommand);
			sqlDataAdapter.Fill(dataTable);
			if (dataTable.Rows.Count > 0)
			{
				TotalMeterCount = dataTable.Rows.Count;
				string text2 = "";
				for (int i = 0; i < dataTable.Rows.Count; i++)
				{
					if (_PollingID == 0)
					{
						string text3 = dataTable.Rows[i]["MeterName"].ToString();
						string[] array = text3.Split('/');
						text2 = Reverse(array[0]);
					}
					else
					{
						text2 = dataTable.Rows[i]["DMeterID"].ToString();
					}
					string text4 = dataTable.Rows[i]["MeterType"].ToString();
					list.Add(new MDetails
					{
						TransmitId = Convert.ToInt16(dataTable.Rows[i]["MeterTransmitID"]),
						MBUSID = text2,
						MeterId = Convert.ToInt32(dataTable.Rows[i]["MeterID"]),
						MModelId = Convert.ToInt16(dataTable.Rows[i]["MeterModelID"].ToString()),
						MeterType = Convert.ToInt16(dataTable.Rows[i]["MeterType"].ToString()),
						TagsCount = Convert.ToInt16(dataTable.Rows[i]["Tags"]),
						DMeterId = Convert.ToByte(dataTable.Rows[i]["DMeterID"].ToString()),
						MeterName = dataTable.Rows[i]["MeterName"].ToString(),
						ValveId = Convert.ToInt32(dataTable.Rows[i]["ValveID"])
					});
				}
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU GlobalDeclaration", "GetMeterDetails", strLogPath);
		}
		return list;
	}

	public static void TelegramDetailsGet()
	{
		try
		{
			string text = "";
			text = "  Select [ModelID],[TelNo],[Telegram] FROM [SETTINGS_TELEGRAMMASTER] Where [GatewayTypeId]=1 Order by [ModelID],[TelNo]";
			SqlCommand selectCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(selectCommand);
			DataTable dataTable = new DataTable();
			sqlDataAdapter.Fill(dataTable);
			if (dataTable.Rows.Count > 0)
			{
				for (int i = 0; i < dataTable.Rows.Count; i++)
				{
					int num = Convert.ToInt16(dataTable.Rows[i]["ModelID"]);
					int num2 = Convert.ToInt16(dataTable.Rows[i]["TelNo"]);
					Telegram[num, num2] = dataTable.Rows[i]["Telegram"].ToString();
				}
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU, GlobalDeclaration", "TelegramDetailsGet", strLogPath);
		}
	}

	public static string Reverse(string s)
	{
		string text = "";
		string[] array = new string[s.Length / 2 + ((s.Length % 2 != 0) ? 1 : 0)];
		for (int i = 0; i < array.Length; i++)
		{
			array[i] = s.Substring(i * 2, (i * 2 + 2 > s.Length) ? 1 : 2);
		}
		for (int j = 0; j < array.Length; j++)
		{
			text += array[array.Length - (j + 1)];
		}
		return text;
	}

	public static void GetValveStatusTag()
	{
		SqlDataReader sqlDataReader = null;
		try
		{
			string text = "";
			text = ("Select [Value] From [dbo].[SoftwareSettings] where [SettingsName]='ValveStatusTag' And [CustomerID]=" + CustomerId) ?? "";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			if (sqlDataReader.Read())
			{
				ValveStatusTag = Convert.ToInt16(sqlDataReader["Value"].ToString());
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.ToString(), "DCU Global Declaration", "GetValveStatusTag", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
	}

	public static List<ValDetails> GetValveDetails()
	{
		List<ValDetails> list = new List<ValDetails>();
		SqlDataReader sqlDataReader = null;
		try
		{
			string text = "";
			text = " select [ScheduleID],[MeterID] as ValveID,[State],[ScheduleTime],[LastUpdated],(Select [MeterTransmitID] from [SETTINGS_MeterList] where CustomerID=" + CustomerId + " and MeterID=SV.MeterID) TransmitId ,( Select [MeterID] From [SETTINGS_MeterList] where [ValveID]=SV.MeterID) MeterID from [Schedule_Valve] SV Where [CustomerID] = " + CustomerId + " Order by ScheduleID";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				DateTime lastUpdated = ((!(sqlDataReader["LastUpdated"].ToString() == "")) ? Convert.ToDateTime(sqlDataReader["LastUpdated"]) : DateTime.Now.AddHours(-24.0));
				list.Add(new ValDetails
				{
					ScheduleID = Convert.ToInt16(sqlDataReader["ScheduleID"].ToString()),
					MeterID = Convert.ToInt16(sqlDataReader["MeterID"].ToString()),
					ValveID = Convert.ToInt16(sqlDataReader["ValveID"].ToString()),
					Status = Convert.ToBoolean(sqlDataReader["State"]),
					SchTime = TimeSpan.Parse(sqlDataReader["ScheduleTime"].ToString()),
					LastUpdated = lastUpdated,
					TransmitId = Convert.ToInt16(sqlDataReader["TransmitId"].ToString())
				});
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.ToString(), "MBUS DCU GlobalDeclarations", "GetValveDetails", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
		return list;
	}

	public static void ClearTransmitPoll()
	{
		try
		{
			if (_ASIMBUSSETTINGSCnx.State == ConnectionState.Closed)
			{
				_ASIMBUSSETTINGSCnx.Open();
			}
			string text = "";
			text = "Update [dbo].[TransmitHistory] set Status=0";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			int num = sqlCommand.ExecuteNonQuery();
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU, GlobalDeclaration", "ClearTransmitPoll", strLogPath);
		}
	}

	public static void UpdateHistory(int Id)
	{
		try
		{
			int transmitId = TransDetails[Id].TransmitId;
			string text = "";
			if (_ASIMBUSSETTINGSCnx.State == ConnectionState.Closed)
			{
				_ASIMBUSSETTINGSCnx.Open();
			}
			text = "Update [TransmitHistory] set status=0 where TransmitID=" + transmitId + " and Logtime=(select Max(logtime) FROM [TransmitHistory] where TransmitID=" + transmitId + ")";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			int num = sqlCommand.ExecuteNonQuery();
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU, GlobalDeclaration", "UpdateHistory", strLogPath);
		}
	}

	public static void StringLogger(string _string, string _ModuleName, string _ProcedureName, string _Path)
	{
		string text = DateTime.Now.ToString("dd-MM-yyyy") + ".txt";
		try
		{
			if (!Directory.Exists(_Path))
			{
				Directory.CreateDirectory(_Path);
			}
			if (File.Exists(_Path + "\\" + text))
			{
				FileStream fileStream = new FileStream(_Path + "\\" + text, FileMode.Append, FileAccess.Write);
				fileStream.Close();
				File.AppendAllText(_Path + "\\" + text, Convert.ToString(Convert.ToString(Convert.ToString(DateTime.Now.ToString() + ": ") + _ModuleName + ": ") + _ProcedureName + ": ") + _string + Environment.NewLine);
			}
			else
			{
				FileStream fileStream2 = new FileStream(_Path + "\\" + text, FileMode.Create, FileAccess.Write);
				fileStream2.Close();
				StreamWriter streamWriter = new StreamWriter(_Path + "\\" + text);
				streamWriter.Write(Convert.ToString(Convert.ToString(Convert.ToString(DateTime.Now.ToString() + ": ") + _ModuleName + ": ") + _ProcedureName + ": ") + _string + Environment.NewLine);
				streamWriter.Close();
			}
		}
		catch (Exception ex)
		{
			if (File.Exists(_Path + "\\" + Convert.ToString("ErrLogger.txt")))
			{
				FileStream fileStream3 = new FileStream(_Path + "\\" + Convert.ToString("ErrLogger.txt"), FileMode.Append, FileAccess.Write);
				fileStream3.Close();
				File.AppendAllText(_Path + "\\" + Convert.ToString("ErrLogger.txt"), Convert.ToString(Convert.ToString(DateTime.Now.ToString() + ": ") + _ModuleName + ": ") + _ProcedureName + ": " + ex.Message + Environment.NewLine);
			}
			else
			{
				FileStream fileStream4 = new FileStream(_Path + "\\" + Convert.ToString("ErrLogger.txt"), FileMode.Create, FileAccess.Write);
				fileStream4.Close();
				StreamWriter streamWriter2 = new StreamWriter(_Path + "\\" + Convert.ToString("ErrLogger.txt"));
				streamWriter2.Write(Convert.ToString(Convert.ToString(DateTime.Now.ToString() + ": ") + _ModuleName + ": ") + _ProcedureName + ": " + ex.Message + Environment.NewLine);
				streamWriter2.Close();
			}
		}
	}

	public static List<MMId> GetMeterModelID()
	{
		List<MMId> list = new List<MMId>();
		SqlDataReader sqlDataReader = null;
		try
		{
			string text = "";
			text = "  Select Distinct([MeterID]),[ModelID] From [SETTINGS_AssignParameters] where [MeterID] is not null";
			SqlCommand sqlCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				list.Add(new MMId
				{
					MeterId = Convert.ToUInt16(sqlDataReader["MeterID"].ToString()),
					ModelId = Convert.ToUInt16(sqlDataReader["ModelID"].ToString())
				});
			}
			sqlDataReader.Close();
		}
		catch (Exception ex)
		{
			sqlDataReader?.Close();
			StringLogger(ex.Message.ToString(), "MBUS DCU, GlobalDeclaration", "GetMeterModelID", strLogPath);
		}
		return list;
	}

	public static List<GatwayTelDetails> GetGateWayTelegram()
	{
		List<GatwayTelDetails> list = new List<GatwayTelDetails>();
		SqlDataReader sqlDataReader = null;
		try
		{
			string cmdText = "Select  GT.[TransmitID],GT.[TelegramNo],GT.[Telegram] From [SETTINGS_TELEGRAMGATEWAY] GT Inner Join [SETTINGS_Transmit] ST on ST.TransmitID=GT.TransmitID where ISNULL(ST.GatewayTelegram,0)=1 order by [TelegramNo]";
			SqlCommand sqlCommand = new SqlCommand(cmdText, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				list.Add(new GatwayTelDetails
				{
					TransmitId = Convert.ToUInt16(sqlDataReader["TransmitID"].ToString()),
					TelNo = Convert.ToUInt16(sqlDataReader["TelegramNo"].ToString()),
					Telegram = sqlDataReader["Telegram"].ToString()
				});
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU, GlobalDeclaration", "GetGateWayTelegram", strLogPath);
		}
		finally
		{
			sqlDataReader.Close();
		}
		return list;
	}

	public static void IntegratedDetailsGet()
	{
		try
		{
			string text = "";
			SqlDataAdapter sqlDataAdapter = new SqlDataAdapter();
			DataTable dataTable = new DataTable();
			text = "Select [ParameterNameID],ISNULL([IntegratedID],0) IntegratedID From [SETTINGS_ParameterList] Where IntegratedID>0 AND CustomerID=" + CustomerId + " order by IntegratedID";
			SqlCommand selectCommand = new SqlCommand(text, _ASIMBUSSETTINGSCnx);
			sqlDataAdapter.SelectCommand = selectCommand;
			sqlDataAdapter.Fill(dataTable);
			int num = 0;
			for (int i = 0; i < dataTable.Rows.Count; i++)
			{
				num = Convert.ToInt16(dataTable.Rows[i]["IntegratedID"].ToString());
				IntegratedTag[num] = Convert.ToInt16(dataTable.Rows[i]["ParameterNameID"].ToString());
			}
			dataTable.Clear();
		}
		catch (Exception ex)
		{
			StringLogger(ex.ToString(), "MBUS DCU, GlobalDeclaration", "IntegratedDetailsGet", strLogPath);
		}
	}

	public static List<ImportDetails> GetImportSchedule()
	{
		List<ImportDetails> list = new List<ImportDetails>();
		SqlDataReader sqlDataReader = null;
		try
		{
			string cmdText = "  Select [TemplateID],[ScheduleTime] From [SETTINGS_InterfaceSchedule]";
			SqlCommand sqlCommand = new SqlCommand(cmdText, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				list.Add(new ImportDetails
				{
					TemplateId = Convert.ToUInt16(sqlDataReader["TemplateID"].ToString()),
					SchTime = TimeSpan.Parse(sqlDataReader["ScheduleTime"].ToString())
				});
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.Message.ToString(), "MBUS DCU GlobalDeclarations", "GetImportSchedule", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
		return list;
	}

	public static List<MSDetails> GetMeterScheduleDetails()
	{
		List<MSDetails> list = new List<MSDetails>();
		SqlDataReader sqlDataReader = null;
		try
		{
			string cmdText = "Select [MeterID],[TransmitID],[DayInterval],[ScheduleTime] From [SETTINGS_MeterSchedule] Where [TransmitID] in(Select [TransmitID] From [dbo].[SETTINGS_Transmit] where [Active]=1 AND [CustomerID]=" + CustomerId + ")";
			SqlCommand sqlCommand = new SqlCommand(cmdText, _ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				list.Add(new MSDetails
				{
					MeterId = Convert.ToInt32(sqlDataReader["MeterID"].ToString()),
					ScheduleTime = TimeSpan.Parse(sqlDataReader["ScheduleTime"].ToString()),
					TransmitId = Convert.ToInt32(sqlDataReader["TransmitID"].ToString()),
					DayInterval = Convert.ToInt16(sqlDataReader["DayInterval"].ToString())
				});
			}
		}
		catch (Exception ex)
		{
			StringLogger(ex.ToString(), "MBUS DCU GlobalDeclarations", "GetMeterScheduleDetails", strLogPath);
		}
		finally
		{
			sqlDataReader?.Close();
		}
		return list;
	}
}
