using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO.Ports;
using System.Net.Sockets;
using System.Threading.Tasks;
using Modbus.Device;

namespace MBUS_DCU;

public class Mod_Bus
{
	public SerialPort[] _SerialPort;

	public ModbusSerialMaster[] _masterRTU;

	public ModbusIpMaster[] _masterModbus;

	public TcpClient[] _tcpClient;

	public byte[] MeterRetry;

	public int[] MeterScanRate;

	public short[] MeterModelID;

	public byte[] DMeterID;

	public byte[] modbusCommbyte;

	public float[] VoltsMF;

	public float[] AmpsMF;

	public float[] PowerMF;

	public float[] EnergyMF;

	public short[] ParamMF = new short[300];

	public string[] ParameterName = new string[300];

	public static ArrayList arrParamName = new ArrayList();

	public static ArrayList arrParamID = new ArrayList();

	public float[,] RealTimeData = new float[1, 1];

	public float[,] MaxData = new float[2, 2];

	public float[,] MinData = new float[2, 2];

	public DateTime[,] MaxTime = new DateTime[2, 2];

	public DateTime[,] MinTime = new DateTime[2, 2];

	public int TotalModels;

	public int[] TotalModelBlock;

	public ushort[,] ModelFC = new ushort[2, 2];

	public ushort[,] ModelStartAdd = new ushort[2, 2];

	public ushort[,] ModelTotReg = new ushort[2, 2];

	public ushort[,] ModelStartParam = new ushort[2, 2];

	public ushort[,] ModelEndParam = new ushort[2, 2];

	public ushort[,,] ParameterIndex = new ushort[2, 2, 2];

	public float[,,] ParameterMF = new float[2, 2, 2];

	public float[,,] ParameterDataType = new float[2, 2, 2];

	public ushort[,,] ParamIndexMeterId = new ushort[2, 2, 2];

	public bool[] PollStatus;

	public void SetVariables()
	{
		SqlDataReader sqlDataReader = null;
		try
		{
			int num = 0;
			int num2 = 0;
			int num3 = 0;
			int num4 = 0;
			modbusCommbyte = new byte[GlobalDeclaration.ModTransmitTotal];
			_SerialPort = new SerialPort[GlobalDeclaration.ModTransmitTotal];
			_masterRTU = (ModbusSerialMaster[])(object)new ModbusSerialMaster[GlobalDeclaration.ModTransmitTotal];
			_masterModbus = (ModbusIpMaster[])(object)new ModbusIpMaster[GlobalDeclaration.ModTransmitTotal];
			_tcpClient = new TcpClient[GlobalDeclaration.ModTransmitTotal];
			string text = "";
			text = " Select Max(MeterID) Total from [dbo].[SETTINGS_MeterList]";
			SqlCommand sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			if (sqlDataReader.Read())
			{
				num = Convert.ToInt16(sqlDataReader["Total"].ToString());
			}
			sqlDataReader.Close();
			num++;
			VoltsMF = new float[num];
			AmpsMF = new float[num];
			PowerMF = new float[num];
			EnergyMF = new float[num];
			MeterModelID = new short[num];
			MeterScanRate = new int[num];
			MeterRetry = new byte[num];
			DMeterID = new byte[num];
			text = " Select Max([ParameterNameID]) TotalParam FROM [dbo].[SETTINGS_ParameterList]";
			sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			if (sqlDataReader.Read())
			{
				num2 = Convert.ToInt16(sqlDataReader["TotalParam"].ToString());
			}
			sqlDataReader.Close();
			num2++;
			ParamMF = new short[num2];
			ParameterName = new string[num2];
			RealTimeData = new float[num, 301];
			text = "Select Max(BlockID) MaxBlock From [dbo].[SETTINGS_Block]";
			sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			if (sqlDataReader.Read())
			{
				num3 = Convert.ToInt16(sqlDataReader["MaxBlock"].ToString());
			}
			sqlDataReader.Close();
			num3++;
			text = "Select max(ModelID) MaxModel From [dbo].[SETTINGS_Model]";
			sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			if (sqlDataReader.Read())
			{
				num4 = Convert.ToInt16(sqlDataReader["MaxModel"].ToString());
			}
			sqlDataReader.Close();
			num4++;
			ModelFC = new ushort[num4, num3];
			ModelStartAdd = new ushort[num4, num3];
			ModelTotReg = new ushort[num4, num3];
			ModelStartParam = new ushort[num4, num3];
			ModelEndParam = new ushort[num4, num3];
			ParameterIndex = new ushort[num4, num3, 100];
			ParameterMF = new float[num4, num3, 100];
			ParameterDataType = new float[num4, num3, 100];
			ParamIndexMeterId = new ushort[num4, num3, 100];
		}
		catch (Exception ex)
		{
			sqlDataReader.Close();
			GlobalDeclaration.StringLogger(ex.StackTrace.ToString() + ":/" + ex.ToString(), "MBUS DCU, Mod_Bus", "SetVariables", GlobalDeclaration.strLogPath);
		}
	}

	public void CreateTransmit()
	{
		SqlDataReader sqlDataReader = null;
		int num = 0;
		try
		{
			string text = "";
			text = "select * from SETTINGS_Transmit Where [Active]=1 AND [GatewayTypeId] In(2,3) Order by TransmitID";
			SqlCommand sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
			sqlDataReader = sqlCommand.ExecuteReader();
			while (sqlDataReader.Read())
			{
				int num2 = Convert.ToInt16(sqlDataReader["TransmitMode"].ToString());
				string text2 = sqlDataReader["TransmitName"].ToString();
				switch (num2)
				{
				case 1:
				{
					_SerialPort[num] = new SerialPort();
					_SerialPort[num].BaudRate = Convert.ToInt16(sqlDataReader["BaudRate"].ToString());
					string text3 = sqlDataReader["Parity"].ToString().Trim();
					if (text3 == "EVEN")
					{
						_SerialPort[num].Parity = Parity.Even;
					}
					else if (text3 == "NONE")
					{
						_SerialPort[num].Parity = Parity.None;
					}
					else
					{
						_SerialPort[num].Parity = Parity.Odd;
					}
					int num4 = Convert.ToInt16(sqlDataReader["StopBit"].ToString());
					if (num4 == 1)
					{
						_SerialPort[num].StopBits = StopBits.One;
					}
					else
					{
						_SerialPort[num].StopBits = StopBits.Two;
					}
					_SerialPort[num].DataBits = 8;
					_SerialPort[num].ReadTimeout = 1000;
					_SerialPort[num].PortName = text2;
					if (_SerialPort[num].IsOpen)
					{
						_SerialPort[num].Close();
					}
					_SerialPort[num].Open();
					if (_masterRTU[num] != null)
					{
						((ModbusDevice)_masterRTU[num]).Dispose();
					}
					_masterRTU[num] = ModbusSerialMaster.CreateRtu(_SerialPort[num]);
					break;
				}
				case 2:
				{
					short num3 = Convert.ToInt16(sqlDataReader["PortNo"].ToString());
					if (num3 != 502)
					{
						modbusCommbyte[num] = 0;
						ConnectTCPIP(text2, num3, num);
					}
					else
					{
						modbusCommbyte[num] = 1;
						ConnectTCPIP(text2, num3, num);
					}
					break;
				}
				}
				num++;
			}
		}
		catch (Exception ex)
		{
			GlobalDeclaration.StringLogger(ex.ToString(), "MBUS DCU, Mod_Bus", "CreateTransmits", GlobalDeclaration.strLogPath);
		}
		finally
		{
			PollStatus = new bool[num + 1];
			sqlDataReader.Close();
		}
	}

	public bool ConnectTCPIP(string tcpip, short portno, int Transmitno)
	{
		if (_masterRTU[Transmitno] != null)
		{
			((ModbusDevice)_masterRTU[Transmitno]).Dispose();
		}
		if (_masterModbus[Transmitno] != null)
		{
			((ModbusDevice)_masterModbus[Transmitno]).Dispose();
		}
		if (_tcpClient[Transmitno] != null)
		{
			_tcpClient[Transmitno].Close();
		}
		try
		{
			_tcpClient[Transmitno] = new TcpClient();
			IAsyncResult asyncResult = _tcpClient[Transmitno].BeginConnect(tcpip, portno, null, null);
			asyncResult.AsyncWaitHandle.WaitOne(300, exitContext: true);
			if (modbusCommbyte[Transmitno] == 0)
			{
				_masterRTU[Transmitno] = ModbusSerialMaster.CreateRtu(_tcpClient[Transmitno]);
			}
			else if (modbusCommbyte[Transmitno] == 1)
			{
				_masterModbus[Transmitno] = ModbusIpMaster.CreateIp(_tcpClient[Transmitno]);
			}
			return true;
		}
		catch (Exception ex)
		{
			GlobalDeclaration.StringLogger(ex.ToString(), "MBUS DCU, Mod_Bus", "ConnectTCPIP", GlobalDeclaration.strLogPath);
			return false;
		}
	}

	public void ReadMeterListDetails()
	{
		SqlDataAdapter sqlDataAdapter = new SqlDataAdapter();
		DataTable dataTable = new DataTable();
		try
		{
			string cmdText = "SELECT * FROM SETTINGS_MeterList Where MeterID<100000  order by MeterSN";
			SqlCommand sqlCommand2 = (sqlDataAdapter.SelectCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx));
			sqlDataAdapter.Fill(dataTable);
			for (int i = 0; i < dataTable.Rows.Count; i++)
			{
				ushort num = Convert.ToUInt16(dataTable.Rows[i]["MeterID"].ToString());
				VoltsMF[num] = ((dataTable.Rows[i]["MeterVoltsMF"].ToString() == "") ? 1f : Convert.ToSingle(dataTable.Rows[i]["MeterVoltsMF"]));
				AmpsMF[num] = ((dataTable.Rows[i]["MeterAmpsMF"].ToString() == "") ? 1f : Convert.ToSingle(dataTable.Rows[i]["MeterAmpsMF"]));
				PowerMF[num] = ((dataTable.Rows[i]["MeterPowerMF"].ToString() == "") ? 1f : Convert.ToSingle(dataTable.Rows[i]["MeterPowerMF"]));
				EnergyMF[num] = ((dataTable.Rows[i]["MeterIntMF"].ToString() == "") ? 1f : Convert.ToSingle(dataTable.Rows[i]["MeterIntMF"]));
				MeterModelID[num] = Convert.ToInt16(dataTable.Rows[i]["MeterModelID"].ToString());
				MeterScanRate[num] = ((dataTable.Rows[i]["MeterScanRate"].ToString() == "") ? 1000 : Convert.ToInt32(dataTable.Rows[i]["MeterScanRate"]));
				MeterRetry[num] = ((dataTable.Rows[i]["MeterRetry"].ToString() == "") ? Convert.ToByte(3) : Convert.ToByte(dataTable.Rows[i]["MeterRetry"]));
				DMeterID[num] = Convert.ToByte(dataTable.Rows[i]["DMeterID"].ToString());
			}
			dataTable.Clear();
			sqlDataAdapter.Dispose();
			sqlCommand2.Dispose();
		}
		catch (Exception ex)
		{
			GlobalDeclaration.StringLogger(ex.StackTrace.ToString() + ": " + ex.ToString(), "MBUS DCU, Mod_Bus", "ReadMeterListDetails", GlobalDeclaration.strLogPath);
		}
		finally
		{
		}
	}

	public void ReadParameterListDetails()
	{
		SqlCommand sqlCommand = null;
		SqlDataAdapter sqlDataAdapter = new SqlDataAdapter();
		DataSet dataSet = new DataSet();
		DataSet dataSet2 = new DataSet();
		DataSet dataSet3 = new DataSet();
		DataTable dataTable = new DataTable();
		DataTable dataTable2 = new DataTable();
		int num = 0;
		SqlDataReader sqlDataReader = null;
		try
		{
			try
			{
				string cmdText = "SELECT * FROM SETTINGS_ParameterList Where ParameterNameID>0  order by ParameterNameID";
				sqlCommand = (sqlDataAdapter.SelectCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx));
				sqlDataAdapter.Fill(dataTable);
				for (ushort num2 = 0; num2 < dataTable.Rows.Count; num2++)
				{
					ParamMF[num2] = Convert.ToInt16(dataTable.Rows[num2]["Scaling"].ToString());
					ParameterName[num2] = dataTable.Rows[num2]["ParameterName"].ToString();
				}
				dataTable.Clear();
			}
			catch (Exception ex)
			{
				GlobalDeclaration.StringLogger(ex.ToString(), "MBUS DCU, Mod_Bus", "ReadParameterListDetails, SETTINGS_ParameterList ", GlobalDeclaration.strLogPath);
			}
			try
			{
				string cmdText = " Select Max(ModelId) TotalModel from [dbo].[SETTINGS_Model]";
				sqlCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx);
				sqlDataReader = sqlCommand.ExecuteReader();
				if (sqlDataReader.Read())
				{
					TotalModels = Convert.ToUInt16(sqlDataReader["TotalModel"].ToString());
				}
				sqlDataReader.Close();
				TotalModelBlock = new int[TotalModels + 1];
			}
			catch (Exception ex2)
			{
				GlobalDeclaration.StringLogger(ex2.ToString(), "MBUS DCU, Mod_Bus", "ReadParameterListDetails, SETTINGS_Model ", GlobalDeclaration.strLogPath);
			}
			try
			{
				for (ushort num3 = 1; num3 <= TotalModels; num3++)
				{
                   // GlobalDeclaration.StringLogger(num3.ToString(), "MBUS DCU, Mod_Bus", "Model ID current ", GlobalDeclaration.strLogPath);
                    string cmdText = "select * from SETTINGS_Block where Modelid = " + num3 + " order by ModelID,BlockID";
					sqlCommand = (sqlDataAdapter.SelectCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx));
					sqlDataAdapter.Fill(dataTable);
					TotalModelBlock[num3] = dataTable.Rows.Count;
					//GlobalDeclaration.StringLogger(TotalModelBlock[num3].ToString(), "MBUS DCU, Mod_Bus", "Total Model Blocks", GlobalDeclaration.strLogPath);
                    for (ushort num2 = 1; num2 <= dataTable.Rows.Count; num2++)
					{
						ushort num4 = Convert.ToUInt16(dataTable.Rows[num2 - 1]["BlockID"].ToString());

						ModelFC[num3, num4] = Convert.ToUInt16(dataTable.Rows[num2 - 1]["FunctionCode"]);
                       // GlobalDeclaration.StringLogger(ModelFC[num3, num4].ToString(), "MBUS DCU, Mod_Bus", "Function Code ", GlobalDeclaration.strLogPath);
                        ModelStartAdd[num3, num4] = Convert.ToUInt16(dataTable.Rows[num2 - 1]["StartingAddress"]);
                       // GlobalDeclaration.StringLogger(ModelStartAdd[num3, num4].ToString(), "MBUS DCU, Mod_Bus", "start Address ", GlobalDeclaration.strLogPath);
                        ModelTotReg[num3, num4] = Convert.ToUInt16(dataTable.Rows[num2 - 1]["TotalRegister"]);
                        // GlobalDeclaration.StringLogger(ModelTotReg[num3, num4].ToString(), "MBUS DCU, Mod_Bus", "Total Register ", GlobalDeclaration.strLogPath);
                        ModelStartParam[num3, num4] = Convert.ToUInt16(dataTable.Rows[num2 - 1]["StartingParameter"]);
                       // GlobalDeclaration.StringLogger(ModelStartParam[num3, num4].ToString(), "MBUS DCU, Mod_Bus", "start Param ", GlobalDeclaration.strLogPath);
                        ModelEndParam[num3, num4] = Convert.ToUInt16(dataTable.Rows[num2 - 1]["EndingParameter"]);
                       // GlobalDeclaration.StringLogger(ModelEndParam[num3, num4].ToString(), "MBUS DCU, Mod_Bus", "End Parameter ", GlobalDeclaration.strLogPath);
                        cmdText = "select * from SETTINGS_AssignParameters where ModelID = " + num3 + " and BlockID = " + num4 + " order by ModelID,BlockID,ParameterID";
						sqlCommand = (sqlDataAdapter.SelectCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx));
						sqlDataAdapter.Fill(dataTable2);
                       // GlobalDeclaration.StringLogger(dataTable2.Rows.Count.ToString(), "MBUS DCU, Mod_Bus", "SETTINGS_AssignParameters count ", GlobalDeclaration.strLogPath);
                        for (ushort num5 = 0; num5 < dataTable2.Rows.Count; num5++)
						{
							ushort num6 = Convert.ToUInt16(dataTable2.Rows[num5]["ParameterID"]);
                         //   GlobalDeclaration.StringLogger(num6.ToString(), "MBUS DCU, Mod_Bus", "Parameter Id ", GlobalDeclaration.strLogPath);
                            ParameterIndex[num3, num4, num6] = Convert.ToUInt16(dataTable2.Rows[num5]["ParameterIndex"]);
                           // GlobalDeclaration.StringLogger(ParameterIndex[num3, num4, num6].ToString(), "MBUS DCU, Mod_Bus", "Parameter index value  ", GlobalDeclaration.strLogPath);
                            ParameterMF[num3, num4, num6] = Convert.ToSingle(dataTable2.Rows[num5]["MF"]);
                           // GlobalDeclaration.StringLogger(ParameterMF[num3, num4, num6].ToString(), "MBUS DCU, Mod_Bus", "MF value " , GlobalDeclaration.strLogPath);
                            ParameterDataType[num3, num4, num6] = ((dataTable2.Rows[num5]["DataType"].ToString() == "") ? Convert.ToSingle(0) : ((float)(int)Convert.ToUInt16(dataTable2.Rows[num5]["DataType"])));
                           // GlobalDeclaration.StringLogger(ParameterDataType[num3, num4, num6].ToString(), "MBUS DCU, Mod_Bus", "Data Type ", GlobalDeclaration.strLogPath);
                            ParamIndexMeterId[num3, num4, num6] = ((dataTable2.Rows[num5]["MeterID"].ToString() == "") ? Convert.ToUInt16(0) : Convert.ToUInt16(dataTable2.Rows[num5]["MeterID"]));
                           // GlobalDeclaration.StringLogger(ParamIndexMeterId[num3, num4, num6].ToString(), "MBUS DCU, Mod_Bus", "MeterId  "	, GlobalDeclaration.strLogPath);

                        }
						dataTable2.Clear();
					}
					dataTable.Clear();
				}
			}
			catch (Exception ex3)
			{
				GlobalDeclaration.StringLogger(ex3.ToString(), "MBUS DCU, Mod_Bus", "ReadParameterListDetails,SETTINGS_Block, SETTINGS_AssignParameters ", GlobalDeclaration.strLogPath);
			}
			sqlDataAdapter.Dispose();
			sqlCommand.Dispose();
		}
		catch (Exception ex4)
		{
			sqlDataReader.Close();
			GlobalDeclaration.StringLogger(ex4.ToString(), "MBUS DCU, Mod_Bus", "ReadParameterListDetails", GlobalDeclaration.strLogPath);
		}
		finally
		{
		}
	}

	public async Task ModBus_Serial(int Id)
	{
		byte Index = Convert.ToByte(GlobalDeclaration.TransDetails[Id].TypeId);
		int TransId = GlobalDeclaration.TransDetails[Id].TransmitId;
		for (int i = 0; i < GlobalDeclaration.TotalMeterCount; i++)
		{
			if (GlobalDeclaration.MeterDetails[i].TransmitId == TransId)
			{
				byte Mid = Convert.ToByte(GlobalDeclaration.MeterDetails[i].MeterId);
				byte DMeterId = Convert.ToByte(GlobalDeclaration.MeterDetails[i].DMeterId);
				await PollSerialPort(Index, Mid, DMeterId, Id);
			}
		}
	}

	public async Task ModBus_TCP(int Id)
	{
		byte Index = Convert.ToByte(GlobalDeclaration.TransDetails[Id].TypeId);
		int TransId = GlobalDeclaration.TransDetails[Id].TransmitId;
		for (int i = 0; i < GlobalDeclaration.TotalMeterCount; i++)
		{
			if (GlobalDeclaration.MeterDetails[i].TransmitId == TransId)
			{
				byte Mid = Convert.ToByte(GlobalDeclaration.MeterDetails[i].MeterId);
				byte DMeterId = Convert.ToByte(GlobalDeclaration.MeterDetails[i].DMeterId);
				await PollTCPIP(Index, Mid, DMeterId, Id);
			}
		}
	}

	public async Task PollSerialPort(byte _index, byte _slaveID, byte DMeterid, int Id)
	{
		DateTime LogTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
		try
		{
			while (PollStatus[_index])
			{
				Task.Delay(1000);
			}
			PollStatus[_index] = true;
			((ModbusDevice)_masterRTU[_index]).Transport.Retries = MeterRetry[_slaveID];
			((ModbusDevice)_masterRTU[_index]).Transport.ReadTimeout = Convert.ToInt16(MeterScanRate[_slaveID]);
			ushort[] holding_register = new ushort[10];
			if (!_SerialPort[_index].IsOpen)
			{
				_SerialPort[_index].Open();
			}
			for (int i = 1; i <= TotalModelBlock[MeterModelID[_slaveID]]; i++)
			{
				if (ModelFC[MeterModelID[_slaveID], i] == 3)
				{
					holding_register = ((ModbusMaster)_masterRTU[_index]).ReadHoldingRegisters(DMeterid, ModelStartAdd[MeterModelID[_slaveID], i], ModelTotReg[MeterModelID[_slaveID], i]);
				}
				else if (ModelFC[MeterModelID[_slaveID], i] == 4)
				{
					holding_register = ((ModbusMaster)_masterRTU[_index]).ReadInputRegisters(DMeterid, ModelStartAdd[MeterModelID[_slaveID], i], ModelTotReg[MeterModelID[_slaveID], i]);
				}
				string HexString = ByteArrayToString(holding_register);
				if (holding_register.Length == ModelTotReg[MeterModelID[_slaveID], i])
				{
					DecodeData(HexString, i, _slaveID);
				}
			}
			SaveData(_slaveID, 1, LogTime);
		}
		catch (Exception ex2)
		{
			Exception ex = ex2;
			SaveData(_slaveID, 0, LogTime);
			string Transmit = GlobalDeclaration.TransDetails[Id].TransmitName;
			GlobalDeclaration.StringLogger(ex.ToString(), "MBUS DCU Mod_Bus", "PollSerialPort:Error in " + Transmit + ", " + _slaveID, GlobalDeclaration.strLogPath);
		}
		finally
		{
			PollStatus[_index] = false;
			GlobalDeclaration.UpdateHistory(Id);
		}
	}

	public async Task PollTCPIP(byte _index, byte _slaveID, byte DMeterid, int Id)
	{
		DateTime LogTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
		try
		{
			while (PollStatus[_index])
			{
				Task.Delay(1000);
			}
			PollStatus[_index] = true;
			byte modbuscomm = modbusCommbyte[_index];
			switch (modbuscomm)
			{
			case 0:
				((ModbusDevice)_masterRTU[_index]).Transport.Retries = MeterRetry[_slaveID];
				((ModbusDevice)_masterRTU[_index]).Transport.ReadTimeout = MeterScanRate[_slaveID];
				break;
			case 1:
				((ModbusDevice)_masterModbus[_index]).Transport.Retries = MeterRetry[_slaveID];
				((ModbusDevice)_masterModbus[_index]).Transport.ReadTimeout = MeterScanRate[_slaveID];
				break;
			}
			ushort[] holding_register = new ushort[10];
			for (int i = 1; i <= TotalModelBlock[MeterModelID[_slaveID]]; i++)
			{
				switch (modbuscomm)
				{
				case 0:
					if (ModelFC[MeterModelID[_slaveID], i] == 3)
					{
						holding_register = ((ModbusMaster)_masterRTU[_index]).ReadHoldingRegisters(DMeterid, ModelStartAdd[MeterModelID[_slaveID], i], ModelTotReg[MeterModelID[_slaveID], i]);
					}
					else if (ModelFC[MeterModelID[_slaveID], i] == 4)
					{
						holding_register = ((ModbusMaster)_masterRTU[_index]).ReadInputRegisters(DMeterid, ModelStartAdd[MeterModelID[_slaveID], i], ModelTotReg[MeterModelID[_slaveID], i]);
					}
					break;
				case 1:
					if (ModelFC[MeterModelID[_slaveID], i] == 3)
					{
						holding_register = ((ModbusMaster)_masterModbus[_index]).ReadHoldingRegisters(DMeterid, ModelStartAdd[MeterModelID[_slaveID], i], ModelTotReg[MeterModelID[_slaveID], i]);
					}
					else if (ModelFC[MeterModelID[_slaveID], i] == 4)
					{
						holding_register = ((ModbusMaster)_masterModbus[_index]).ReadInputRegisters(DMeterid, ModelStartAdd[MeterModelID[_slaveID], i], ModelTotReg[MeterModelID[_slaveID], i]);
					}
					break;
				}
				string HexString = ByteArrayToString(holding_register);
				if (holding_register.Length == ModelTotReg[MeterModelID[_slaveID], i])
				{
					DecodeData(HexString, i, _slaveID);
				}
			}
			SaveData(_slaveID, 1, LogTime);
		}
		catch (Exception ex2)
		{
			Exception ex = ex2;
			SaveData(_slaveID, 0, LogTime);
			string TransmitNme = GlobalDeclaration.TransDetails[Id].TransmitName;
			if (ex.Message == "The operation is not allowed on non-connected sockets." || ex.Message.Contains("Response was not of expected"))
			{
				short PortNo = Convert.ToInt16(GlobalDeclaration.TransDetails[Id].PortNo);
				ConnectTCPIP(TransmitNme, PortNo, _index);
			}
			GlobalDeclaration.StringLogger(ex.ToString(), "MBUS DCU Mod_Bus", "PollTCPIP:Error in " + TransmitNme + ", " + _slaveID, GlobalDeclaration.strLogPath);
		}
		finally
		{
			PollStatus[_index] = false;
			GlobalDeclaration.UpdateHistory(Id);
		}
	}

	public static string ByteArrayToString(ushort[] data)
	{
		string text = "";
		foreach (ushort num in data)
		{
			string text2 = Convert.ToString(num, 16);
			if (text2.Length == 1)
			{
				text = text + "000" + text2;
			}
			else if (text2.Length == 2)
			{
				text = text + "00" + text2;
			}
			else if (text2.Length == 3)
			{
				text = text + "0" + text2;
			}
			else if (text2.Length == 4)
			{
				text += text2;
			}
		}
		return text.ToString().ToUpper();
	}

	public void DecodeData(string ReceivedData, int PollIndex, int slaveID, int ModelId = 0)
	{
		string text = "";
		bool flag = false;
		try
		{
			ReceivedData = ReceivedData.Replace(" ", string.Empty);
			if (ModelId == 0)
			{
				ModelId = MeterModelID[slaveID];
			}
			else
			{
				flag = true;
			}
			ushort num = ModelStartParam[ModelId, PollIndex];
			ushort num2 = ModelEndParam[ModelId, PollIndex];
			int num3 = 0;
			for (int i = num; i <= num2; i++)
			{
				float num4 = ParameterDataType[ModelId, PollIndex, i];
				int num5 = ParameterIndex[ModelId, PollIndex, i];
				if (flag)
				{
					slaveID = ParamIndexMeterId[ModelId, PollIndex, i];
				}
				if (num5 != 0)
				{
					string text2;
					if (num4 == 0f)
					{
						text2 = ReceivedData.Substring(num3 + 6, 2) + ReceivedData.Substring(num3 + 4, 2) + ReceivedData.Substring(num3 + 2, 2) + ReceivedData.Substring(num3, 2);
						num3 += 8;
					}
					else if (num4 == 1f || num4 == 4f)
					{
						text2 = ReceivedData.Substring(num3 + 4, 4) + ReceivedData.Substring(num3, 4);
						num3 += 8;
					}
					else if (num4 == 2f || num4 == 3f)
					{
						text2 = ReceivedData.Substring(num3, 4) + ReceivedData.Substring(num3 + 4, 4);
						num3 += 8;
					}
					else if (num4 == 8f)
					{
						text2 = ReceivedData.Substring(num3 + 12, 4) + ReceivedData.Substring(num3 + 8, 4) + ReceivedData.Substring(num3 + 4, 4) + ReceivedData.Substring(num3, 4);
						num3 += 16;
					}
					else if (num4 == 7f)
					{
						text2 = ReceivedData.Substring(num3, 16);
						num3 += 16;
					}
					else if (num4 == 9f)
					{
						text2 = ReceivedData.Substring(num3, 8);
						num3 += 8;
					}
					else
					{
						text2 = ReceivedData.Substring(num3, 4);
						num3 += 4;
					}
					if (num4 <= 2f)
					{
						RealTimeData[slaveID, num5] = Hex2Float(text2);
					}
					else if (num4 <= 4f)
					{
						if (text2.Substring(1, 4) == "FFFF")
						{
							text2 = text2.Substring(5, 4);
							long num6 = Convert.ToInt64(text2, 16);
							RealTimeData[slaveID, num5] = 65535 - num6;
						}
						else
						{
							long num6 = Convert.ToInt64(text2, 16);
							RealTimeData[slaveID, num5] = num6;
						}
					}
					else if (num4 == 5f)
					{
						RealTimeData[slaveID, num5] = Convert.ToInt64(text2, 16);
					}
					else if (num4 == 6f)
					{
						RealTimeData[slaveID, num5] = Convert.ToInt64(text2, 16);
					}
					else if (num4 == 8f)
					{
						string text3 = "";
						long num7 = 0L;
						for (byte b = 0; b <= 16; b += 4)
						{
							text = text2.Substring(b, 4);
							num7 += Convert.ToInt64(text, 16);
						}
						RealTimeData[slaveID, num5] = num7;
					}
					else if (num4 == 7f)
					{
						RealTimeData[slaveID, num5] = Convert.ToInt64(text2, 16);
					}
					else if (num4 == 9f)
					{
						text = text2.Substring(1, 4);
						string text3 = text2.Substring(5, 4);
						long num8 = Convert.ToInt64(text, 16) * 10000;
						RealTimeData[slaveID, num5] = num8 + Convert.ToInt64(text3, 16);
					}
					RealTimeData[slaveID, num5] = Convert.ToSingle(RealTimeData[slaveID, num5]) * Convert.ToSingle(ParameterMF[MeterModelID[slaveID], PollIndex, i]);
					if (ParamMF[num5] == 1)
					{
						RealTimeData[slaveID, num5] *= VoltsMF[slaveID];
					}
					else if (ParamMF[num5] == 2)
					{
						RealTimeData[slaveID, num5] *= AmpsMF[slaveID];
					}
					else if (ParamMF[num5] == 3)
					{
						RealTimeData[slaveID, num5] *= PowerMF[slaveID];
					}
					else if (ParamMF[num5] == 4)
					{
						RealTimeData[slaveID, num5] *= EnergyMF[slaveID];
					}
					RealTimeData[slaveID, num5] = Convert.ToSingle(Math.Round(RealTimeData[slaveID, num5], 6));
				}
				else
				{
					num3 = ((!(num4 <= 4f)) ? ((num4 != 7f) ? (num3 + 4) : (num3 + 16)) : (num3 + 8));
				}
			}
		}
		catch (Exception ex)
		{
			GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU Mod_Bus", "DecodeData -  Meter ID: " + slaveID + " ...", GlobalDeclaration.strLogPath);
		}
	}

	private void SaveData(int _SlaveID, byte status, DateTime LogTime)
	{
		SqlCommand sqlCommand = new SqlCommand();
		try
		{
            GlobalDeclaration.StringLogger("Slave id = "+_SlaveID.ToString(), "Mod_Bus", "SaveData", GlobalDeclaration.strLogPath);
            sqlCommand.Connection = GlobalDeclaration._ASIMBUSDATA;
			sqlCommand.CommandTimeout = 300;
			sqlCommand.Parameters.AddWithValue("LogTime", LogTime);
			for (int i = 1; i <= 300; i++)
			{
				if (RealTimeData[_SlaveID, i] == 0f)
				{
					sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
				}
				else if (status == 0)
				{
					sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
				}
				else
				{
					sqlCommand.Parameters.AddWithValue("V" + i, RealTimeData[_SlaveID, i]);
                    GlobalDeclaration.StringLogger("V" + i.ToString() + " = " + RealTimeData[_SlaveID, i], "Mod_Bus", "SaveData", GlobalDeclaration.strLogPath);
                }
			}
			sqlCommand.CommandType = CommandType.StoredProcedure;
			sqlCommand.CommandText = "SP_MACGRID15HISTORIAN" + _SlaveID;
			sqlCommand.ExecuteNonQuery();
			sqlCommand.Dispose();
		}
		catch (Exception ex)
		{
			sqlCommand.Dispose();
			GlobalDeclaration.StringLogger(ex.Message.ToString() + "\nSaving Data... ", "MBUS DCU  Mod_Bus", "SaveData", GlobalDeclaration.strLogPath);
		}
		try
		{
			sqlCommand.Parameters.AddWithValue("Active", status);
			sqlCommand.Parameters.AddWithValue("MeterID", _SlaveID);
			sqlCommand.CommandType = CommandType.StoredProcedure;
			sqlCommand.CommandText = "SP_LIVEDATA";
			sqlCommand.ExecuteNonQuery();
			sqlCommand.Dispose();
			sqlCommand.Parameters.Clear();
		}
		catch (Exception ex2)
		{
			sqlCommand.Dispose();
			sqlCommand.Parameters.Clear();
			if (!ex2.Message.Contains("PRIMARY KEY"))
			{
				GlobalDeclaration.StringLogger(ex2.Message.ToString() + "\nSaving Realtime Data ", "MBUS DCU  Mod_Bus", "SaveData RealTime", GlobalDeclaration.strLogPath);
			}
		}
	}

	private float Hex2Float(string hex_string)
	{
		float result = 0f;
		try
		{
			uint value = uint.Parse(hex_string, NumberStyles.AllowHexSpecifier);
			byte[] bytes = BitConverter.GetBytes(value);
			result = BitConverter.ToSingle(bytes, 0);
		}
		catch (Exception ex)
		{
			GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU Mod_Bus", "Hex2Float", GlobalDeclaration.strLogPath);
		}
		return result;
	}

	public void Port_Disable()
	{
		try
		{
			try
			{
				for (int i = 0; i < _tcpClient.Length; i++)
				{
					_tcpClient[i].Close();
				}
			}
			catch (Exception)
			{
			}
			try
			{
				for (int j = 0; j < _SerialPort.Length; j++)
				{
					_SerialPort[j].Close();
				}
			}
			catch (Exception)
			{
			}
		}
		catch (Exception ex3)
		{
			GlobalDeclaration.StringLogger(ex3.Message.ToString(), "MBUS DCU Mod_Bus", "Port_Disable", GlobalDeclaration.strLogPath);
		}
	}
}
