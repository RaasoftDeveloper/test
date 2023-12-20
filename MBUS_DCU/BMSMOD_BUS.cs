using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Modbus.Device;

namespace MBUS_DCU;

public class BMSMOD_BUS : Mod_Bus
{
	public async Task BMSMODBUS_Poll(int Id)
	{
		byte Index = Convert.ToByte(GlobalDeclaration.TransDetails[Id].TypeId);
		_ = GlobalDeclaration.TransDetails[Id].TransmitId;
		ushort Model = GlobalDeclaration.TransDetails[Id].ModelId;
		if (GlobalDeclaration.TransDetails[Id].TransmitMode == "TCP")
		{
			await BMS_PollTCPIP(Index, Model, Id);
		}
		else if (GlobalDeclaration.TransDetails[Id].TransmitMode == "Serial")
		{
			await BMS_PollTCPIP(Index, Model, Id);
		}
	}

	public async Task BMS_PollTCPIP(byte _index, ushort ModelId, int Id)
	{
		byte DMeterid = 1;
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
				((ModbusDevice)_masterRTU[_index]).Transport.Retries = 3;
				((ModbusDevice)_masterRTU[_index]).Transport.ReadTimeout = 1000;
				break;
			case 1:
				((ModbusDevice)_masterModbus[_index]).Transport.Retries = 3;
				((ModbusDevice)_masterModbus[_index]).Transport.ReadTimeout = 1000;
				break;
			}
			ushort[] holding_register = new ushort[10];
			for (int i = 1; i <= TotalModelBlock[ModelId]; i++)
			{
				switch (modbuscomm)
				{
				case 0:
					if (ModelFC[ModelId, i] == 3)
					{
						holding_register = ((ModbusMaster)_masterRTU[_index]).ReadHoldingRegisters(DMeterid, ModelStartAdd[ModelId, i], ModelTotReg[ModelId, i]);
					}
					else if (ModelFC[ModelId, i] == 4)
					{
						holding_register = ((ModbusMaster)_masterRTU[_index]).ReadInputRegisters(DMeterid, ModelStartAdd[ModelId, i], ModelTotReg[ModelId, i]);
					}
					break;
				case 1:
					if (ModelFC[ModelId, i] == 3)
					{
						holding_register = ((ModbusMaster)_masterModbus[_index]).ReadHoldingRegisters(DMeterid, ModelStartAdd[ModelId, i], ModelTotReg[ModelId, i]);
					}
					else if (ModelFC[ModelId, i] == 4)
					{
						holding_register = ((ModbusMaster)_masterModbus[_index]).ReadInputRegisters(DMeterid, ModelStartAdd[ModelId, i], ModelTotReg[ModelId, i]);
					}
					break;
				}
				string HexString = Mod_Bus.ByteArrayToString(holding_register);
				if (holding_register.Length == ModelTotReg[ModelId, i])
				{
					DecodeData(HexString, i, 0, ModelId);
				}
			}
			SaveData_BMS(1, LogTime, ModelId);
		}
		catch (Exception ex2)
		{
			Exception ex = ex2;
			SaveData_BMS(0, LogTime, ModelId);
			string TransmitNme = GlobalDeclaration.TransDetails[Id].TransmitName;
			if (ex.Message == "The operation is not allowed on non-connected sockets." || ex.Message.Contains("Response was not of expected"))
			{
				short PortNo = Convert.ToInt16(GlobalDeclaration.TransDetails[Id].PortNo);
				ConnectTCPIP(TransmitNme, PortNo, _index);
			}
			GlobalDeclaration.StringLogger(ex.ToString(), "MBUS DCU BMSMOD_BUS", "BMS_PollTCPIP:Error in " + TransmitNme, GlobalDeclaration.strLogPath);
		}
		finally
		{
			PollStatus[_index] = false;
			GlobalDeclaration.UpdateHistory(Id);
		}
	}

	public async Task BMS_PollSerial(byte _index, ushort ModelId, int Id)
	{
		byte DMeterid = 1;
		DateTime LogTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
		try
		{
			while (PollStatus[_index])
			{
				Task.Delay(1000);
			}
			PollStatus[_index] = true;
			((ModbusDevice)_masterRTU[_index]).Transport.Retries = 3;
			((ModbusDevice)_masterRTU[_index]).Transport.ReadTimeout = 2000;
			ushort[] holding_register = new ushort[10];
			if (!_SerialPort[_index].IsOpen)
			{
				_SerialPort[_index].Open();
			}
			for (int i = 1; i <= TotalModelBlock[ModelId]; i++)
			{
				if (ModelFC[ModelId, i] == 3)
				{
					holding_register = ((ModbusMaster)_masterRTU[_index]).ReadHoldingRegisters(DMeterid, ModelStartAdd[ModelId, i], ModelTotReg[ModelId, i]);
				}
				else if (ModelFC[ModelId, i] == 4)
				{
					holding_register = ((ModbusMaster)_masterRTU[_index]).ReadInputRegisters(DMeterid, ModelStartAdd[ModelId, i], ModelTotReg[ModelId, i]);
				}
				string HexString = Mod_Bus.ByteArrayToString(holding_register);
				if (holding_register.Length == ModelTotReg[ModelId, i])
				{
					DecodeData(HexString, i, 0, ModelId);
				}
			}
			SaveData_BMS(1, LogTime, ModelId);
		}
		catch (Exception ex2)
		{
			Exception ex = ex2;
			SaveData_BMS(0, LogTime, ModelId);
			GlobalDeclaration.StringLogger(_ProcedureName: "PollSerialPort:Error in " + GlobalDeclaration.TransDetails[Id].TransmitName, _string: ex.ToString(), _ModuleName: "MBUS DCU Mod_Bus ", _Path: GlobalDeclaration.strLogPath);
		}
		finally
		{
			PollStatus[_index] = false;
			GlobalDeclaration.UpdateHistory(Id);
		}
	}

	private void SaveData_BMS(byte status, DateTime LogTime, ushort ModelId)
	{
		SqlCommand sqlCommand = new SqlCommand();
		try
		{
			int count = GlobalDeclaration.MeterModelID.Count;
			for (int i = 0; i < count; i++)
			{
				if (ModelId != GlobalDeclaration.MeterModelID[i].ModelId)
				{
					continue;
				}
				int meterId = GlobalDeclaration.MeterModelID[i].MeterId;
				if (meterId == 0)
				{
					continue;
				}
				try
				{
					sqlCommand.Connection = GlobalDeclaration._ASIMBUSDATA;
					sqlCommand.CommandTimeout = 300;
					sqlCommand.Parameters.AddWithValue("LogTime", LogTime);
					for (int j = 1; j <= 300; j++)
					{
						if (RealTimeData[meterId, j] == 0f)
						{
							sqlCommand.Parameters.AddWithValue("V" + j, Convert.DBNull);
						}
						else if (status == 0)
						{
							sqlCommand.Parameters.AddWithValue("V" + j, Convert.DBNull);
						}
						else
						{
							sqlCommand.Parameters.AddWithValue("V" + j, RealTimeData[meterId, j]);
                            GlobalDeclaration.StringLogger("V" + i.ToString() + " = " + RealTimeData[meterId, j], "BMSMOD_BUS", "SaveData", GlobalDeclaration.strLogPath);
                        }
					}
					sqlCommand.CommandType = CommandType.StoredProcedure;
					sqlCommand.CommandText = "SP_MACGRID15HISTORIAN" + meterId;
					sqlCommand.ExecuteNonQuery();
					sqlCommand.Dispose();
				}
				catch (Exception ex)
				{
					sqlCommand.Dispose();
					GlobalDeclaration.StringLogger(ex.Message.ToString() + "\nSaving Data... ", "MBUS DCU  BMSMOD_BUS", "SaveData", GlobalDeclaration.strLogPath);
				}
				try
				{
					sqlCommand.Parameters.AddWithValue("Active", status);
					sqlCommand.Parameters.AddWithValue("MeterID", meterId);
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
		}
		catch (Exception ex3)
		{
			sqlCommand.Dispose();
			GlobalDeclaration.StringLogger(ex3.Message.ToString(), "MBUS DCU  BMSMOD_BUS", "SaveData", GlobalDeclaration.strLogPath);
		}
	}
}
