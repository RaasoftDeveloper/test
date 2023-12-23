using MBUS_DCU.Properties;
using SharedMemory;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.IO.Ports;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using System.Windows.Forms;
using Valley.Net.Protocols.MeterBus;
using Valley.Net.Protocols.MeterBus.EN13757_2;
using Valley.Net.Protocols.MeterBus.EN13757_3;

namespace MBUS_DCU;
public class frmMain : Form
{
    public class Import
    {
        public int ColumnIndex { get; set; }

        public string ColumnName { get; set; }
    }

    public class Integrated
    {
        public int ParamId { get; set; }

        public int IntegratedId { get; set; }
    }

    private ArrayList DIFE1 = new ArrayList();

    private ArrayList DIFE2 = new ArrayList();

    private ArrayList DIFE3 = new ArrayList();

    private ArrayList DIFE4 = new ArrayList();

    private ArrayList DIFE5 = new ArrayList();

    private ArrayList DIFE6 = new ArrayList();

    private ArrayList DIFE7 = new ArrayList();

    private ArrayList DIFE8 = new ArrayList();

    private ArrayList DIFE9 = new ArrayList();

    private ArrayList DIFE10 = new ArrayList();

    private ArrayList VIFE1 = new ArrayList();

    private TcpClient[] MBUSTcpClients;

    private string[] TCPPortName;

    private int[] TCPPortNo;

    private string[] rxBuff;

    private string[] tcprxBuff;

    private string[] Retry;

    private float[,] RealTimeData = new float[10024, 301];

    private byte byteload = 0;

    private ArrayList MBUSSerialPorts = new ArrayList();

    private DateTime[] LastPoll;

    private DateTime LastDBCheck = DateTime.Now;

    private DateTime LastRepeatPoll = DateTime.Now;

    private Timer MyTimer = new Timer();

    private Timer Timer_ValveSchedule = new Timer();

    private Timer MemoryCheck = new Timer();

    private Timer Timer_SchedulePoll = new Timer();

    private Timer Timer_MeterSchedule = new Timer();

    private bool[] Serial_Status;

    private bool[] TCP_Status;

    private DateTime[] ValveTime;

    private DateTime[] ScheduleTime;

    private DateTime[] ImportTime;

    private DateTime[] MeterSchTime;

    private string Error = "";

    private BMSMOD_BUS ObjBMS = new BMSMOD_BUS();

    private IContainer components = null;

    private NotifyIcon nfi;

    private ContextMenuStrip contextMenuStrip1;

    private ToolStripMenuItem tsStop;

    private ToolStripMenuItem tsStart;

    private ToolStripMenuItem tsExit;
    private ToolTip toolTip1;

    public frmMain()
    {
        if (byteload == 0)
        {
            base.Visible = false;
            byteload = 1;
        }
        InitializeComponent();
    }

    private void Form1_Load(object sender, EventArgs e)
    {
        GlobalDeclaration.strLogPath = Environment.CurrentDirectory + "\\LOGS\\DCU";
        if (GlobalDeclaration.ErrorTest)
        {
            GlobalDeclaration.StringLogger("Polling Started at :" + DateTime.Now, "MBUS TEST", "Frm Main", GlobalDeclaration.strLogPath);
        }
        GlobalDeclaration.GetCustomerId();
        Loadconfig();
        AppDomain.CurrentDomain.ProcessExit += OnProcessExit;
        GlobalDeclaration.ClearTransmitPoll();
        GlobalDeclaration._TotlaSlaveCount = GlobalDeclaration.GetTotalSlaveCount();
        GlobalDeclaration.TotalSerial = GlobalDeclaration.GetTotalPorts(1);
        GlobalDeclaration.TotalTCP = GlobalDeclaration.GetTotalPorts(2);
        rxBuff = new string[GlobalDeclaration.TotalSerial];
        tcprxBuff = new string[GlobalDeclaration.TotalTCP];
        Serial_Status = new bool[GlobalDeclaration.TotalSerial];
        TCP_Status = new bool[GlobalDeclaration.TotalTCP];
        TCPPortName = new string[GlobalDeclaration.TotalTCP];
        TCPPortNo = new int[GlobalDeclaration.TotalTCP];
        MBUSTcpClients = new TcpClient[GlobalDeclaration.TotalTCP];
        Retry = new string[GlobalDeclaration.TotalSerial];
        byteload = 0;
        GlobalDeclaration.MeterModelID = GlobalDeclaration.GetMeterModelID();
        GlobalDeclaration.TransDetails = GlobalDeclaration.GetTransmitDetails();
        LastPoll = new DateTime[GlobalDeclaration.TransDetails.Count];
        for (int i = 0; i < GlobalDeclaration.TransDetails.Count; i++)
        {
            LastPoll[i] = DateTime.Now;
        }
        GlobalDeclaration.MeterDetails = GlobalDeclaration.GetMeterDetails();
        ObjBMS.SetVariables();
        ObjBMS.ReadMeterListDetails();
        ObjBMS.ReadParameterListDetails();
        ObjBMS.CreateTransmit();
        GlobalDeclaration.TelegramDetailsGet();
        GlobalDeclaration.GatewayTelegram = GlobalDeclaration.GetGateWayTelegram();
        SetSerialPort();
        SetTCPPort();
        if (GlobalDeclaration.IsSaveIntegrated)
        {
            GlobalDeclaration.IntegratedDetailsGet();
        }
        GlobalDeclaration.TransmitSchedule = GlobalDeclaration.GetScheduleDetails();
        ScheduleTime = new DateTime[GlobalDeclaration.TransmitSchedule.Count];
        GlobalDeclaration.ImportSchedule = GlobalDeclaration.GetImportSchedule();
        ImportTime = new DateTime[GlobalDeclaration.ImportSchedule.Count];
        if (GlobalDeclaration.TransmitSchedule.Count > 0 || GlobalDeclaration.ImportSchedule.Count > 0)
        {
            Timer_SchedulePoll.Interval = 1000;
            Timer_SchedulePoll.Start();
            Timer_SchedulePoll.Enabled = true;
            Timer_SchedulePoll.Tag = 1;
            Timer_SchedulePoll.Tick += ScheduleTriggerTime;
        }
        GlobalDeclaration.GetValveStatusTag();
        GlobalDeclaration.ValveDetails = GlobalDeclaration.GetValveDetails();
        if (GlobalDeclaration.ValveDetails.Count > 0)
        {
            ValveTime = new DateTime[GlobalDeclaration.ValveDetails.Count];
            Timer_ValveSchedule.Interval = 1000;
            Timer_ValveSchedule.Start();
            Timer_ValveSchedule.Enabled = true;
            Timer_ValveSchedule.Tag = 1;
            Timer_ValveSchedule.Tick += ValveTriggerTime;
        }
        MemoryCheck.Interval = 1000;
        MemoryCheck.Start();
        MemoryCheck.Enabled = true;
        MemoryCheck.Tag = 1;
        MemoryCheck.Tick += SharedMemoryCheck;
        GlobalDeclaration.MeterSchedule = GlobalDeclaration.GetMeterScheduleDetails();
        MeterSchTime = new DateTime[GlobalDeclaration.MeterSchedule.Count];
        if (GlobalDeclaration.MeterSchedule.Count > 0)
        {
            Timer_MeterSchedule.Interval = 1000;
            Timer_MeterSchedule.Start();
            Timer_MeterSchedule.Enabled = true;
            Timer_MeterSchedule.Tag = 1;
            Timer_MeterSchedule.Tick += MeterScheduleTrigger;
        }
        string tipText = "Started";
        tsStart.Visible = false;
        tsStop.Visible = true;
        if (GlobalDeclaration.InitialReset)
        {
            DateTime logTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
            for (int j = 0; j <= GlobalDeclaration._TotlaSlaveCount - 1; j++)
            {
                SaveLiveDataOnly(j + 1, 0, logTime);
            }
        }
        nfi.ShowBalloonTip(1000, "SMART MBUS HYBRID - POLLING", tipText, ToolTipIcon.Info);

        //Timer MyTimer = new Timer();
        //MyTimer.Interval = (1000);
        //MyTimer.Tick += new EventHandler(MyTimer_Tick);
        //MyTimer.Start();

    }
    //private void MyTimer_Tick(object sender, EventArgs e)
    //{
    //    string Recieved = "68 c9 c9 68 08 12 72 18 99 64 80 2d 2c 35 0a 20 10 00 00 04 fb 00 07 40 00 00 04 ff 07 cc 6e 01 00 04 ff 08 a8 a1 01 00 04 16 fd d7 0f 00 84 40 14 00 00 00 00 84 80 40 14 00 00 00 00 04 22 48 72 00 00 34 22 35 06 00 00 02 59 e2 02 02 5d e3 03 02 61 ff fe 04 2e 00 00 00 00 14 2e 6d ff ff ff 04 3c 00 00 00 00 14 3c 7c 11 00 00 04 ff 22 00 01 00 00 04 6d 3a 31 f5 2c 44 fb 00 84 3e 00 00 44 ff 07 be 6b 01 00 44 ff 08 4d 9d 01 00 44 16 71 af 0f 00 c4 40 14 00 00 00 00 c4 80 40 14 00 00 00 00 54 2e 1f ff ff ff 54 3c 08 14 00 00 42 6c e1 2c 02 ff 1a 03 1b 0c 78 18 99 64 80 04 ff 16 e6 84 1e 00 04 ff 17 c1 d5 b4 00 a9 16";
    //    int ModelID = 86;
    //    int SlaveID = 14;
    //    int NoOfTags = 32;
    //    DateTime LogTime = DateTime.Now;


    //    ParseMBUSData(Recieved, ModelID, SlaveID, NoOfTags, LogTime, "");

    //}

    private void OnProcessExit(object sender, EventArgs e)
    {
        nfi.Visible = false;
    }

    private void Loadconfig()
    {
        try
        {
            string selectCommandText = "Select [PollingTime],[PollingID],[MID],[ThirdParty],[ThirdPartyGateway],[PushDatatoCloud],[CloudIP],[NoofRetries],[DongleAlert],[AlarmVal],[NoofModel],ISNULL([IsRepeatPoll],0) [IsRepeatPoll],ISNULL([InitialReset],0) [InitialReset],ISNULL([SaveIntegratedTags],0) [SaveIntegratedTags] FROM [dbo].[MBUS_Configuration]";
            SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(selectCommandText, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            DataRow dataRow = null;
            DataTable dataTable = new DataTable();
            dataTable.Rows.Clear();
            sqlDataAdapter.Fill(dataTable);
            DataTableReader dataTableReader = dataTable.CreateDataReader();
            dataTableReader.Read();
            if (dataTableReader.HasRows)
            {
                dataRow = dataTable.Rows[0];
                GlobalDeclaration._PollingID = Convert.ToByte(dataRow["PollingID"]);
                GlobalDeclaration._MID = Convert.ToByte(dataRow["MID"]);
                GlobalDeclaration._ThirdParty = Convert.ToByte(dataRow["ThirdParty"]);
                GlobalDeclaration._ThirdPartyGateway = Convert.ToByte(dataRow["ThirdPartyGateway"]);
                GlobalDeclaration._PushDatatoCloud = Convert.ToByte(dataRow["PushDatatoCloud"]);
                GlobalDeclaration._CloudIP = dataRow["CloudIP"].ToString();
                GlobalDeclaration.ISRepeatPoll = Convert.ToBoolean(dataRow["IsRepeatPoll"].ToString());
                GlobalDeclaration.InitialReset = Convert.ToBoolean(dataRow["InitialReset"].ToString());
                GlobalDeclaration.IsSaveIntegrated = Convert.ToBoolean(dataRow["SaveIntegratedTags"].ToString());
                if (Convert.ToByte(dataRow["NoofRetries"]) == 0)
                {
                    GlobalDeclaration._NoofRetries = 3;
                }
                else if (Convert.ToByte(dataRow["NoofRetries"]) == 1)
                {
                    GlobalDeclaration._NoofRetries = 4;
                }
                else if (Convert.ToByte(dataRow["NoofRetries"]) == 2)
                {
                    GlobalDeclaration._NoofRetries = 5;
                }
                else if (Convert.ToByte(dataRow["NoofRetries"]) == 3)
                {
                    GlobalDeclaration._NoofRetries = 6;
                }
            }
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU frmMain", "Loadconfig", GlobalDeclaration.strLogPath);
        }
    }

    private void SetSerialPort()
    {
        string text = "";
        string text2 = "";
        text2 = "select * from SETTINGS_Transmit Where TransmitMode = 1 AND [Active]=1 AND [GatewayTypeId]=1 Order by TransmitID";
        SqlCommand sqlCommand = new SqlCommand(text2, GlobalDeclaration._ASIMBUSSETTINGSCnx);
        SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
        try
        {
            int num = 0;
            while (sqlDataReader.Read())
            {
                try
                {
                    text = sqlDataReader["TransmitName"].ToString();
                    MBUSSerialPorts.Add(new SerialPort());
                    (MBUSSerialPorts[num] as SerialPort).BaudRate = Convert.ToInt32(sqlDataReader["Baudrate"]);
                    if (sqlDataReader["Parity"].ToString() == "ODD")
                    {
                        (MBUSSerialPorts[num] as SerialPort).Parity = Parity.Odd;
                    }
                    else if (sqlDataReader["Parity"].ToString() == "EVEN")
                    {
                        (MBUSSerialPorts[num] as SerialPort).Parity = Parity.Even;
                    }
                    else if (sqlDataReader["Parity"].ToString() == "NONE")
                    {
                        (MBUSSerialPorts[num] as SerialPort).Parity = Parity.None;
                    }
                    if (sqlDataReader["StopBit"].ToString() == "1")
                    {
                        (MBUSSerialPorts[num] as SerialPort).StopBits = StopBits.One;
                    }
                    else if (sqlDataReader["Parity"].ToString() == "2")
                    {
                        (MBUSSerialPorts[num] as SerialPort).StopBits = StopBits.Two;
                    }
                    else if (sqlDataReader["Parity"].ToString() == "1.5")
                    {
                        (MBUSSerialPorts[num] as SerialPort).StopBits = StopBits.OnePointFive;
                    }
                    (MBUSSerialPorts[num] as SerialPort).DataBits = 8;
                    (MBUSSerialPorts[num] as SerialPort).ReadTimeout = 1000;
                    (MBUSSerialPorts[num] as SerialPort).PortName = "COM" + sqlDataReader["TransmitName"].ToString().Substring(3);
                    (MBUSSerialPorts[num] as SerialPort).Encoding = Encoding.GetEncoding(1252);
                    rxBuff[num] = "";
                    Retry[num] = "";
                    if (!(MBUSSerialPorts[num] as SerialPort).IsOpen)
                    {
                        (MBUSSerialPorts[num] as SerialPort).Close();
                    }
                    (MBUSSerialPorts[num] as SerialPort).DataReceived += SerialPortDataReceivedHandler;
                    (MBUSSerialPorts[num] as SerialPort).Open();
                    (MBUSSerialPorts[num] as SerialPort).DiscardInBuffer();
                    num++;
                }
                catch (Exception ex)
                {
                    num++;
                    GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU SetSerialPort", "Error in :" + text, GlobalDeclaration.strLogPath);
                }
            }
            sqlDataReader.Close();
        }
        catch (Exception ex2)
        {
            GlobalDeclaration.StringLogger(ex2.Message.ToString(), "MBUS DCU SetSerialPort", "Error in :" + text, GlobalDeclaration.strLogPath);
            sqlDataReader.Close();
        }
    }

    private void SetTCPPort()
    {
        string text = "";
        text = "select * from SETTINGS_Transmit Where TransmitMode = 2 AND [Active]=1 AND [GatewayTypeId]=1 Order by TransmitID";
        SqlCommand sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
        SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
        try
        {
            int i = 0;
            while (sqlDataReader.Read())
            {
                try
                {
                    TCPPortName[i] = sqlDataReader["TransmitName"].ToString();
                    TCPPortNo[i] = Convert.ToInt32(sqlDataReader["PortNo"]);
                    MBUSTcpClients[i] = new TcpClient();
                    IPGlobalProperties iPGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
                    TcpConnectionInformation[] array = (from x in iPGlobalProperties.GetActiveTcpConnections()
                                                        where x.LocalEndPoint.Equals(MBUSTcpClients[i].Client.LocalEndPoint) && x.RemoteEndPoint.Equals(MBUSTcpClients[i].Client.RemoteEndPoint)
                                                        select x).ToArray();
                    if ((array != null && array.Length != 0) || !MBUSTcpClients[i].ConnectAsync(sqlDataReader["TransmitName"].ToString(), Convert.ToInt32(sqlDataReader["PortNo"])).Wait(2000))
                    {
                    }
                    MBUSTcpClients[i].Close();
                    i++;
                }
                catch (Exception ex)
                {
                    GlobalDeclaration.StringLogger(ex.Message.ToString() + " Serial Number :" + i + " Tcpname :" + TCPPortName[i], "MBUS", "SetTCPPort, Reader", GlobalDeclaration.strLogPath);
                }
            }
        }
        catch (Exception ex2)
        {
            GlobalDeclaration.StringLogger(ex2.Message.ToString(), "MBUS", "SetTCPPort", GlobalDeclaration.strLogPath);
        }
        finally
        {
            sqlDataReader.Close();
        }
    }

    private async void SharedMemoryCheck(object sender, EventArgs e)
    {
        DateTime SysTime = DateTime.Now;
        if (GlobalDeclaration._PushDatatoCloud == 1 && SysTime.Minute == 40 && (SysTime.Second == 0 || SysTime.Second == 1) && (SysTime - LastDBCheck).TotalSeconds > 2.0)
        {
            LastDBCheck = SysTime;
            StoredDataPushtoCloud();
        }
        if (GlobalDeclaration.ISRepeatPoll && (SysTime.Hour == 1 || SysTime.Hour == 5 || SysTime.Hour == 9 || SysTime.Hour == 13 || SysTime.Hour == 17 || SysTime.Hour == 21) && SysTime.Minute == 20 && (SysTime.Second == 0 || SysTime.Second == 1) && (SysTime - LastRepeatPoll).TotalSeconds > 2.0)
        {
            DeleteRepeatPolling(0);
            LastRepeatPoll = SysTime;
            Send_RepeatPoll();
        }
        string Request = DCUstorage.ReadData((DCU)0);
        if (Request != "")
        {
            string[] ReqArray = Request.Split(':');
            int MeterId = 0;
            int ValveId = 0;
            if (ReqArray[2] == "Read")
            {
                MeterId = Convert.ToInt16(ReqArray[0]);
            }
            else
            {
                MeterId = Convert.ToInt16(ReqArray[0]);
                ValveId = Convert.ToInt16(ReqArray[1]);
            }
            int TransmitId = GlobalDeclaration.MeterDetails.First((GlobalDeclaration.MDetails item) => item.MeterId == MeterId).TransmitId;
            nfi.ShowBalloonTip(200, "Polling Started", ReqArray[2], ToolTipIcon.Info);
            if (ReqArray[2] == "Read")
            {
                await Meter_Poll(MeterId, Control: true, TransmitId, "Read", 0);
            }
            else if (ReqArray[2] == "Open")
            {
                await Meter_Poll(MeterId, Control: true, TransmitId, "OPCL", ValveId);
            }
            else if (ReqArray[2] == "Close")
            {
                await Meter_Poll(MeterId, Control: false, TransmitId, "OPCL", ValveId);
            }
        }
    }

    private void ValveTriggerTime(object sender, EventArgs e)
    {
        try
        {
            DateTime now = DateTime.Now;
            DateTime dateTime = now.AddSeconds(-1.0);
            for (int i = 0; i < GlobalDeclaration.ValveDetails.Count; i++)
            {
                TimeSpan schTime = GlobalDeclaration.ValveDetails[i].SchTime;
                if (((schTime.Hours == now.Hour && schTime.Minutes == now.Minute && schTime.Seconds == now.Second) || (schTime.Hours == dateTime.Hour && schTime.Minutes == dateTime.Minute && schTime.Seconds == dateTime.Second)) && (now - ValveTime[i]).TotalSeconds > 2.0)
                {
                    ValveTime[i] = now;
                    int meterID = GlobalDeclaration.ValveDetails[i].MeterID;
                    int valveID = GlobalDeclaration.ValveDetails[i].ValveID;
                    int transmitId = GlobalDeclaration.ValveDetails[i].TransmitId;
                    if (GlobalDeclaration.ValveDetails[i].Status)
                    {
                        ValveUpdate(i);
                        Meter_Poll(meterID, Control: true, transmitId, "OPCL", valveID);
                    }
                    else
                    {
                        ValveUpdate(i);
                        Meter_Poll(meterID, Control: false, transmitId, "OPCL", valveID);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU, FrmMain", "ValveTriggerTime", GlobalDeclaration.strLogPath);
        }
    }

    private void MeterScheduleTrigger(object sender, EventArgs e)
    {
        try
        {
            DateTime now = DateTime.Now;
            DateTime dateTime = now.AddSeconds(-1.0);
            for (int i = 0; i < GlobalDeclaration.MeterSchedule.Count; i++)
            {
                TimeSpan scheduleTime = GlobalDeclaration.MeterSchedule[i].ScheduleTime;
                int dayInterval = GlobalDeclaration.MeterSchedule[i].DayInterval;
                if ((now.Day % dayInterval == 0 || now.Day == 1 || now.Day == DateTime.DaysInMonth(now.Year, now.Month)) && ((scheduleTime.Hours == now.Hour && scheduleTime.Minutes == now.Minute && scheduleTime.Seconds == now.Second) || (scheduleTime.Hours == dateTime.Hour && scheduleTime.Minutes == dateTime.Minute && scheduleTime.Seconds == dateTime.Second)) && (now - MeterSchTime[i]).TotalSeconds > 2.0)
                {
                    MeterSchTime[i] = now;
                    int TransmitId = GlobalDeclaration.MeterSchedule[i].TransmitId;
                    int meterId = GlobalDeclaration.MeterSchedule[i].MeterId;
                    int num = 0;
                    int num2 = 0;
                    string text = "";
                    num = GlobalDeclaration.TransDetails.FindIndex((GlobalDeclaration.TransmitDetails a) => a.TransmitId == TransmitId);
                    num2 = GlobalDeclaration.TransDetails[num].TypeId;
                    text = GlobalDeclaration.TransDetails[num].TransmitMode;
                    if (text == "Serial")
                    {
                        Single_Serial(meterId, Control: true, num2, "Read", num, 0);
                    }
                    else if (text == "TCP")
                    {
                        Single_TCP(meterId, Control: true, num2, "Read", num, 0);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU, FrmMain", "MeterScheduleTrigger", GlobalDeclaration.strLogPath);
        }
    }

    private async Task Meter_Poll(int MeterId, bool Control, int TransmitId, string ReadType, int ValveId)
    {
        string TMode = "";
        int TypeId = 0;
        int Id = 0;
        for (int i = 0; i < GlobalDeclaration.TransDetails.Count; i++)
        {
            if (TransmitId == GlobalDeclaration.TransDetails[i].TransmitId)
            {
                TMode = GlobalDeclaration.TransDetails[i].TransmitMode;
                TypeId = GlobalDeclaration.TransDetails[i].TypeId;
                Id = i;
                break;
            }
        }
        if (TMode == "Serial")
        {
            await Single_Serial(MeterId, Control, TypeId, ReadType, Id, ValveId);
        }
        else if (TMode == "TCP")
        {
            await Single_TCP(MeterId, Control, TypeId, ReadType, Id, ValveId);
        }
    }

    private async Task Send_RepeatPoll()
    {
        SqlDataAdapter sqladapter = new SqlDataAdapter();
        DataTable dt = new DataTable();
        try
        {
            string TMode = "";
            int TypeId = 0;
            int Id = 0;
            string Query = "Select Distinct [MeterID],[TransmitID] From [MeterRepeatPolling]";
            SqlCommand GetCommand = new SqlCommand(Query, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            sqladapter.SelectCommand = GetCommand;
            sqladapter.Fill(dt);
            for (int j = 0; j < dt.Rows.Count; j++)
            {
                short MeterId = Convert.ToInt16(dt.Rows[j]["MeterID"].ToString());
                short TransmitId = Convert.ToInt16(dt.Rows[j]["TransmitID"].ToString());
                for (int i = 0; i < GlobalDeclaration.TransDetails.Count; i++)
                {
                    if (TransmitId == GlobalDeclaration.TransDetails[i].TransmitId)
                    {
                        TMode = GlobalDeclaration.TransDetails[i].TransmitMode;
                        TypeId = GlobalDeclaration.TransDetails[i].TypeId;
                        Id = i;
                        break;
                    }
                }
                if (TMode == "Serial")
                {
                    await Single_Serial(MeterId, Control: true, TypeId, "Read", Id, 0);
                }
                else if (TMode == "TCP")
                {
                    await Single_TCP(MeterId, Control: true, TypeId, "Read", Id, 0);
                }
            }
        }
        catch (Exception ex2)
        {
            Exception ex = ex2;
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU FrmMain", "Send_RepeatPoll", GlobalDeclaration.strLogPath);
        }
    }

    private async Task Single_Serial(int MeterId, bool Control, int TypeId, string ReadType, int Id, int ValveId)
    {
        string TrName = GlobalDeclaration.TransDetails[Id].TransmitName;
        string DeviceId = GlobalDeclaration.TransDetails[Id].DeviceId;
        while (Serial_Status[TypeId])
        {
            await Task.Delay(2000);
        }
        DateTime LogTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
        if (GlobalDeclaration.ErrorTest)
        {
            GlobalDeclaration.StringLogger(string.Concat("TransmitName :", TrName, " Logtime :", LogTime, " MeterID :", MeterId, " ValveID :", ValveId, " ReadType :", ReadType), "MBUS TEST", "Single_Serial", GlobalDeclaration.strLogPath);
        }
        try
        {
            Serial_Status[TypeId] = true;
            if (!(MBUSSerialPorts[TypeId] as SerialPort).IsOpen)
            {
                (MBUSSerialPorts[TypeId] as SerialPort).Open();
            }
            (MBUSSerialPorts[TypeId] as SerialPort).DiscardInBuffer();
            int index = GlobalDeclaration.MeterDetails.FindIndex((GlobalDeclaration.MDetails a) => a.MeterId == MeterId);
            if (GlobalDeclaration._PollingID == 0)
            {
                int ModelId = Convert.ToInt16(GlobalDeclaration.MeterDetails[index].MModelId);
                Convert.ToInt16(GlobalDeclaration.MeterDetails[index].MeterType);
                bool ValControl = false;
                int RTry = GlobalDeclaration._NoofRetries;
                while (true)
                {
                    RTry--;
                    if (ReadType == "OPCL")
                    {
                        int Vindex = GlobalDeclaration.MeterDetails.FindIndex((GlobalDeclaration.MDetails a) => a.MeterId == ValveId);
                        int VModelId = Convert.ToInt16(GlobalDeclaration.MeterDetails[Vindex].MModelId);
                        byte[] data4 = Telegram_Convert(GlobalDeclaration.MeterDetails[Vindex].MBUSID, TypeId, GlobalDeclaration.Telegram[VModelId, 1], "Serial", Control);
                        (MBUSSerialPorts[TypeId] as SerialPort).Write(data4, 0, data4.Length);
                        await Task.Delay(500);
                        byte[] data5 = Telegram_Convert(GlobalDeclaration.MeterDetails[Vindex].MBUSID, TypeId, GlobalDeclaration.Telegram[VModelId, 2], "Serial", Control);
                        (MBUSSerialPorts[TypeId] as SerialPort).Write(data5, 0, data5.Length);
                        await Task.Delay(1000);
                        if (rxBuff[TypeId].Trim() == "E5")
                        {
                            ValControl = true;
                            string Status = "Valve Closed Successfully";
                            if (Control)
                            {
                                Status = "Valve Opened Successfully";
                            }
                            DCUstorage.WriteData((DCU)1, Status);
                            SaveValveStatus(ValveId, Control, "OPCL", LogTime);
                        }
                        else
                        {
                            if (RTry >= 1)
                            {
                                continue;
                            }
                            string Status2 = "Valve Close Failed";
                            if (Control)
                            {
                                Status2 = "Valve Open Failed";
                            }
                            DCUstorage.WriteData((DCU)1, Status2);
                        }
                    }
                    if (!(ReadType != "OPCL" || ValControl))
                    {
                        break;
                    }
                    byte[] data = Telegram_Convert(GlobalDeclaration.MeterDetails[index].MBUSID, TypeId, GlobalDeclaration.Telegram[ModelId, 1], "Serial", Control);
                    (MBUSSerialPorts[TypeId] as SerialPort).Write(data, 0, data.Length);
                    await Task.Delay(1000);
                    if (rxBuff[TypeId].Trim() == "E5")
                    {
                        nfi.Icon = Resources.Green;
                        byte[] data3 = Telegram_Convert(GlobalDeclaration.MeterDetails[index].MBUSID, TypeId, GlobalDeclaration.Telegram[ModelId, 2], "Serial", Control);
                        (MBUSSerialPorts[TypeId] as SerialPort).Write(data3, 0, data3.Length);
                        await Task.Delay(2000);
                        GlobalDeclaration.StringLogger("PArserMbus Called at 669", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                        ParseMBUSData(rxBuff[TypeId], GlobalDeclaration.MeterDetails[index].MModelId, MeterId, GlobalDeclaration.MeterDetails[index].TagsCount, LogTime, DeviceId);
                        if (ReadType != "OPCL")
                        {
                            DCUstorage.WriteData((DCU)1, "Meter Read Successully");
                        }
                        break;
                    }
                    if (RTry < 1)
                    {
                        nfi.Icon = Resources.Red;
                        if (ReadType != "OPCL")
                        {
                            SaveData(MeterId, 0, LogTime);
                            DCUstorage.WriteData((DCU)1, "Meter Read Failed");
                        }
                        break;
                    }
                }
            }
            else if (GlobalDeclaration._PollingID == 1)
            {
                byte[] data2 = SerialTelegramPrimaryID(GlobalDeclaration.MeterDetails[index].MBUSID, TypeId, "Serial");
                (MBUSSerialPorts[TypeId] as SerialPort).Write(data2, 0, data2.Length);
                await Task.Delay(2000);
                GlobalDeclaration.StringLogger("PArserMbus Called at 694", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                ParseMBUSData(rxBuff[TypeId], GlobalDeclaration.MeterDetails[index].MModelId, MeterId, GlobalDeclaration.MeterDetails[index].TagsCount, LogTime, DeviceId);
            }
        }
        catch (Exception ex2)
        {
            Exception ex = ex2;
            SaveData(MeterId, 0, LogTime);
            GlobalDeclaration.StringLogger(ex.Message.ToString() + ": " + ex.StackTrace.ToString() + " in Serial Port : " + TrName, "MBUS", "Single_Serial", GlobalDeclaration.strLogPath);
            if (ex.Message.Contains("does not exist"))
            {
                nfi.ShowBalloonTip(1000, "SERIAL PORT DISCONNECTED / MALFUNCTIONING..", "ERROR", ToolTipIcon.Error);
            }
        }
        finally
        {
            Serial_Status[TypeId] = false;
        }
    }

    private async Task Single_TCP(int MeterId, bool Control, int TypeId, string ReadType, int Id, int ValveId)
    {
        while (TCP_Status[TypeId])
        {
            await Task.Delay(1000);
        }
        string TrName = GlobalDeclaration.TransDetails[Id].TransmitName;
        string DeviceId = GlobalDeclaration.TransDetails[Id].DeviceId;
        try
        {
            DateTime Logtime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
            TCP_Status[TypeId] = true;
            if (GlobalDeclaration.ErrorTest)
            {
                GlobalDeclaration.StringLogger(string.Concat("TransmitName :", TrName, " Logtime :", Logtime, " MeterID :", MeterId, " ValveID :", ValveId, " ReadType :", ReadType), "MBUS TEST", "Single_TCP", GlobalDeclaration.strLogPath);
            }
            MBUSTcpClients[TypeId] = new TcpClient();
            IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            TcpConnectionInformation[] tcpConnections = (from x in ipProperties.GetActiveTcpConnections()
                                                         where x.LocalEndPoint.Equals(MBUSTcpClients[TypeId].Client.LocalEndPoint) && x.RemoteEndPoint.Equals(MBUSTcpClients[TypeId].Client.RemoteEndPoint)
                                                         select x).ToArray();
            if ((tcpConnections == null || tcpConnections.Length == 0) && !MBUSTcpClients[TypeId].ConnectAsync(TCPPortName[TypeId], TCPPortNo[TypeId]).Wait(TimeSpan.FromSeconds(2.0)))
            {
                GlobalDeclaration.StringLogger("TCP Connection Error in TCP Port : " + TrName, "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                nfi.Icon = Resources.Red;
                SaveData(MeterId, 0, Logtime);
                return;
            }
            int index = GlobalDeclaration.MeterDetails.FindIndex((GlobalDeclaration.MDetails a) => a.MeterId == MeterId);
            Stream stm = MBUSTcpClients[TypeId].GetStream();
            stm.ReadTimeout = 2000;
            if (GlobalDeclaration._PollingID == 0)
            {
                bool ValControl = false;
                int ModelId = Convert.ToInt16(GlobalDeclaration.MeterDetails[index].MModelId);
                int MeterType = Convert.ToInt16(GlobalDeclaration.MeterDetails[index].MeterType);
                _ = new byte[255];
                byte[] ReadDataByte = new byte[255];
                int RTry = GlobalDeclaration._NoofRetries;
                while (true)
                {
                    RTry--;
                    if (GlobalDeclaration.ErrorTest)
                    {
                        GlobalDeclaration.StringLogger("Test : MeterID :" + MeterId + " TypeId :" + TypeId + " ModelID :" + ModelId + " Type " + MeterType + " ReadType :" + ReadType, "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                    }
                    try
                    {
                        if (!(ReadType == "OPCL"))
                        {
                            goto IL_0945;
                        }
                        int Vindex = GlobalDeclaration.MeterDetails.FindIndex((GlobalDeclaration.MDetails a) => a.MeterId == ValveId);
                        int VModelId = Convert.ToInt16(GlobalDeclaration.MeterDetails[Vindex].MModelId);
                        if (GlobalDeclaration.ErrorTest)
                        {
                            GlobalDeclaration.StringLogger("MeterID :" + MeterId + " ,MIndex :" + index + " ,VIndex :" + Vindex + " ,VH :" + GlobalDeclaration.MeterDetails[Vindex].MBUSID + ", MH :" + GlobalDeclaration.MeterDetails[index].MBUSID, "MBUS", "TEST", GlobalDeclaration.strLogPath);
                        }
                        byte[] Writedatabyte4 = Telegram_Convert(GlobalDeclaration.MeterDetails[Vindex].MBUSID, TypeId, GlobalDeclaration.Telegram[VModelId, 1], "TCP", Control);
                        stm.Write(Writedatabyte4, 0, Writedatabyte4.Length);
                        await Task.Delay(500);
                        Writedatabyte4 = Telegram_Convert(GlobalDeclaration.MeterDetails[Vindex].MBUSID, TypeId, GlobalDeclaration.Telegram[VModelId, 2], "TCP", Control);
                        stm.Write(Writedatabyte4, 0, Writedatabyte4.Length);
                        await Task.Delay(1000);
                        stm.Read(ReadDataByte, 0, 255);
                        tcprxBuff[TypeId] = ByteArrayToHexString(ReadDataByte);
                        if (tcprxBuff[TypeId].Substring(0, 2) == "E5")
                        {
                            ValControl = true;
                            string Status = "Valve Closed Successfully";
                            if (Control)
                            {
                                Status = "Valve Opened Successfully";
                            }
                            DCUstorage.WriteData((DCU)1, Status);
                            SaveValveStatus(ValveId, Control, "OPCL", Logtime);
                            goto IL_0945;
                        }
                        if (RTry < 1)
                        {
                            string Status2 = "Valve Close Failed";
                            if (Control)
                            {
                                Status2 = "Valve Open Failed";
                            }
                            DCUstorage.WriteData((DCU)1, Status2);
                            goto IL_0945;
                        }
                        goto end_IL_0558;
                    IL_0945:
                        if (!(ReadType != "OPCL" || ValControl))
                        {
                            break;
                        }
                        Writedatabyte4 = Telegram_Convert(GlobalDeclaration.MeterDetails[index].MBUSID, TypeId, GlobalDeclaration.Telegram[ModelId, 1], "TCP", Control);
                        stm.Write(Writedatabyte4, 0, Writedatabyte4.Length);
                        await Task.Delay(3000);
                        stm.Read(ReadDataByte, 0, 255);
                        nfi.Icon = Resources.Green;
                        tcprxBuff[TypeId] = ByteArrayToHexString(ReadDataByte);
                        if (tcprxBuff[TypeId].Substring(0, 2) == "E5")
                        {
                            if (GlobalDeclaration.ErrorTest)
                            {
                                GlobalDeclaration.StringLogger("Test E5 : MeterID :" + MeterId + " TypeId :" + TypeId + " ModelID :" + ModelId + " Type " + MeterType + " ReadType :" + ReadType, "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                            }
                            Writedatabyte4 = Telegram_Convert(GlobalDeclaration.MeterDetails[index].MBUSID, TypeId, GlobalDeclaration.Telegram[ModelId, 2], "TCP", Control);
                            stm.Write(Writedatabyte4, 0, Writedatabyte4.Length);
                            await Task.Delay(2000);
                            stm.Read(ReadDataByte, 0, 255);
                            GlobalDeclaration.StringLogger("PArserMbus Called at 824", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                            ParseMBUSData(ByteArrayToHexString(ReadDataByte), GlobalDeclaration.MeterDetails[index].MModelId, MeterId, GlobalDeclaration.MeterDetails[index].TagsCount, Logtime, DeviceId);
                            if (GlobalDeclaration.ISRepeatPoll)
                            {
                                DeleteRepeatPolling(MeterId);
                            }
                            if (ReadType != "OPCL")
                            {
                                DCUstorage.WriteData((DCU)1, "Meter Read Successully");
                            }
                            break;
                        }
                        if (RTry >= 1)
                        {
                            continue;
                        }
                        nfi.Icon = Resources.Red;
                        if (ReadType != "OPCL")
                        {
                            SaveData(MeterId, 0, Logtime);
                            DCUstorage.WriteData((DCU)1, "Meter Read Failed");
                        }
                        break;
                    end_IL_0558:;
                    }
                    catch
                    {
                        nfi.Icon = Resources.Red;
                        if (ReadType != "OPCL")
                        {
                            SaveData(MeterId, 0, Logtime);
                            DCUstorage.WriteData((DCU)1, "Meter Read Failed");
                        }
                        break;
                    }
                }
            }
            else if (GlobalDeclaration._PollingID == 1)
            {
                _ = new byte[255];
                byte[] ReadDataByte2 = new byte[255];
                byte[] Writedatabyte5 = SerialTelegramPrimaryID(GlobalDeclaration.MeterDetails[index].MBUSID, TypeId, "TCP");
                stm.Write(Writedatabyte5, 0, Writedatabyte5.Length);
                await Task.Delay(2000);
                stm.Read(ReadDataByte2, 0, 255);
                GlobalDeclaration.StringLogger("PArserMbus Called at 868", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                ParseMBUSData(ByteArrayToHexString(ReadDataByte2), GlobalDeclaration.MeterDetails[index].MModelId, MeterId, GlobalDeclaration.MeterDetails[index].TagsCount, Logtime, DeviceId);
            }
        }
        catch (Exception ex2)
        {
            Exception ex = ex2;
            GlobalDeclaration.StringLogger(ex.Message.ToString() + " in TCP Port : " + TrName, "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
            if (ex.Message.Contains("does not exist"))
            {
                nfi.ShowBalloonTip(1000, "TCP PORT DISCONNECTED / MALFUNCTIONING..", "ERROR", ToolTipIcon.Error);
            }
        }
        finally
        {
            TCP_Status[TypeId] = false;
            MBUSTcpClients[TypeId].Close();
        }
    }

    private void ValveUpdate(int Id)
    {
        try
        {
            int scheduleID = GlobalDeclaration.ValveDetails[Id].ScheduleID;
            string text = "";
            DateTime now = DateTime.Now;
            text = string.Concat("Update [Schedule_Valve] set [LastUpdated]='", now, "' where [ScheduleID]=", scheduleID) ?? "";
            SqlCommand sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            int num = sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "ValveUpdate", "ValveUpdate", GlobalDeclaration.strLogPath);
        }
    }

    private void FindTriggerTime(object sender, EventArgs e)
    {
        try
        {
            DateTime now = DateTime.Now;
            for (int i = 0; i < GlobalDeclaration.TransDetails.Count; i++)
            {
                if (GlobalDeclaration.TransDetails[i].ScheduledPoll)
                {
                    continue;
                }
                DateTime dateTime = LastPoll[i];
                if (GlobalDeclaration.TransDetails[i].PollTime == 1)
                {
                    if ((now.Second == 0 || (now - dateTime).TotalSeconds >= 60.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 5)
                {
                    if (((DateTime.Now.Minute % GlobalDeclaration.TransDetails[i].PollTime == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 5.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 15)
                {
                    if (((DateTime.Now.Minute % GlobalDeclaration.TransDetails[i].PollTime == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 15.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 60)
                {
                    if (((now.Minute == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 60.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 100)
                {
                    if ((((now.Hour == 0 || now.Hour == 2 || now.Hour == 4 || now.Hour == 6 || now.Hour == 8 || now.Hour == 10 || now.Hour == 12 || now.Hour == 14 || now.Hour == 16 || now.Hour == 18 || now.Hour == 20 || now.Hour == 22 || now.Hour == 24) && now.Minute == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 120.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 101)
                {
                    if ((((now.Hour == 0 || now.Hour == 5 || now.Hour == 10 || now.Hour == 15 || now.Hour == 20) && now.Minute == 0 && now.Second == 0) || (dateTime.Hour >= 20 && (now - dateTime).TotalMinutes >= 240.0) || (dateTime.Hour < 20 && (now - dateTime).TotalMinutes >= 300.0)) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 102)
                {
                    if ((((now.Hour == 0 || now.Hour == 6 || now.Hour == 12 || now.Hour == 18) && now.Minute == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 360.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 103)
                {
                    if ((((now.Hour == 0 || now.Hour == 8 || now.Hour == 16) && now.Minute == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 480.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 104)
                {
                    if ((((now.Hour == 0 || now.Hour == 12) && now.Minute == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 720.0) && CheckHistory(i))
                    {
                        LastPoll[i] = now;
                        Send_Command(i);
                    }
                }
                else if (GlobalDeclaration.TransDetails[i].PollTime == 105 && ((now.Hour == 0 && now.Minute == 0 && now.Second == 0) || (now - dateTime).TotalMinutes >= 1440.0) && CheckHistory(i))
                {
                    LastPoll[i] = now;
                    Send_Command(i);
                }
            }
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU", "FindTriggerTime", GlobalDeclaration.strLogPath);
        }
        finally
        {
            nfi.Icon = Resources.Comm;
        }
    }

    private void ScheduleTriggerTime(object sender, EventArgs e)
    {
        try
        {
            DateTime now = DateTime.Now;
            DateTime dateTime = now.AddSeconds(-1.0);
            for (int i = 0; i < GlobalDeclaration.TransmitSchedule.Count; i++)
            {
                TimeSpan transSch = GlobalDeclaration.TransmitSchedule[i].TransSch;
                int dayInterval = GlobalDeclaration.TransmitSchedule[i].DayInterval;
                if ((now.Day % dayInterval != 0 && now.Day != 1 && (dayInterval != 30 || now.Day != DateTime.DaysInMonth(now.Year, now.Month))) || ((transSch.Hours != now.Hour || transSch.Minutes != now.Minute || transSch.Seconds != now.Second) && (transSch.Hours != dateTime.Hour || transSch.Minutes != dateTime.Minute || transSch.Seconds != dateTime.Second)) || !((now - ScheduleTime[i]).TotalSeconds > 2.0))
                {
                    continue;
                }
                ScheduleTime[i] = now;
                int transmitId = GlobalDeclaration.TransmitSchedule[i].TransmitId;
                int id = 0;
                for (int j = 0; j < GlobalDeclaration.TransDetails.Count; j++)
                {
                    if (transmitId == GlobalDeclaration.TransDetails[j].TransmitId)
                    {
                        id = j;
                        break;
                    }
                }
                Send_Command(id);
            }
            for (int k = 0; k < GlobalDeclaration.ImportSchedule.Count; k++)
            {
                TimeSpan schTime = GlobalDeclaration.ImportSchedule[k].SchTime;
                if (((schTime.Hours == now.Hour && schTime.Minutes == now.Minute && schTime.Seconds == now.Second) || (schTime.Hours == dateTime.Hour && schTime.Minutes == dateTime.Minute && schTime.Seconds == dateTime.Second)) && (now - ImportTime[k]).TotalSeconds > 2.0)
                {
                    ImportTime[k] = now;
                    ReadTemplate(GlobalDeclaration.ImportSchedule[k].TemplateId);
                }
            }
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU, FrmMain", "ScheduleTriggerTime", GlobalDeclaration.strLogPath);
        }
    }

    private async void Send_Command(int Id)
    {
        SaveHistory(Id);
        if (GlobalDeclaration.TransDetails[Id].GatewayType == "MBUS")
        {
            if (GlobalDeclaration.TransDetails[Id].TransmitMode == "Serial")
            {
                SendSerialCommand(Id);
            }
            else if (GlobalDeclaration.TransDetails[Id].TransmitMode == "TCP")
            {
                SendTCPCommand(Id);
            }
        }
        else if (GlobalDeclaration.TransDetails[Id].GatewayType == "MODBUS")
        {
            nfi.Icon = Resources.Green;
            if (GlobalDeclaration.TransDetails[Id].TransmitMode == "Serial")
            {
                ObjBMS.ModBus_Serial(Id);
            }
            else if (GlobalDeclaration.TransDetails[Id].TransmitMode == "TCP")
            {
                ObjBMS.ModBus_TCP(Id);
            }
        }
        else if (GlobalDeclaration.TransDetails[Id].GatewayType == "BMS MODBUS")
        {
            nfi.Icon = Resources.Green;
            ObjBMS.BMSMODBUS_Poll(Id);
        }
    }

    private bool CheckHistory(int Id)
    {
        bool result = true;
        SqlDataReader sqlDataReader = null;
        try
        {
            if (GlobalDeclaration._ASIMBUSSETTINGSCnx.State == ConnectionState.Closed)
            {
                GlobalDeclaration._ASIMBUSSETTINGSCnx.Open();
            }
            int transmitId = GlobalDeclaration.TransDetails[Id].TransmitId;
            string text = "";
            text = "Select [Status] FROM [TransmitHistory] where TransmitID=" + transmitId + " and Logtime=(select Max(logtime) FROM [TransmitHistory] where TransmitID=" + transmitId + ")";
            SqlCommand sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            sqlDataReader = sqlCommand.ExecuteReader();
            if (sqlDataReader.Read())
            {
                int num = Convert.ToInt16(sqlDataReader["Status"].ToString());
                if (num == 1)
                {
                    result = false;
                }
            }
        }
        catch (Exception ex)
        {
            result = false;
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU", "CheckHistory", GlobalDeclaration.strLogPath);
        }
        finally
        {
            sqlDataReader.Close();
        }
        return result;
    }

    private void SaveHistory(int Id)
    {
        string text = "";
        try
        {
            string text2 = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt");
            text = "Insert into [TransmitHistory]([TransmitID],[Logtime],[Status]) Values(" + GlobalDeclaration.TransDetails[Id].TransmitId + ",'" + text2 + "',1)";
            SqlCommand sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            int num = sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU frmMain", "Save History", GlobalDeclaration.strLogPath);
        }
    }

    private void SaveError(int MeterId, string ErrorMsg, DateTime ErrTime)
    {
        try
        {
            string text = "";
            string text2 = ErrTime.ToString("yyyy-MM-dd hh:mm:ss tt");
            text = "  Insert Into [dbo].[DCU_ERROR]([MeterId],[ErrMessage],[CustomerID],[ErrorDate]) Values(" + MeterId + ",'" + ErrorMsg + "'," + GlobalDeclaration.CustomerId + ",'" + text2 + "')";
            SqlCommand sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            int num = sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU frmMain", "SaveError", GlobalDeclaration.strLogPath);
        }
    }

    private async Task SendSerialCommand(int Id)
    {
        int SerialPortID = GlobalDeclaration.TransDetails[Id].TypeId;
        int TransId = GlobalDeclaration.TransDetails[Id].TransmitId;
        string TrName = GlobalDeclaration.TransDetails[Id].TransmitName;
        string DeviceId = GlobalDeclaration.TransDetails[Id].DeviceId;
        bool GatTelegram = GlobalDeclaration.TransDetails[Id].GatewayTelegram;
        DateTime Logtime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
        int MeterId = 0;
        try
        {
            if (GlobalDeclaration.ErrorTest)
            {
                GlobalDeclaration.StringLogger("TransmitName :" + TrName + " Logtime :" + Logtime, "MBUS TEST", "SendSerialCommand", GlobalDeclaration.strLogPath);
            }
            while (Serial_Status[SerialPortID])
            {
                await Task.Delay(1000);
            }
            Serial_Status[SerialPortID] = true;
            if (!(MBUSSerialPorts[SerialPortID] as SerialPort).IsOpen)
            {
                (MBUSSerialPorts[SerialPortID] as SerialPort).Open();
            }
            (MBUSSerialPorts[SerialPortID] as SerialPort).DiscardInBuffer();
            for (int j = 0; j < GlobalDeclaration.TotalMeterCount; j++)
            {
                if (GlobalDeclaration.MeterDetails[j].TransmitId != TransId)
                {
                    continue;
                }
                int ModelId = GlobalDeclaration.MeterDetails[j].MModelId;
                int Type = GlobalDeclaration.MeterDetails[j].MeterType;
                int ValveId = GlobalDeclaration.MeterDetails[j].ValveId;
                MeterId = GlobalDeclaration.MeterDetails[j].MeterId;
                int RTry = GlobalDeclaration._NoofRetries;
                while (true)
                {
                    RTry--;
                    if (GlobalDeclaration.ErrorTest)
                    {
                        GlobalDeclaration.StringLogger("TransmitName :" + TrName + " ,ModelId :" + ModelId + ", MeterID :" + MeterId + ", MeterType :" + Type, "MBUS TEST", "SendSerialCommand", GlobalDeclaration.strLogPath);
                    }
                    if (Type == 23)
                    {
                        break;
                    }
                    if (GlobalDeclaration._PollingID == 0)
                    {
                        if (GatTelegram)
                        {
                            for (int z = 0; z < GlobalDeclaration.GatewayTelegram.Count; z++)
                            {
                                if (TransId == GlobalDeclaration.GatewayTelegram[z].TransmitId)
                                {
                                    byte[] Request = HexStringToByteArray(GlobalDeclaration.GatewayTelegram[z].Telegram);
                                    (MBUSSerialPorts[SerialPortID] as SerialPort).Write(Request, 0, Request.Length);
                                    await Task.Delay(1000);
                                }
                            }
                        }
                        byte[] data = Telegram_Convert(GlobalDeclaration.MeterDetails[j].MBUSID, SerialPortID, GlobalDeclaration.Telegram[ModelId, 1], "Serial", Control: true);
                        (MBUSSerialPorts[SerialPortID] as SerialPort).Write(data, 0, data.Length);
                        await Task.Delay(1000);
                        if (GlobalDeclaration.ErrorTest)
                        {
                            GlobalDeclaration.StringLogger("TransmitName :" + TrName + " ,MBUSID :" + GlobalDeclaration.MeterDetails[j].MBUSID, "MBUS TEST After First Telegram", "SendSerialCommand", GlobalDeclaration.strLogPath);
                        }
                        if (rxBuff[SerialPortID].Trim() == "E5")
                        {
                            nfi.Icon = Resources.Green;
                            byte[] data2 = Telegram_Convert(GlobalDeclaration.MeterDetails[j].MBUSID, SerialPortID, GlobalDeclaration.Telegram[ModelId, 2], "Serial", Control: true);
                            (MBUSSerialPorts[SerialPortID] as SerialPort).Write(data2, 0, data2.Length);
                            await Task.Delay(2000);
                            if (rxBuff[SerialPortID].Length > 4)
                            {
                                GlobalDeclaration.StringLogger("PArserMbus Called at 1226", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                                ParseMBUSData(rxBuff[SerialPortID], GlobalDeclaration.MeterDetails[j].MModelId, GlobalDeclaration.MeterDetails[j].MeterId, GlobalDeclaration.MeterDetails[j].TagsCount, Logtime, DeviceId);
                            }
                            else
                            {
                                if (RTry != 0)
                                {
                                    continue;
                                }
                                nfi.Icon = Resources.Red;
                                SaveData(GlobalDeclaration.MeterDetails[j].MeterId, 0, Logtime);
                            }
                        }
                        else if (rxBuff[SerialPortID].Trim() != "E5")
                        {
                            if (RTry != 0)
                            {
                                continue;
                            }
                            nfi.Icon = Resources.Red;
                            SaveData(GlobalDeclaration.MeterDetails[j].MeterId, 0, Logtime);
                        }
                    }
                    else if (GlobalDeclaration._PollingID == 1)
                    {
                        byte[] data3 = SerialTelegramPrimaryID(GlobalDeclaration.MeterDetails[j].MBUSID, SerialPortID, "Serial");
                        (MBUSSerialPorts[SerialPortID] as SerialPort).Write(data3, 0, data3.Length);
                        await Task.Delay(2000);
                        GlobalDeclaration.StringLogger("PArserMbus Called at 1255", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                        ParseMBUSData(rxBuff[SerialPortID], GlobalDeclaration.MeterDetails[j].MModelId, GlobalDeclaration.MeterDetails[j].MeterId, GlobalDeclaration.MeterDetails[j].TagsCount, Logtime, DeviceId);
                    }
                    if (ValveId != 0)
                    {
                        SaveValveStatus(GlobalDeclaration.MeterDetails[j].ValveId, Status: true, "Read", Logtime);
                    }
                    break;
                }
            }
        }
        catch (Exception ex2)
        {
            Exception ex = ex2;
            GlobalDeclaration.StringLogger(ex.Message.ToString() + " in Serial Port : " + TrName, "MBUS", "SendSerialCommand", GlobalDeclaration.strLogPath);
            if (ex.Message.Contains("does not exist"))
            {
                for (int i = 0; i < GlobalDeclaration.TotalMeterCount; i++)
                {
                    if (GlobalDeclaration.MeterDetails[i].TransmitId == TransId)
                    {
                        SaveData(GlobalDeclaration.MeterDetails[i].MeterId, 0, Logtime);
                        nfi.Icon = Resources.Red;
                    }
                }
                nfi.ShowBalloonTip(1000, "SERIAL PORT DISCONNECTED / MALFUNCTIONING..", "ERROR", ToolTipIcon.Error);
            }
            else
            {
                SaveData(MeterId, 0, Logtime);
                nfi.Icon = Resources.Red;
            }
        }
        finally
        {
            Serial_Status[SerialPortID] = false;
            GlobalDeclaration.UpdateHistory(Id);
        }
    }

    private async Task SendTCPCommand(int Id)
    {
        int _tcpPortID = GlobalDeclaration.TransDetails[Id].TypeId;
        int TransId = GlobalDeclaration.TransDetails[Id].TransmitId;
        string TrName = GlobalDeclaration.TransDetails[Id].TransmitName;
        bool GatTelegram = GlobalDeclaration.TransDetails[Id].GatewayTelegram;
        string DeviceId = GlobalDeclaration.TransDetails[Id].DeviceId;
        try
        {
            DateTime LogTime = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, 0);
            if (GlobalDeclaration.ErrorTest)
            {
                GlobalDeclaration.StringLogger("TransmitName :" + TrName + " Logtime :" + LogTime, "MBUS TEST", "SendTCPCommand", GlobalDeclaration.strLogPath);
            }
            while (TCP_Status[_tcpPortID])
            {
                await Task.Delay(1000);
            }
            TCP_Status[_tcpPortID] = true;
            MBUSTcpClients[_tcpPortID] = new TcpClient();
            IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
            TcpConnectionInformation[] tcpConnections = (from x in ipProperties.GetActiveTcpConnections()
                                                         where x.LocalEndPoint.Equals(MBUSTcpClients[_tcpPortID].Client.LocalEndPoint) && x.RemoteEndPoint.Equals(MBUSTcpClients[_tcpPortID].Client.RemoteEndPoint)
                                                         select x).ToArray();
            if ((tcpConnections == null || tcpConnections.Length == 0) && !MBUSTcpClients[_tcpPortID].ConnectAsync(TCPPortName[_tcpPortID], TCPPortNo[_tcpPortID]).Wait(TimeSpan.FromSeconds(2.0)))
            {
                GlobalDeclaration.StringLogger("TCP Connection Error in TCP Port : " + TrName, "MBUS", "SendTCPCommand Connection", GlobalDeclaration.strLogPath);
                for (int j = 0; j < GlobalDeclaration.TotalMeterCount; j++)
                {
                    if (GlobalDeclaration.MeterDetails[j].TransmitId == TransId)
                    {
                        nfi.Icon = Resources.Red;
                        SaveData(GlobalDeclaration.MeterDetails[j].MeterId, 0, LogTime);
                        if (GlobalDeclaration.ISRepeatPoll)
                        {
                            SaveRepeatPolling(GlobalDeclaration.MeterDetails[j].MeterId, LogTime, TransId);
                        }
                    }
                }
                return;
            }
            Stream stm = MBUSTcpClients[_tcpPortID].GetStream();
            stm.ReadTimeout = 4000;
            for (int i = 0; i < GlobalDeclaration.TotalMeterCount; i++)
            {
                if (GlobalDeclaration.MeterDetails[i].TransmitId != TransId)
                {
                    continue;
                }
                int ModelId = GlobalDeclaration.MeterDetails[i].MModelId;
                int Type = GlobalDeclaration.MeterDetails[i].MeterType;
                int ValveId = GlobalDeclaration.MeterDetails[i].ValveId;
                int MeterId = GlobalDeclaration.MeterDetails[i].MeterId;
                int RTry = GlobalDeclaration._NoofRetries;
                while (true)
                {
                    RTry--;
                    if (GlobalDeclaration.ErrorTest)
                    {
                        GlobalDeclaration.StringLogger("TransmitName :" + TrName + " ,ModelId :" + ModelId + ", MeterID :" + MeterId + ", MeterType :" + Type, "MBUS TEST", "SendTCPCommand", GlobalDeclaration.strLogPath);
                    }
                    if (Type == 23)
                    {
                        break;
                    }
                    if (GlobalDeclaration._PollingID == 0)
                    {
                        try
                        {
                            _ = new byte[255];
                            byte[] ReadDataByte = new byte[255];
                            byte[] Writedatabyte3;
                            if (GatTelegram)
                            {
                                for (int z = 0; z < GlobalDeclaration.GatewayTelegram.Count; z++)
                                {
                                    if (TransId == GlobalDeclaration.GatewayTelegram[z].TransmitId)
                                    {
                                        Writedatabyte3 = HexStringToByteArray(GlobalDeclaration.GatewayTelegram[z].Telegram);
                                        stm.Write(Writedatabyte3, 0, Writedatabyte3.Length);
                                        await Task.Delay(3000);
                                    }
                                }
                            }
                            Writedatabyte3 = Telegram_Convert(GlobalDeclaration.MeterDetails[i].MBUSID, _tcpPortID, GlobalDeclaration.Telegram[ModelId, 1], "TCP", Control: true);
                            stm.Write(Writedatabyte3, 0, Writedatabyte3.Length);
                            await Task.Delay(4000);
                            stm.Read(ReadDataByte, 0, 255);
                            nfi.Icon = Resources.Green;
                            tcprxBuff[_tcpPortID] = ByteArrayToHexString(ReadDataByte);
                            if (GlobalDeclaration.ErrorTest)
                            {
                                GlobalDeclaration.StringLogger("TransmitName :" + TrName + ", MeterID :" + MeterId + " ,MBUSID :" + GlobalDeclaration.MeterDetails[i].MBUSID, "MBUS TEST After First Telegram", "SendTCPCommand", GlobalDeclaration.strLogPath);
                            }
                            if (tcprxBuff[_tcpPortID].Substring(0, 2) == "E5")
                            {
                                Writedatabyte3 = Telegram_Convert(GlobalDeclaration.MeterDetails[i].MBUSID, _tcpPortID, GlobalDeclaration.Telegram[ModelId, 2], "TCP", Control: true);
                                stm.Write(Writedatabyte3, 0, Writedatabyte3.Length);
                                await Task.Delay(2000);
                                stm.Read(ReadDataByte, 0, 255);
                                string DataReceived = ByteArrayToHexString(ReadDataByte);
                                if (DataReceived.Length > 4)
                                {
                                    GlobalDeclaration.StringLogger("PArserMbus Called at 1397", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                                    ParseMBUSData(DataReceived, GlobalDeclaration.MeterDetails[i].MModelId, MeterId, GlobalDeclaration.MeterDetails[i].TagsCount, LogTime, DeviceId);
                                    goto IL_0e3f;
                                }
                                if (RTry == 0)
                                {
                                    SaveData(MeterId, 0, LogTime);
                                    if (GlobalDeclaration.ISRepeatPoll)
                                    {
                                        SaveRepeatPolling(MeterId, LogTime, TransId);
                                    }
                                    goto IL_0e3f;
                                }
                                continue;
                            }
                            if (GlobalDeclaration.ErrorTest)
                            {
                                GlobalDeclaration.StringLogger("TCP Port : " + TrName + " MeterId: " + MeterId + " Data: " + tcprxBuff[_tcpPortID], "MBUS", "SendTCPCommand E5 Not Received", GlobalDeclaration.strLogPath);
                            }
                            nfi.Icon = Resources.Red;
                            if (RTry == 0)
                            {
                                SaveData(MeterId, 0, LogTime);
                                if (GlobalDeclaration.ISRepeatPoll)
                                {
                                    SaveRepeatPolling(MeterId, LogTime, TransId);
                                }
                                goto IL_0e3f;
                            }
                        }
                        catch (Exception ex3)
                        {
                            Exception ex2 = ex3;
                            ex2.ToString();
                            GlobalDeclaration.StringLogger(ex2.Message.ToString() + " in TCP Port : " + TrName + " MeterId: " + GlobalDeclaration.MeterDetails[i].MeterId + " Data: " + tcprxBuff[_tcpPortID], "MBUS", "SendTCPCommand Error", GlobalDeclaration.strLogPath);
                            nfi.Icon = Resources.Red;
                            SaveData(MeterId, 0, LogTime);
                            if (GlobalDeclaration.ISRepeatPoll)
                            {
                                SaveRepeatPolling(MeterId, LogTime, TransId);
                            }
                            goto IL_0e3f;
                        }
                        continue;
                    }
                    if (GlobalDeclaration._PollingID == 1)
                    {
                        _ = new byte[255];
                        byte[] ReadDataByte2 = new byte[255];
                        byte[] Writedatabyte4 = SerialTelegramPrimaryID(GlobalDeclaration.MeterDetails[i].MBUSID, _tcpPortID, "TCP");
                        stm.Write(Writedatabyte4, 0, Writedatabyte4.Length);
                        await Task.Delay(2000);
                        stm.Read(ReadDataByte2, 0, 255);
                        GlobalDeclaration.StringLogger("PArserMbus Called at 1451", "MBUS", "Single_TCP", GlobalDeclaration.strLogPath);
                        ParseMBUSData(ByteArrayToHexString(ReadDataByte2), GlobalDeclaration.MeterDetails[i].MModelId, GlobalDeclaration.MeterDetails[i].MeterId, GlobalDeclaration.MeterDetails[i].TagsCount, LogTime, DeviceId);
                    }
                    goto IL_0e3f;
                IL_0e3f:
                    if (ValveId != 0)
                    {
                        SaveValveStatus(GlobalDeclaration.MeterDetails[i].ValveId, Status: true, "Read", LogTime);
                    }
                    break;
                }
            }
        }
        catch (Exception ex3)
        {
            Exception ex = ex3;
            GlobalDeclaration.StringLogger(ex.Message.ToString() + " in TCP Port : " + TrName, "MBUS", "SendTCPCommand", GlobalDeclaration.strLogPath);
            if (ex.Message.Contains("does not exist"))
            {
                nfi.ShowBalloonTip(1000, "TCP PORT DISCONNECTED / MALFUNCTIONING..", "ERROR", ToolTipIcon.Error);
            }
        }
        finally
        {
            TCP_Status[_tcpPortID] = false;
            GlobalDeclaration.UpdateHistory(Id);
            MBUSTcpClients[_tcpPortID].Close();
        }
    }

    private void SaveLiveDataOnly(int _SlaveID, byte status, DateTime LogTime)
    {
        SqlCommand sqlCommand = new SqlCommand();
        try
        {
            sqlCommand.Connection = GlobalDeclaration._ASIMBUSDATA;
            sqlCommand.CommandTimeout = 300;
            sqlCommand.Parameters.AddWithValue("LogTime", LogTime);
            sqlCommand.Parameters.AddWithValue("Active", status);
            sqlCommand.Parameters.AddWithValue("MeterID", _SlaveID);
            for (int i = 1; i <= 300; i++)
            {
                if (RealTimeData[_SlaveID, i] == 0f)
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
                }
                else
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
                }
            }
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.CommandText = "SP_LIVEDATA";
            sqlCommand.ExecuteNonQuery();
            sqlCommand.Dispose();
            sqlCommand.Parameters.Clear();
        }
        catch (Exception ex)
        {
            sqlCommand.Dispose();
            GlobalDeclaration.StringLogger(ex.Message.ToString() + "\nSaving Data... ", "MBUS", "ParseMBUSData", GlobalDeclaration.strLogPath);
        }
    }

    private void SaveData(int _SlaveID, byte status, DateTime LogTime)
    {
        if (GlobalDeclaration.ErrorTest)
        {
            GlobalDeclaration.StringLogger(string.Concat("Meter ID :", _SlaveID, " Status :", status, " Logtime :", LogTime, " SaveintegratedTag", GlobalDeclaration.IsSaveIntegrated.ToString()), "MBUS TEST", "SaveData", GlobalDeclaration.strLogPath);
        }
        SqlCommand sqlCommand = new SqlCommand();
        try
        {
            GlobalDeclaration.StringLogger("Slave id = " + _SlaveID.ToString(), "Mod_Bus", "SaveData", GlobalDeclaration.strLogPath);
            sqlCommand.Connection = GlobalDeclaration._ASIMBUSDATA;
            sqlCommand.CommandTimeout = 300;
            sqlCommand.Parameters.AddWithValue("LogTime", LogTime);
            for (int i = 1; i <= 300; i++)
            {
                if (RealTimeData[_SlaveID, i] == 0f)
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
                    GlobalDeclaration.StringLogger("V" + i.ToString() + " = " + RealTimeData[_SlaveID, i], "frmMain", "SaveData", GlobalDeclaration.strLogPath);
                }
                else if (status == 0)
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
                    GlobalDeclaration.StringLogger("V" + i.ToString() + " = " + RealTimeData[_SlaveID, i], "frmMain", "SaveData", GlobalDeclaration.strLogPath);
                }
                else
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, RealTimeData[_SlaveID, i]);
                    GlobalDeclaration.StringLogger("V" + i.ToString() + " = " + RealTimeData[_SlaveID, i], "frmMain", "SaveData", GlobalDeclaration.strLogPath);
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
            GlobalDeclaration.StringLogger(ex.Message.ToString() + "\nSaving Data... ", "MBUS", "ParseMBUSData", GlobalDeclaration.strLogPath);
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
                GlobalDeclaration.StringLogger(ex2.Message.ToString() + "\nSaving Realtime Data ", "MBUS", "ParseMBUSData", GlobalDeclaration.strLogPath);
            }
        }
        if (GlobalDeclaration.IsSaveIntegrated)
        {
            Save_IntegratedTag(_SlaveID, LogTime, status);
        }
    }

    private void Save_IntegratedTag(int MeterId, DateTime LogTime, byte Status)
    {
        SqlCommand sqlCommand = new SqlCommand();
        try
        {
            if (GlobalDeclaration.ErrorTest)
            {
                GlobalDeclaration.StringLogger("Meter ID :" + MeterId + " Status :" + Status + " Logtime :" + LogTime, "MBUS TEST", "Save_IntegratedTag ", GlobalDeclaration.strLogPath);
            }
            sqlCommand.Connection = GlobalDeclaration._ASIMBUSDATA;
            sqlCommand.CommandTimeout = 300;
            sqlCommand.Parameters.AddWithValue("MeterID", MeterId);
            sqlCommand.Parameters.AddWithValue("LogTime", LogTime);
            sqlCommand.CommandType = CommandType.StoredProcedure;
            sqlCommand.CommandText = "SP_MASTERDATA_INTEGRATED";
            for (int i = 1; i <= 20; i++)
            {
                int num = GlobalDeclaration.IntegratedTag[i];
                if (RealTimeData[MeterId, num] == 0f)
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
                }
                else if (Status == 0)
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, Convert.DBNull);
                }
                else
                {
                    sqlCommand.Parameters.AddWithValue("V" + i, RealTimeData[MeterId, num]);
                }
            }
            sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            if (!ex.Message.Contains("PRIMARY KEY"))
            {
                GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS Frm Main", "Save_IntegratedTag", GlobalDeclaration.strLogPath);
            }
        }
        finally
        {
            sqlCommand.Dispose();
            sqlCommand.Parameters.Clear();
        }
    }

    private void SaveValveStatus(int ValveId, bool Status, string PollType, DateTime Logtime)
    {
        SqlDataReader sqlDataReader = null;
        try
        {
            int valveStatusTag = GlobalDeclaration.ValveStatusTag;
            string text = "";
            int num = 0;
            if (Status)
            {
                num = 1;
            }
            if (GlobalDeclaration.ErrorTest)
            {
                GlobalDeclaration.StringLogger("Valve ID :" + ValveId + " Control :" + Status.ToString() + " PollType :" + PollType + " Logtime :" + Logtime, "MBUS TEST", "SaveValveStatus", GlobalDeclaration.strLogPath);
            }
            string text2 = Logtime.ToString("yyyy-MM-dd hh:mm:ss tt");
            SqlCommand sqlCommand;
            if (PollType == "Read")
            {
                text = " Select Top 1 V" + valveStatusTag + " from [dbo].[Meter" + ValveId + "] where LogTime<(select max(LogTime) from [dbo].[Meter" + ValveId + "]) order by logtime desc";
                sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSDATA);
                sqlDataReader = sqlCommand.ExecuteReader();
                if (sqlDataReader.Read())
                {
                    num = Convert.ToInt32(sqlDataReader[0].ToString());
                }
            }
            text = " Insert into [dbo].[Meter" + ValveId + "]([LogTime],V" + valveStatusTag + ") Values('" + text2 + "'," + num + ") ";
            text = (text + " Update [MASTERDATA_REALTIME] Set [LogTime]='" + text2 + "',V" + valveStatusTag + "=" + num + " Where [MeterID]=" + ValveId) ?? "";
            sqlCommand = new SqlCommand(text, GlobalDeclaration._ASIMBUSDATA);
            int num2 = sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.ToString(), "DCU frmMain", "SaveValveStatus", GlobalDeclaration.strLogPath);
        }
        finally
        {
            sqlDataReader?.Close();
        }
    }

    private void SaveRepeatPolling(int MeterId, DateTime Logtime, int TransmitId)
    {
        try
        {
            string text = Logtime.ToString("yyyy-MM-dd hh:mm:ss tt");
            string cmdText = "Insert into [MeterRepeatPolling]([MeterID],[Logtime],[TransmitID]) Values(" + MeterId + ",'" + text + "'," + TransmitId + ")";
            SqlCommand sqlCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            int num = sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.ToString(), "DCU frmMain", "SaveRepeatPolling", GlobalDeclaration.strLogPath);
        }
    }

    private void DeleteRepeatPolling(int MeterId)
    {
        try
        {
            string text = "";
            text = ((MeterId != 0) ? ((" MeterID=" + MeterId) ?? "") : (" [Logtime]<'" + DateTime.Now.AddDays(-1.0).ToString("yyyy-MM-dd hh:mm:ss tt") + "'"));
            string cmdText = ("  Delete from [MeterRepeatPolling] Where " + text) ?? "";
            SqlCommand sqlCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            int num = sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.ToString(), "DCU frmMain", "SaveRepeatPolling", GlobalDeclaration.strLogPath);
        }
    }

    private byte[] SerialTelegramPrimaryID(string _slavename, int _portID, string Mode)
    {
        string text = "";
        text = "107B";
        string text2 = Convert.ToInt32(_slavename).ToString("X");
        if (text2.Length == 1)
        {
            text2 = "0" + text2;
        }
        text += text2;
        text += computeCheckSum(text.Substring(2));
        text += "16";
        if (Mode == "TCP")
        {
            tcprxBuff[_portID] = "";
        }
        else
        {
            rxBuff[_portID] = "";
        }
        return HexStringToByteArray(text);
    }

    private byte[] Telegram_Convert(string MeterName, int _portID, string Telegramstring, string Mode)
    {
        string[] array = Telegramstring.Split('~');
        string text = "";
        for (int i = 0; i < array.Length; i++)
        {
            text = ((!(array[i] == "MeterName")) ? ((!(array[i] == "CheckSum")) ? (text + array[i]) : (text + computeCheckSum(text.Substring(8)))) : (text + MeterName));
        }
        byte[] array2;
        if (Mode == "TCP")
        {
            array2 = new byte[255];
            array2 = HexStringToByteArray(text);
            tcprxBuff[_portID] = "";
        }
        else
        {
            array2 = HexStringToByteArray(text);
            rxBuff[_portID] = "";
        }
        return array2;
    }

    private byte[] Telegram_Convert(string MeterName, int _portID, string Telegramstring, string Mode, bool Control)
    {
        string[] array = Telegramstring.Split('~');
        string text = "";
        for (int i = 0; i < array.Length; i++)
        {
            text = ((!(array[i] == "MeterName")) ? ((!(array[i] == "CheckSum")) ? ((!(array[i] == "Control")) ? (text + array[i]) : ((!Control) ? (text + "00") : (text + "01"))) : (text + computeCheckSum(text.Substring(8)))) : (text + MeterName));
        }
        byte[] array2;
        if (Mode == "TCP")
        {
            array2 = new byte[255];
            array2 = HexStringToByteArray(text);
            tcprxBuff[_portID] = "";
        }
        else
        {
            array2 = HexStringToByteArray(text);
            rxBuff[_portID] = "";
        }
        return array2;
    }

    public bool isClientConnected(TcpClient ClientSocket)
    {
        IPGlobalProperties iPGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
        TcpConnectionInformation[] activeTcpConnections = iPGlobalProperties.GetActiveTcpConnections();
        TcpConnectionInformation[] array = activeTcpConnections;
        foreach (TcpConnectionInformation tcpConnectionInformation in array)
        {
            TcpState tcpState = tcpConnectionInformation.State;
            if (tcpConnectionInformation.LocalEndPoint.Equals(ClientSocket.Client.LocalEndPoint))
            {
                if (tcpState == TcpState.Established)
                {
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    private byte[] HexStringToByteArray(string s)
    {
        s = s.Replace(" ", "");
        byte[] array = new byte[s.Length / 2];
        for (int i = 0; i < s.Length; i += 2)
        {
            array[i / 2] = Convert.ToByte(s.Substring(i, 2), 16);
        }
        return array;
    }

    private void CalculateCheckSum(string s)
    {
        byte[] bytes = Encoding.ASCII.GetBytes(s);
        byte b = 0;
        for (int i = 0; i < bytes.Length; i++)
        {
            b += bytes[i];
        }
    }

    public static string computeCheckSum(string values)
    {
        int num = 0;
        string s;
        for (int i = 0; i < values.Length - 1; i += 2)
        {
            s = values[i].ToString() + values[i + 1];
            int num2 = int.Parse(s, NumberStyles.HexNumber);
            num += int.Parse(s, NumberStyles.HexNumber);
        }
        s = (num % 256).ToString("X");
        if (s.Length != 2)
        {
            s = "0" + s;
        }
        return s;
    }

    private string StringToHex(string hexstring)
    {
        StringBuilder stringBuilder = new StringBuilder();
        foreach (char value in hexstring)
        {
            stringBuilder.Append(Convert.ToInt32(value).ToString("x"));
        }
        return stringBuilder.ToString();
    }

    private void SerialPortDataReceivedHandler(object sender, SerialDataReceivedEventArgs e)
    {
        SerialPort serialPort = (SerialPort)sender;
        string portName = serialPort.PortName;
        int num = 0;
        for (int i = 0; i < GlobalDeclaration.TransDetails.Count; i++)
        {
            if (GlobalDeclaration.TransDetails[i].TransmitName == portName)
            {
                num = GlobalDeclaration.TransDetails[i].TypeId;
            }
        }
        if (serialPort.IsOpen)
        {
            int bytesToRead = serialPort.BytesToRead;
            byte[] array = new byte[bytesToRead];
            serialPort.Read(array, 0, bytesToRead);
            rxBuff[num] += ByteArrayToHexString(array);
        }
    }

    private void ParseMBUSData(string _ReceivedData, int MODELID, int SlaveID, int NoofTags, DateTime LogTime, string DeviceId)
    {
        //GlobalDeclaration.StringLogger("release 16 ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
        //GlobalDeclaration.StringLogger($"Recieved Data {_ReceivedData}\n MODELID{MODELID}\n " +
        //    $"slaveID {SlaveID} \n NoOfTags {NoofTags}\n LogTime {LogTime}\n" +
        //    $"DeviceID {DeviceId}", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

        if (SlaveID == 15)
        {
            GlobalDeclaration.StringLogger($"Enetered New If Conditions", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
            var packet1 = _ReceivedData
         .HexToBytes()
         .ToFrame()
         .ToPacket();
            double energyValue = Convert.ToDouble(((VariableDataPacket)packet1).Records[0].NormalizedValue.Item2);
            double volumeValue = Convert.ToDouble(((VariableDataPacket)packet1).Records[3].NormalizedValue.Item2);
            double flowTempValue = Convert.ToDouble(((VariableDataPacket)packet1).Records[8].NormalizedValue.Item2);
            string connectionString = "Data Source=localhost;Initial Catalog=CustomerDB;User Id=sa;Password=Macgrid18*";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    string query = "Insert into Meter" + SlaveID + "(LogTime, V1, V7, V30) Values (@value1,@value2,@value3,@value4)";
                    using (SqlCommand command = new SqlCommand(query,connection))
                    {
                        command.Parameters.AddWithValue("@value1",LogTime);
                        command.Parameters.AddWithValue("@value2",volumeValue);
                        command.Parameters.AddWithValue("@value3", flowTempValue);
                        command.Parameters.AddWithValue("@value4", energyValue);
                        command.ExecuteNonQuery();
                    }
                    string query2 = "update MASTERDATA_REALTIME SET LogTime= @NewValue1, V1=@NewValue2, V7=@NewValue3, V30=@NewValue4 WHERE MeterId="+SlaveID;
                    using (SqlCommand Command2 = new SqlCommand(query2, connection))
                    {
                        Command2.Parameters.AddWithValue("@NewValue1",LogTime);
                        Command2.Parameters.AddWithValue("@NewValue2",volumeValue);
                        Command2.Parameters.AddWithValue("@NewValue3",flowTempValue);
                        Command2.Parameters.AddWithValue("@NewValue4",energyValue);
                        Command2.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    GlobalDeclaration.StringLogger($" Error in new implementation. {ex.Message}", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                }

            }
        }
        else
        {
           
            //GlobalDeclaration.StringLogger("Recieved Data = " + _ReceivedData.ToString(), "Mod_Bus", "ParseMbus", GlobalDeclaration.strLogPath);

            try
            {
                nfi.Icon = Resources.Green;
                if (GlobalDeclaration.ErrorTest)
                {
                    GlobalDeclaration.StringLogger("MeterId: " + SlaveID + " LogTime :" + LogTime, "MBUS TEST", "ParseMBUSData", GlobalDeclaration.strLogPath);
                }
                if (GlobalDeclaration._PushDatatoCloud == 1)
                {
                    int num = (int)LogTime.Subtract(new DateTime(1970, 1, 1)).TotalSeconds;
                    string dT = Convert.ToString(num);
                    string cLIENT = "";
                    for (int i = 0; i < GlobalDeclaration.MeterDetails.Count; i++)
                    {
                        if (SlaveID == GlobalDeclaration.MeterDetails[i].MeterId)
                        {
                            cLIENT = GlobalDeclaration.MeterDetails[i].MeterName;
                            break;
                        }
                    }
                    RealtimeDataPushtoCloud(DeviceId, dT, cLIENT, _ReceivedData);
                }
                int length = _ReceivedData.Length;
                int num2 = 0;
                int num3 = 0;
                float num4 = 0f;
                int num5 = 0;
                int num6 = 0;
                if (_ReceivedData.Length <= 4)
                {
                    GlobalDeclaration.StringLogger("returned from if (_ReceivedData.Length <= 4) ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    return;
                }
                _ReceivedData = _ReceivedData.Replace(" ", string.Empty);
                //GlobalDeclaration.StringLogger("Recieved Data After Replace = " + _ReceivedData.ToString(), "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                if (GlobalDeclaration._PollingID == 1 && _ReceivedData.Substring(0, 2) == "10")
                {
                    _ReceivedData = _ReceivedData.Substring(10);
                }
                int num7 = Convert.ToInt32(_ReceivedData.Substring(2, 2), 16);
                num5 = 8;
                num6 = 2;
                int num8 = Convert.ToInt32(_ReceivedData.Substring(num5, num6), 16);
                num5 += 2;
                num6 = 2;
                int num9 = Convert.ToInt32(_ReceivedData.Substring(num5, num6), 16);
                num5 += 2;
                num6 = 2;
                string text = _ReceivedData.Substring(num5, num6);
                //GlobalDeclaration.StringLogger("text = " + text.ToString(), "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                num5 += 2;
                num6 = 8;
                string text2 = GlobalDeclaration.Reverse(_ReceivedData.Substring(num5, num6));
               // GlobalDeclaration.StringLogger("text2 = " + text2.ToString(), "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                num5 += num6;
                num6 = 4;
                string value = GlobalDeclaration.Reverse(_ReceivedData.Substring(num5, num6));
                int num10 = Convert.ToInt32(value, 16);
                byte[] buffer = new byte[num10];
                decodeManufacturerId(buffer, 0);
                num5 += num6;
                num6 = 2;
                string text3 = _ReceivedData.Substring(num5, num6);
               // GlobalDeclaration.StringLogger("text3 = " + text.ToString(), "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                num5 += num6;
                num6 = 2;
                string medium = _ReceivedData.Substring(num5, num6);
                string text4 = MediumTable(medium);
                num5 += num6;
                num6 = 2;
                int num11 = Convert.ToInt32(_ReceivedData.Substring(num5, num6), 16);
                num5 += num6;
                num6 = 2;
                string text5 = _ReceivedData.Substring(num5, num6);
                if (text5 != "00")
                {
                    SaveMeterError(text5, MODELID, SlaveID);
                }
                num5 += num6;
                num6 = 4;
                string text6 = _ReceivedData.Substring(num5, num6);
                string text7 = "";
                if (GlobalDeclaration._ASIMBUSSETTINGSCnx.State == ConnectionState.Closed)
                {
                    GlobalDeclaration._ASIMBUSSETTINGSCnx.Open();
                }
                DataTable dataTable = new DataTable();
                SqlCommand sqlCommand = new SqlCommand("Select * from SETTINGS_AssignParameters Where Modelid = " + MODELID + " Order by [ParameterID]", GlobalDeclaration._ASIMBUSSETTINGSCnx);
                dataTable.Clear();
                dataTable.Load(sqlCommand.ExecuteReader());
               // GlobalDeclaration.StringLogger("No of Tags: " + NoofTags, "frmMain", "SaveData", GlobalDeclaration.strLogPath);
                do
                {
                    //GlobalDeclaration.StringLogger("inside do num 3 is : " + num3, "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                IL_0328:
                    num5 += num6;
                    num6 = 2;
                    string s = _ReceivedData.Substring(num5, num6);
                    string text8 = HextoBinary(s, 8);

                    string binaryValue = HextoBinary(s, 4);
                    string text9 = CalulateDataField(binaryValue);

                    string text10 = CalulateFunctionField(text8.Substring(2, 2));

                    string text11 = text8.Substring(2, 1);

                    string text12 = FindVIForDIFE(text8.Substring(0, 1));
                    //GlobalDeclaration.StringLogger($"text 8 : {text8}\n text 9 : {text9} \n text 10 : {text10} \n text 11 : {text11} \n text 12 : {text12}", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    //GlobalDeclaration.StringLogger("checking condition text12==DIFE", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    if (text12 == "DIFE")
                    {
                        DIFE1.Clear();
                        DIFE2.Clear();
                        DIFE3.Clear();
                        DIFE4.Clear();
                        DIFE5.Clear();
                        DIFE6.Clear();
                        DIFE7.Clear();
                        DIFE8.Clear();
                        DIFE9.Clear();
                        DIFE10.Clear();
                        for (int j = 0; j <= 9; j++)
                        {
                            //GlobalDeclaration.StringLogger("executing J for Loop. ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                            num5 += num6;
                            num6 = 2;
                            string dIFE = HextoBinary(_ReceivedData.Substring(num5, num6), 8);
                            dIFE = ReadDIFETable(dIFE, j);
                            if (dIFE == "0")
                            {
                                // GlobalDeclaration.StringLogger("returned by break  if (dIFE == \"0\")", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                                break;
                            }
                        }
                    }
                    num5 += num6;
                    num6 = 2;
                    //GlobalDeclaration.StringLogger("checking condition num5>num7*2", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    if (num5 >= num7 * 2)
                    {
                        SaveData(SlaveID, 1, LogTime);


                        //GlobalDeclaration.StringLogger("returned from if (num5 >= num7 * 2)  ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                        return;
                    }
                    string text13 = HextoBinary(_ReceivedData.Substring(num5, num6), 8);
                    string text14 = ReadVIFTable(text13.Substring(1));
                    GlobalDeclaration.StringLogger($"text 13 : {text13}\n text 14 : {text14} ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    VIFE1.Clear();
                    //GlobalDeclaration.StringLogger("checking condition text13.substring", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    if (text13.Substring(0, 1) == "1")
                    {
                        for (int k = 0; k <= 9; k++)
                        {
                            num5 += num6;
                            num6 = 2;
                            string vIFE = HextoBinary(_ReceivedData.Substring(num5, num6), 8);
                            GlobalDeclaration.StringLogger($"string vife value in k loop {vIFE} k value {k} ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                            vIFE = ReadVIFETable(vIFE, k);
                            if (Error != "")
                            {
                            }
                            if (vIFE == "0")
                            {
                                //  GlobalDeclaration.StringLogger("returned by break  if (vIFE == \"0\")", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                                break;
                            }
                        }
                    }
                    foreach (var item in VIFE1)
                    {
                        GlobalDeclaration.StringLogger("VIFE1 Value " + item, "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    }

                    //GlobalDeclaration.StringLogger("checking condition Vife1 Contains ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    if (VIFE1.Contains("Data Over Flow") || VIFE1.Contains("Data Under Flow") || VIFE1.Contains("Data Error"))
                    {
                        SaveData(SlaveID, 1, LogTime);

                        //GlobalDeclaration.StringLogger("returned from if  (VIFE1.Contains(\"Data Over Flow\") || VIFE1.Contains(\"Data Under Flow\") || VIFE1.Contains(\"Data Error\")) ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                        return;
                    }
                    num2 = Convert.ToInt32(dataTable.Rows[num3]["ParameterIndex"].ToString());

                    //GlobalDeclaration.StringLogger("num 2 value : " + num2.ToString(), "frmMain", "SaveData", GlobalDeclaration.strLogPath);
                    num4 = Convert.ToSingle(dataTable.Rows[num3]["MF"].ToString());
                    //GlobalDeclaration.StringLogger("num 3 value : " + num3.ToString(), "frmMain", "SaveData", GlobalDeclaration.strLogPath);
                    string[] array = text9.Split('/');
                    int num12 = Convert.ToInt32(array[0]);
                    //GlobalDeclaration.StringLogger("checking condition num2 != 0 ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    if (num2 != 0)
                    {
                        string text15 = array[1].ToString();
                        num5 += num6;
                        num6 = num12;
                        if (text15 == "NoData")
                        {
                            num3++;
                            if (NoofTags == num3)
                            {
                                SaveData(SlaveID, 1, LogTime);
                                //GlobalDeclaration.StringLogger("returned from if (nooftags==num3) ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                                return;
                            }
                            //GlobalDeclaration.StringLogger("num3 : " + num3, "Mod_Bus", "gotoIL_0328-1", GlobalDeclaration.strLogPath);
                            goto IL_0328;
                        }
                        string[] array2 = text14.Split(' ');
                        if (array2.Length <= 1)
                        {
                            num3++;
                            if (NoofTags == num3)
                            {
                                SaveData(SlaveID, 1, LogTime);
                                GlobalDeclaration.StringLogger("returned from if (Nooftags==num3) ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                                return;
                            }
                            //GlobalDeclaration.StringLogger("num 3 : " + num3, "Mod_Bus", "gotoIL0328-2", GlobalDeclaration.strLogPath);
                            goto IL_0328;
                        }
                        string text16 = array2[0].ToString();
                        string text17 = array2[1].ToString();
                        //GlobalDeclaration.StringLogger($"text 15 {text15} text 16 {text16} text17 {text17}", "Mbus", "ParseMBUS", GlobalDeclaration.strLogPath);
                        if (text15 == "BCD")
                        {
                            float num13 = 0f;
                            string text18 = HextoBCD(_ReceivedData.Substring(num5, num6));
                            //GlobalDeclaration.StringLogger($"text 18 {text18} ", "Mbus", "ParseMBUS", GlobalDeclaration.strLogPath);
                            if (text18.Substring(0, 1) == "F")
                            {
                                text18 = "-" + text18.Substring(1).ToString();
                            }
                            try
                            {
                                num13 = Convert.ToInt64(text18);

                            }
                            catch (Exception ex)
                            {
                                GlobalDeclaration.StringLogger(ex.Message.ToString() + "\n ArrivedData " + _ReceivedData, "MBUS", "ParseMBUSData : SlaveId :" + SlaveID, GlobalDeclaration.strLogPath);
                                num13 = 0f;
                            }
                            RealTimeData[SlaveID, num2] = Convert.ToSingle(num13 * num4);
                            //GlobalDeclaration.StringLogger($"Real time value inside BCD slave id = {SlaveID} ,num2 = {num2}, MF ={num4} : " + RealTimeData[SlaveID, num2].ToString(), "frmMain", "ParseMbus", GlobalDeclaration.strLogPath);

                            text7 = text7 + "Value " + num13 + ", Unit " + text16 + ", Description " + text17 + "\n";
                        }
                        else if (text15 == "Integer")
                        {
                            if (num12 == 2)
                            {
                                int num14 = 0;
                                string s2 = LittleEndian(_ReceivedData.Substring(num5, num6));

                                num14 = int.Parse(s2, NumberStyles.HexNumber);

                                RealTimeData[SlaveID, num2] = Convert.ToSingle((float)num14 * num4);
                                //GlobalDeclaration.StringLogger($"Real time value inside Integer slave id = {SlaveID} ,num2 = {num2}, MF = {num4}, S2 = {s2} : " + RealTimeData[SlaveID, num2].ToString(), "frmMain", "ParseMbus", GlobalDeclaration.strLogPath);
                                text7 = text7 + "Value " + num14 + ", Unit " + text16 + ", Description " + text17 + "\n";

                            }
                            else
                            {
                                float num15 = 0f;
                                string text19 = LittleEndian(_ReceivedData.Substring(num5, num6));
                                num15 = ((!(text19.Substring(0, 1) == "F")) ? ((float)long.Parse(text19, NumberStyles.HexNumber)) : ((float)FromHex(text19.Substring(1).ToString())));
                                RealTimeData[SlaveID, num2] = Convert.ToSingle(num15 * num4);
                                //GlobalDeclaration.StringLogger($"Real time value inside else slave id = {SlaveID} ,num2 = {num2} , MF = {num4}, text 19 = {text19} : " + RealTimeData[SlaveID, num2].ToString(), "frmMain", "SaveData", GlobalDeclaration.strLogPath);
                                text7 = text7 + "Value " + num15 + ", Unit " + text16 + ", Description " + text17 + "\n";

                            }
                        }
                    }
                    else
                    {
                        num5 += num6;
                        num6 = num12;
                    }
                    //GlobalDeclaration.StringLogger($"completed do-while for : {num3} ", "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);
                    num3++;
                    //GlobalDeclaration.StringLogger("num3 is : " + num3, "Mod_Bus", "ParseMBUS", GlobalDeclaration.strLogPath);

                }
                while (NoofTags != num3);

                //GlobalDeclaration.StringLogger(" text 7 value =   " + text7, "frmMain", "ParseMbus", GlobalDeclaration.strLogPath);

                SaveData(SlaveID, 1, LogTime);
            }
            catch (Exception ex2)
            {
                if (ex2.Message.Contains("Index and length must refer."))
                {
                    SaveData(SlaveID, 1, LogTime);
                }
                GlobalDeclaration.StringLogger(ex2.Message.ToString() + "\n Received Data " + _ReceivedData, "MBUS", "ParseMBUSData:MeterId :" + SlaveID, GlobalDeclaration.strLogPath);
            }
        }

    }

    private void SaveMeterError(string hexError, int ModelId, int MeterId)
    {
        try
        {
            string text = HextoBinary(hexError, 8);
            string text2 = "";
            if (text[4] == '1')
            {
                text2 = "Permanent Error";
            }
            else if (text[3] == '1')
            {
                text2 = "Temporary Error";
            }
            else if (text[5] == '1')
            {
                text2 = "Power Low";
            }
            if (GlobalDeclaration._ASIMBUSSETTINGSCnx.State == ConnectionState.Closed)
            {
                GlobalDeclaration._ASIMBUSSETTINGSCnx.Open();
            }
            string text3 = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt");
            string cmdText = "Insert into [dbo].[MeterErrorLog]([MeterID],[ErrorCode],[ErrorType],[ErrorDate],[CustomerID])  Values(" + MeterId + ",'" + hexError + "','" + text2 + "','" + text3 + "'," + GlobalDeclaration.CustomerId + ")";
            SqlCommand sqlCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU frmMain", "SaveMeterError, MeterId :" + MeterId + " ErrorCode :" + hexError, GlobalDeclaration.strLogPath);
        }
        finally
        {
        }
    }

    private static int FromHex(string value)
    {
        int num = Convert.ToInt32(value, 16);
        return (num <= 8388607) ? num : (num | -16777216);
    }

    private string LittleEndian(string num)
    {
        int value = Convert.ToInt32(num, 16);
        byte[] bytes = BitConverter.GetBytes(value);
        string text = "";
        byte[] array = bytes;
        foreach (byte b in array)
        {
            text += b.ToString("X2");
        }
        return text;
    }

    private string HextoBCD(string Hex)
    {
        string text = "";
        string[] array = new string[Hex.Length / 2 + ((Hex.Length % 2 != 0) ? 1 : 0)];
        for (int i = 0; i < array.Length; i++)
        {
            array[i] = Hex.Substring(i * 2, (i * 2 + 2 > Hex.Length) ? 1 : 2);
        }
        for (int j = 0; j < array.Length; j++)
        {
            text += array[array.Length - (j + 1)];
        }
        return text;
    }

    private string ReadVIFETable(string VIFE, int VIFENo)
    {
        if (VIFE == "11111011")
        {
            if ((VIFE.Substring(1) == "0000000") | (VIFE.Substring(1) == "0000001"))
            {
                VIFE1.Add("Energy Mwh");
            }
            else if ((VIFE.Substring(1) == "0000010") | (VIFE.Substring(1) == "0000011"))
            {
                VIFE1.Add("Reserved");
            }
            else if ((VIFE.Substring(1) == "0000100") | (VIFE.Substring(1) == "0000101") | (VIFE.Substring(1) == "0000110") | (VIFE.Substring(1) == "0000111"))
            {
                VIFE1.Add("Reserved");
            }
            else if ((VIFE.Substring(1) == "0001000") | (VIFE.Substring(1) == "0001001"))
            {
                VIFE1.Add("Energy GJ");
            }
            else if ((VIFE.Substring(1) == "0001010") | (VIFE.Substring(1) == "0001011"))
            {
                VIFE1.Add("Reserved");
            }
            else if ((VIFE.Substring(1) == "0001100") | (VIFE.Substring(1) == "0001101") | (VIFE.Substring(1) == "0001110") | (VIFE.Substring(1) == "0001111"))
            {
                VIFE1.Add("Reserved");
            }
            else if ((VIFE.Substring(1) == "0010000") | (VIFE.Substring(1) == "0010001"))
            {
                VIFE1.Add("Volume m3");
            }
            else if ((VIFE.Substring(1) == "0010010") | (VIFE.Substring(1) == "0010011"))
            {
                VIFE1.Add("Reserved");
            }
            else if ((VIFE.Substring(1) == "0010100") | (VIFE.Substring(1) == "0010101") | (VIFE.Substring(1) == "0010110") | (VIFE.Substring(1) == "0010101"))
            {
                VIFE1.Add("Reserved");
            }
            else if ((VIFE.Substring(1) == "0011000") | (VIFE.Substring(1) == "0011001"))
            {
                VIFE1.Add("Mass t");
            }
            else if (VIFE.Substring(1) == "0100001")
            {
                VIFE1.Add("Volume feet^3");
            }
            else if (VIFE.Substring(1) == "0100010")
            {
                VIFE1.Add("Volume AmericanGallon");
            }
            else if (VIFE.Substring(1) == "0100011")
            {
                VIFE1.Add("Volume AmericanGallon ");
            }
            else if (VIFE.Substring(1) == "0100100")
            {
                VIFE1.Add("Volumeflow AmericanGallon/m");
            }
            else if (VIFE.Substring(1) == "0100101")
            {
                VIFE1.Add("Volumeflow AmericanGallon/m");
            }
            else if (VIFE.Substring(1) == "0100110")
            {
                VIFE1.Add("Volumeflow AmericanGallon/h");
            }
            else if ((VIFE.Substring(1) == "0101000") | (VIFE.Substring(1) == "0101001"))
            {
                VIFE1.Add("Power MW");
            }
            else if ((VIFE.Substring(1) == "0110000") | (VIFE.Substring(1) == "0110001"))
            {
                VIFE1.Add("Power GJ/h");
            }
            else if ((VIFE.Substring(1) == "1011000") | (VIFE.Substring(1) == "1011001") | (VIFE.Substring(1) == "1011010") | (VIFE.Substring(1) == "1011011"))
            {
                VIFE1.Add("FlowTemperature F");
            }
            else if ((VIFE.Substring(1) == "1011100") | (VIFE.Substring(1) == "1011101") | (VIFE.Substring(1) == "1011110") | (VIFE.Substring(1) == "1011111"))
            {
                VIFE1.Add("ReturnTemperature F");
            }
            else if ((VIFE.Substring(1) == "1100000") | (VIFE.Substring(1) == "1100001") | (VIFE.Substring(1) == "1100010") | (VIFE.Substring(1) == "1100011"))
            {
                VIFE1.Add("TemperaureDiffence F");
            }
            else if ((VIFE.Substring(1) == "1100100") | (VIFE.Substring(1) == "1100101") | (VIFE.Substring(1) == "1100110") | (VIFE.Substring(1) == "1100111"))
            {
                VIFE1.Add("ExternalTemperature F");
            }
            else if ((VIFE.Substring(1) == "1110000") | (VIFE.Substring(1) == "1110001") | (VIFE.Substring(1) == "1110010") | (VIFE.Substring(1) == "1110011"))
            {
                VIFE1.Add("Cold/WarmTemplimt F");
            }
            else if ((VIFE.Substring(1) == "1110100") | (VIFE.Substring(1) == "1110101") | (VIFE.Substring(1) == "1110110") | (VIFE.Substring(1) == "1110111"))
            {
                VIFE1.Add("Cold/WarmTemplimt C");
            }
            else if ((VIFE.Substring(1) == "1111000") | (VIFE.Substring(1) == "1111001") | (VIFE.Substring(1) == "1111010") | (VIFE.Substring(1) == "1111011") | (VIFE.Substring(1) == "1111100") | (VIFE.Substring(1) == "1111101") | (VIFE.Substring(1) == "1111110") | (VIFE.Substring(1) == "1111111"))
            {
                VIFE1.Add("Cum.CountMaxPower W");
            }
            else
            {
                VIFE1.Add("Reserved");
            }
        }
        else if (VIFE == "11111101")
        {
            if ((VIFE.Substring(1) == "0000000") | (VIFE.Substring(1) == "0000001") | (VIFE.Substring(1) == "0000010") | (VIFE.Substring(1) == "0000011"))
            {
                VIFE1.Add("CreditCurrency Untis");
            }
            else if ((VIFE.Substring(1) == "0000100") | (VIFE.Substring(1) == "0000101") | (VIFE.Substring(1) == "0000110") | (VIFE.Substring(1) == "0000111"))
            {
                VIFE1.Add("DebitCurrency Units");
            }
            else if (VIFE.Substring(1) == "0001000")
            {
                VIFE1.Add("Access No");
            }
            else if (VIFE.Substring(1) == "0001001")
            {
                VIFE1.Add("Medium");
            }
            else if (VIFE.Substring(1) == "0001010")
            {
                VIFE1.Add("Manufacturer");
            }
            else if (VIFE.Substring(1) == "0001011")
            {
                VIFE1.Add("ParameterID");
            }
            else if (VIFE.Substring(1) == "0001100")
            {
                VIFE1.Add("Model/Version");
            }
            else if (VIFE.Substring(1) == "0001101")
            {
                VIFE1.Add("Hardware Version");
            }
            else if (VIFE.Substring(1) == "0001110")
            {
                VIFE1.Add("Firmware Version");
            }
            else if (VIFE.Substring(1) == "0001111")
            {
                VIFE1.Add("Software Version");
            }
            else if (VIFE.Substring(1) == "0010000")
            {
                VIFE1.Add("Customer Location");
            }
            else if (VIFE.Substring(1) == "0010001")
            {
                VIFE1.Add("Customer Name");
            }
            else if (VIFE.Substring(1) == "0010010")
            {
                VIFE1.Add("Access Code User");
            }
            else if (VIFE.Substring(1) == "0010011")
            {
                VIFE1.Add("Access Code Operator");
            }
            else if (VIFE.Substring(1) == "0010100")
            {
                VIFE1.Add("Access Code System Operator");
            }
            else if (VIFE.Substring(1) == "0010101")
            {
                VIFE1.Add("Access Code Developer");
            }
            else if (VIFE.Substring(1) == "0010110")
            {
                VIFE1.Add("Password");
            }
            else if (VIFE.Substring(1) == "0010111")
            {
                VIFE1.Add("Error Flag");
            }
            else if (VIFE.Substring(1) == "0011000")
            {
                VIFE1.Add("Error Mask");
            }
            else if (VIFE.Substring(1) == "0011010")
            {
                VIFE1.Add("Digital Output");
            }
            else if (VIFE.Substring(1) == "0011011")
            {
                VIFE1.Add("Digital Input");
            }
            else if (VIFE.Substring(1) == "0011100")
            {
                VIFE1.Add("Baud Rate");
            }
            else if (VIFE.Substring(1) == "0011101")
            {
                VIFE1.Add("Response Delay Time");
            }
            else if (VIFE.Substring(1) == "0011110")
            {
                VIFE1.Add("Retry");
            }
            else if (VIFE.Substring(1) == "0100000")
            {
                VIFE1.Add("First Storage");
            }
            else if (VIFE.Substring(1) == "0100001")
            {
                VIFE1.Add("Last Storage");
            }
            else if (VIFE.Substring(1) == "0100010")
            {
                VIFE1.Add("Size of Storage");
            }
            else if ((VIFE.Substring(1) == "0100100") | (VIFE.Substring(1) == "0100101") | (VIFE.Substring(1) == "0100110") | (VIFE.Substring(1) == "0100111"))
            {
                if (VIFE.Substring(1) == "0100100")
                {
                    VIFE1.Add("Size of Storage / Second");
                }
                else if (VIFE.Substring(1) == "0100101")
                {
                    VIFE1.Add("Size of Storage / Minute");
                }
                else if (VIFE.Substring(1) == "0100110")
                {
                    VIFE1.Add("Size of Storage / Hour");
                }
                else if (VIFE.Substring(1) == "0100111")
                {
                    VIFE1.Add("Size of Storage / day");
                }
            }
            else if (VIFE.Substring(1) == "0101000")
            {
                VIFE1.Add("Storage Interval / Month");
            }
            else if (VIFE.Substring(1) == "0101001")
            {
                VIFE1.Add("Storage Interval / Year");
            }
            else if ((VIFE.Substring(1) == "0101100") | (VIFE.Substring(1) == "0101101") | (VIFE.Substring(1) == "0101110") | (VIFE.Substring(1) == "0101111"))
            {
                if (VIFE.Substring(1) == "0101100")
                {
                    VIFE1.Add("Duration Since LastReadout / Second");
                }
                else if (VIFE.Substring(1) == "0101101")
                {
                    VIFE1.Add("Duration Since LastReadout / Minute");
                }
                else if (VIFE.Substring(1) == "0101110")
                {
                    VIFE1.Add("Duration Since LastReadout / Hour");
                }
                else if (VIFE.Substring(1) == "0101111")
                {
                    VIFE1.Add("Duration Since LastReadout / day");
                }
            }
            else if (VIFE.Substring(1) == "0110000")
            {
                VIFE1.Add("Starting Date/Time of Tariff");
            }
            else if ((VIFE.Substring(1) == "0110000") | (VIFE.Substring(1) == "0110001") | (VIFE.Substring(1) == "0110010") | (VIFE.Substring(1) == "0110011"))
            {
                if (VIFE.Substring(1) == "0110000")
                {
                    VIFE1.Add("Duration of Tariff / Second");
                }
                else if (VIFE.Substring(1) == "0110001")
                {
                    VIFE1.Add("Duration of Tariff / Minute");
                }
                else if (VIFE.Substring(1) == "0110010")
                {
                    VIFE1.Add("Duration of Tariff / Hour");
                }
                else if (VIFE.Substring(1) == "0110011")
                {
                    VIFE1.Add("Duration of Tariff / day");
                }
            }
            else if ((VIFE.Substring(1) == "0110100") | (VIFE.Substring(1) == "0110101") | (VIFE.Substring(1) == "0110110") | (VIFE.Substring(1) == "0110111"))
            {
                if (VIFE.Substring(1) == "0110100")
                {
                    VIFE1.Add("Period of Tariff / Second");
                }
                else if (VIFE.Substring(1) == "0110101")
                {
                    VIFE1.Add("Period of Tariff / Minute");
                }
                else if (VIFE.Substring(1) == "0110110")
                {
                    VIFE1.Add("Period of Tariff / Hour");
                }
                else if (VIFE.Substring(1) == "0110111")
                {
                    VIFE1.Add("Period of Tariff / day");
                }
            }
            else if (VIFE.Substring(1) == "0111000")
            {
                VIFE1.Add("Period of Tariff / Months");
            }
            else if (VIFE.Substring(1) == "0111001")
            {
                VIFE1.Add("Period of Tariff / Years");
            }
            else if (VIFE.Substring(1) == "0111010")
            {
                VIFE1.Add("Dimensionless / No VIF");
            }
            else if ((VIFE.Substring(1) == "1000000") | (VIFE.Substring(1) == "1000001") | (VIFE.Substring(1) == "1000010") | (VIFE.Substring(1) == "1000011") | (VIFE.Substring(1) == "1000100") | (VIFE.Substring(1) == "1000101") | (VIFE.Substring(1) == "1000110") | (VIFE.Substring(1) == "1000111") | (VIFE.Substring(1) == "1001000") | (VIFE.Substring(1) == "1001001") | (VIFE.Substring(1) == "1001010") | (VIFE.Substring(1) == "1001011") | (VIFE.Substring(1) == "1001100") | (VIFE.Substring(1) == "1001101") | (VIFE.Substring(1) == "1001110") | (VIFE.Substring(1) == "1001111"))
            {
                VIFE1.Add("Volts / 9");
            }
            else if ((VIFE.Substring(1) == "1010000") | (VIFE.Substring(1) == "1010001") | (VIFE.Substring(1) == "1010010") | (VIFE.Substring(1) == "1010011") | (VIFE.Substring(1) == "1010100") | (VIFE.Substring(1) == "1010101") | (VIFE.Substring(1) == "1010110") | (VIFE.Substring(1) == "1010111") | (VIFE.Substring(1) == "1011000") | (VIFE.Substring(1) == "1011001") | (VIFE.Substring(1) == "1011010") | (VIFE.Substring(1) == "1011011") | (VIFE.Substring(1) == "1011100") | (VIFE.Substring(1) == "1011101") | (VIFE.Substring(1) == "1011110") | (VIFE.Substring(1) == "1011111"))
            {
                VIFE1.Add("Amps / 12");
            }
            else if (VIFE.Substring(1) == "1100000")
            {
                VIFE1.Add("Reset Counter");
            }
            else if (VIFE.Substring(1) == "1100001")
            {
                VIFE1.Add("Cummlation Counter");
            }
            else if (VIFE.Substring(1) == "1100010")
            {
                VIFE1.Add("Control Signal");
            }
            else if (VIFE.Substring(1) == "1100011")
            {
                VIFE1.Add("Day of Week");
            }
            else if (VIFE.Substring(1) == "1100100")
            {
                VIFE1.Add("Week No");
            }
            else if (VIFE.Substring(1) == "1100101")
            {
                VIFE1.Add("Time Point of Day Change");
            }
            else if (VIFE.Substring(1) == "1100110")
            {
                VIFE1.Add("State of Parameter Activation");
            }
            else if (VIFE.Substring(1) == "1100111")
            {
                VIFE1.Add("Special Supply of Information");
            }
            else if ((VIFE.Substring(1) == "1101000") | (VIFE.Substring(1) == "1101001") | (VIFE.Substring(1) == "1101010") | (VIFE.Substring(1) == "1101011"))
            {
                if (VIFE.Substring(1) == "1101000")
                {
                    VIFE1.Add("Duration Since Last Cumulation / Hour");
                }
                else if (VIFE.Substring(1) == "1101001")
                {
                    VIFE1.Add("Duration Since Last Cumulation / Day");
                }
                else if (VIFE.Substring(1) == "1101010")
                {
                    VIFE1.Add("Duration Since Last Cumulation / Month");
                }
                else if (VIFE.Substring(1) == "1101011")
                {
                    VIFE1.Add("Duration Since Last Cumulation / Year");
                }
            }
            else if ((VIFE.Substring(1) == "1101100") | (VIFE.Substring(1) == "1101101") | (VIFE.Substring(1) == "1101110") | (VIFE.Substring(1) == "1101111"))
            {
                if (VIFE.Substring(1) == "1101100")
                {
                    VIFE1.Add("Operation Time Batery / Hour");
                }
                else if (VIFE.Substring(1) == "1101101")
                {
                    VIFE1.Add("Operation Time Batery / Day");
                }
                else if (VIFE.Substring(1) == "1101110")
                {
                    VIFE1.Add("Operation Time Batery / Month");
                }
                else if (VIFE.Substring(1) == "1101111")
                {
                    VIFE1.Add("Operation Time Batery / Year");
                }
            }
            else if (VIFE.Substring(1) == "1110000")
            {
                VIFE1.Add("Date and Time of Batter Change");
            }
            else
            {
                VIFE1.Add("Reserved");
            }
        }
        else if (VIFE.Substring(0, 2) == "00")
        {
            bool flag = false;
            if (VIFE.Substring(1) == "0000000")
            {
                VIFE1.Add("None");
            }
            else if (VIFE.Substring(1) == "0000001")
            {
                VIFE1.Add("Too Many DIFE");
            }
            else if (VIFE.Substring(1) == "0000010")
            {
                VIFE1.Add("Storage No Not Implemented");
            }
            else if (VIFE.Substring(1) == "0000011")
            {
                VIFE1.Add("Unit No Not Implemented");
            }
            else if (VIFE.Substring(1) == "0000100")
            {
                VIFE1.Add("Tariff Not Implemented");
            }
            else if (VIFE.Substring(1) == "0000101")
            {
                VIFE1.Add("Function Not Implemented");
            }
            else if (VIFE.Substring(1) == "0000110")
            {
                VIFE1.Add("Data Class Not Implemented");
            }
            else if (VIFE.Substring(1) == "0000111")
            {
                VIFE1.Add("Data Size Not Implemented");
            }
            else if (VIFE.Substring(1) == "0001011")
            {
                VIFE1.Add("Too Many VIFE");
            }
            else if (VIFE.Substring(1) == "0001100")
            {
                VIFE1.Add("Illegal VIF Group");
            }
            else if (VIFE.Substring(1) == "0001101")
            {
                VIFE1.Add("Illegal VIF Exponent");
            }
            else if (VIFE.Substring(1) == "0001110")
            {
                VIFE1.Add("VIF / DIF Mismatch");
            }
            else if (VIFE.Substring(1) == "0001111")
            {
                VIFE1.Add("Unimplemented Action");
            }
            else if (VIFE.Substring(1) == "0010101")
            {
                VIFE1.Add("No Data Available");
            }
            else if (VIFE.Substring(1) == "0010110")
            {
                VIFE1.Add("Data Over Flow");
            }
            else if (VIFE.Substring(1) == "0010111")
            {
                VIFE1.Add("Data Under Flow");
            }
            else if (VIFE.Substring(1) == "0011000")
            {
                VIFE1.Add("Data Error");
            }
            else if (VIFE.Substring(1) == "0011100")
            {
                VIFE1.Add("Premature End of Record");
            }
            else
            {
                VIFE1.Add("Reserved");
                flag = true;
            }
            if (!flag)
            {
                Error = VIFE1[VIFE1.Count - 1].ToString();
            }
        }
        else if (VIFE.Substring(1) == "0100000")
        {
            VIFE1.Add("Per Second");
        }
        else if (VIFE.Substring(1) == "0100001")
        {
            VIFE1.Add("Per Minute");
        }
        else if (VIFE.Substring(1) == "0100010")
        {
            VIFE1.Add("Per Hour");
        }
        else if (VIFE.Substring(1) == "0100011")
        {
            VIFE1.Add("Per Day");
        }
        else if (VIFE.Substring(1) == "0100100")
        {
            VIFE1.Add("Per Week");
        }
        else if (VIFE.Substring(1) == "0100101")
        {
            VIFE1.Add("Per Month");
        }
        else if (VIFE.Substring(1) == "0100110")
        {
            VIFE1.Add("Per Year");
        }
        else if (VIFE.Substring(1) == "0100111")
        {
            VIFE1.Add("Per Revolution or Measurement");
        }
        else if ((VIFE.Substring(1) == "0101000") | (VIFE.Substring(1) == "0101001"))
        {
            if (VIFE.Substring(1) == "0101000")
            {
                VIFE1.Add("Pulse Input Not Incremented");
            }
            else
            {
                VIFE1.Add("Pulse Input Incremented");
            }
        }
        else if (VIFE.Substring(1) == "0101010")
        {
            if (VIFE.Substring(1) == "0101010")
            {
                VIFE1.Add("Pulse Output Not Incremented");
            }
            else
            {
                VIFE1.Add("Pulse Output Incremented");
            }
        }
        else if (VIFE.Substring(1) == "0101100")
        {
            VIFE1.Add("Per Litre");
        }
        else if (VIFE.Substring(1) == "0101101")
        {
            VIFE1.Add("Per m3");
        }
        else if (VIFE.Substring(1) == "0101110")
        {
            VIFE1.Add("Per Kg");
        }
        else if (VIFE.Substring(1) == "0101111")
        {
            VIFE1.Add("Per Kelvin");
        }
        else if (VIFE.Substring(1) == "0100000")
        {
            VIFE1.Add("Per KWh");
        }
        else if (VIFE.Substring(1) == "0100001")
        {
            VIFE1.Add("Per GJ");
        }
        else if (VIFE.Substring(1) == "0100010")
        {
            VIFE1.Add("Per kW");
        }
        else if (VIFE.Substring(1) == "0110011")
        {
            VIFE1.Add("Per Kelvin Litre");
        }
        else if (VIFE.Substring(1) == "0110100")
        {
            VIFE1.Add("Per Volt");
        }
        else if (VIFE.Substring(1) == "0110101")
        {
            VIFE1.Add("Per Amps");
        }
        else if (VIFE.Substring(1) == "0110110")
        {
            VIFE1.Add("multiplied by sek");
        }
        else if (VIFE.Substring(1) == "0110111")
        {
            VIFE1.Add("multiplied by sek / V");
        }
        else if (VIFE.Substring(1) == "0111000")
        {
            VIFE1.Add("multiplied by sek / A");
        }
        else if (VIFE.Substring(1) == "0111001")
        {
            VIFE1.Add("Start Date");
        }
        else if (VIFE.Substring(1) == "0111010")
        {
            VIFE1.Add("VIF Contain Uncorrected Unit Instead of Corrected Unit");
        }
        else if (VIFE.Substring(1) == "0111011")
        {
            VIFE1.Add("Accumulation only if Positive Contributions");
        }
        else if (VIFE.Substring(1) == "0111100")
        {
            VIFE1.Add("Accumuation of ABS Value only if Negative Contribution");
        }
        else if (VIFE.Substring(1) == "1000000")
        {
            VIFE1.Add("Lower Limit Value");
        }
        else if (VIFE.Substring(1) == "1001000")
        {
            VIFE1.Add("Upper Limit Value");
        }
        else if (VIFE.Substring(1) == "1000001")
        {
            VIFE1.Add("Exceeds Lower Limit");
        }
        else if (VIFE.Substring(1) == "1001001")
        {
            VIFE1.Add("Exceeds Upper Limit");
        }
        else if (VIFE.Substring(1) == "1000010")
        {
            VIFE1.Add("Lower Limit Exceed First Date Begin");
        }
        else if (VIFE.Substring(1) == "1001010")
        {
            VIFE1.Add("Upper Limit Exceed First Date Begin");
        }
        else if (VIFE.Substring(1) == "1000110")
        {
            VIFE1.Add("Lower Limit Exceed Last Date Begin");
        }
        else if (VIFE.Substring(1) == "1001110")
        {
            VIFE1.Add("Upper Limit Exceed Last Date Begin");
        }
        else if (VIFE.Substring(1) == "1000011")
        {
            VIFE1.Add("Lower Limit Exceed First Date End");
        }
        else if (VIFE.Substring(1) == "1001011")
        {
            VIFE1.Add("Upper Limit Exceed First Date End");
        }
        else if (VIFE.Substring(1) == "1000111")
        {
            VIFE1.Add("Lower Limit Exceed Last Date End");
        }
        else if (VIFE.Substring(1) == "1001111")
        {
            VIFE1.Add("Upper Limit Exceed Last Date End");
        }
        else if (VIFE.Substring(1) == "1010000")
        {
            VIFE1.Add("lower limit exceed first second");
        }
        else if (VIFE.Substring(1) == "1010001")
        {
            VIFE1.Add("lower limit exceed first minute");
        }
        else if (VIFE.Substring(1) == "1010010")
        {
            VIFE1.Add("lower limit exceed first hour");
        }
        else if (VIFE.Substring(1) == "1010011")
        {
            VIFE1.Add("lower limit exceed first day");
        }
        else if (VIFE.Substring(1) == "1010100")
        {
            VIFE1.Add("lower limit exceed last second");
        }
        else if (VIFE.Substring(1) == "1010101")
        {
            VIFE1.Add("lower limit exceed last minute");
        }
        else if (VIFE.Substring(1) == "1010110")
        {
            VIFE1.Add("lower limit exceed  last hour");
        }
        else if (VIFE.Substring(1) == "1010111")
        {
            VIFE1.Add("lower limit exceed  last day");
        }
        else if (VIFE.Substring(1) == "1011000")
        {
            VIFE1.Add("upper limit exceed first second");
        }
        else if (VIFE.Substring(1) == "1011001")
        {
            VIFE1.Add("upper limit exceed first minute");
        }
        else if (VIFE.Substring(1) == "1011010")
        {
            VIFE1.Add("upper limit exceed first hour");
        }
        else if (VIFE.Substring(1) == "1011011")
        {
            VIFE1.Add("upper limit exceed first day");
        }
        else if (VIFE.Substring(1) == "1011100")
        {
            VIFE1.Add("upper limit exceed last second");
        }
        else if (VIFE.Substring(1) == "1011101")
        {
            VIFE1.Add("upper limit exceed last minute");
        }
        else if (VIFE.Substring(1) == "1011110")
        {
            VIFE1.Add("upper limit exceed last hour");
        }
        else if (VIFE.Substring(1) == "1011111")
        {
            VIFE1.Add("upper limit exceed last day");
        }
        else if (VIFE.Substring(1) == "1100000")
        {
            VIFE1.Add("first second");
        }
        else if (VIFE.Substring(1) == "1100001")
        {
            VIFE1.Add("first minutes");
        }
        else if (VIFE.Substring(1) == "1100010")
        {
            VIFE1.Add("first hour");
        }
        else if (VIFE.Substring(1) == "1100011")
        {
            VIFE1.Add("first day");
        }
        else if (VIFE.Substring(1) == "1100100")
        {
            VIFE1.Add("last second");
        }
        else if (VIFE.Substring(1) == "1100101")
        {
            VIFE1.Add("last minute");
        }
        else if (VIFE.Substring(1) == "1100110")
        {
            VIFE1.Add("last hour");
        }
        else if (VIFE.Substring(1) == "1100111")
        {
            VIFE1.Add("last day");
        }
        else if (VIFE.Substring(1) == "1101010")
        {
            VIFE1.Add("first begin date");
        }
        else if (VIFE.Substring(1) == "1101011")
        {
            VIFE1.Add("first end date");
        }
        else if (VIFE.Substring(1) == "1101110")
        {
            VIFE1.Add("last bedin date ");
        }
        else if (VIFE.Substring(1) == "1101111")
        {
            VIFE1.Add("end of last");
        }
        else if (VIFE.Substring(1) == "1110000")
        {
            VIFE1.Add("Multiplication Factor");
        }
        else if (VIFE.Substring(1) == "1111000")
        {
            VIFE1.Add("Addictive Correction Constant");
        }
        else if (VIFE.Substring(1) == "1111101")
        {
            VIFE1.Add("Multiplication Factor");
        }
        else if (VIFE.Substring(1) == "1111110")
        {
            VIFE1.Add("Future value");
        }
        else if (VIFE.Substring(1) == "1111111")
        {
            VIFE1.Add("Manufacture Specific Data");
        }
        else
        {
            VIFE1.Add("Reserved");
        }
        string text = "";
        int num = Convert.ToInt32(VIFE.Substring(0, 1));
        if (num == 1)
        {
            return "1";
        }
        return "0";
    }

    private string ReadDIFETable(string DIFE, int DIFENo)
    {
        string text = "0";
        ArrayList arrayList = new ArrayList();
        switch (DIFENo)
        {
            case 0:
                arrayList = DIFE1;
                break;
            case 1:
                arrayList = DIFE2;
                break;
            case 2:
                arrayList = DIFE3;
                break;
            case 3:
                arrayList = DIFE4;
                break;
            case 4:
                arrayList = DIFE5;
                break;
            case 5:
                arrayList = DIFE6;
                break;
            case 6:
                arrayList = DIFE7;
                break;
            case 7:
                arrayList = DIFE8;
                break;
            case 8:
                arrayList = DIFE9;
                break;
            case 9:
                arrayList = DIFE10;
                break;
        }
        text = DIFE.Substring(4, 4);
        int num = BinaryToDecimal(Convert.ToInt32(text));
        arrayList.Add(num);
        text = DIFE.Substring(2, 2);
        int num2 = BinaryToDecimal(Convert.ToInt32(text));
        arrayList.Add(num2);
        text = DIFE.Substring(1, 1);
        int num3 = BinaryToDecimal(Convert.ToInt32(text));
        arrayList.Add(num3);
        text = DIFE.Substring(0, 1);
        int num4 = Convert.ToInt32(text);
        if (num4 == 1)
        {
            return text = "1";
        }
        return text = "0";
    }

    private int BinaryToDecimal(int Binary)
    {
        int num = 0;
        int num2 = 0;
        int num3 = 1;
        while (Binary > 0)
        {
            num = Binary % 10;
            num2 += num * num3;
            Binary /= 10;
            num3 *= 2;
        }
        return num2;
    }

    private string decodeManufacturerId(byte[] buffer, int _startByte)
    {
        string text = "";
        int num = (buffer[_startByte++] & 0xFF) + (buffer[_startByte++] + 8);
        char c = (char)((num & 0x1F) + 64);
        num += 5;
        char c2 = (char)((num & 0x1F) + 64);
        num += 5;
        char c3 = (char)((num & 0x1F) + 64);
        return string.Concat(c3 + (c2 + c));
    }

    private string MediumTable(string Medium)
    {
        string text = "";
        switch (Medium)
        {
            case "00":
                return "Other";
            case "01":
                return "Oil";
            case "02":
                return "Electricity";
            case "03":
                return "Gas";
            case "04":
                return "Heat Outlet";
            case "05":
                return "Steam";
            case "06":
                return "Hot Water";
            case "07":
                return "Water";
            case "08":
                return "Heat Cost Allocator";
            case "09":
                return "Compressed Air";
            case "0A":
                return "Cooling Load Meter Outlet";
            case "0B":
                return "Cooling Load Meter Inlet";
            case "0C":
                return "Heat Inlet";
            case "0D":
                return "Heat / Cooling Load Meter";
            case "0E":
                return "Bus / System";
            case "0F":
                return "Unknown Medium";
            default:
                if ((Medium == "10") | (Medium == "11") | (Medium == "12") | (Medium == "13") | (Medium == "14") | (Medium == "15"))
                {
                    return "Reserved";
                }
                return Medium switch
                {
                    "16" => "Cold Water",
                    "17" => "Dual Water",
                    "18" => "Pressure",
                    "19" => "AD Convertor",
                    _ => "Reserved",
                };
        }
    }

    private string ReadVIFTable(string VIF)
    {
        string result = "";
        if ((VIF == "0000000") | (VIF == "0000001") | (VIF == "0000010") | (VIF == "0000011") | (VIF == "0000100") | (VIF == "0000101") | (VIF == "0000110") | (VIF == "0000111"))
        {
            result = "Energy Wh";
        }
        else if ((VIF == "0001000") | (VIF == "0001001") | (VIF == "0001010") | (VIF == "0001011") | (VIF == "0001100") | (VIF == "0001101") | (VIF == "0001110") | (VIF == "0001111"))
        {
            result = "Energy WJ";
        }
        else if ((VIF == "0010000") | (VIF == "0010001") | (VIF == "0010010") | (VIF == "0010011") | (VIF == "0010100") | (VIF == "0010101") | (VIF == "0010110") | (VIF == "0010111"))
        {
            result = "Volume m3";
        }
        else if ((VIF == "0011000") | (VIF == "0011001") | (VIF == "0011010") | (VIF == "0011011") | (VIF == "0011100") | (VIF == "0011101") | (VIF == "0011110") | (VIF == "0011111"))
        {
            result = "Mass kg";
        }
        else if ((VIF == "0100000") | (VIF == "0100001") | (VIF == "0100010") | (VIF == "0100011"))
        {
            switch (VIF)
            {
                case "0100000":
                    result = "OnTime Second";
                    break;
                case "0100001":
                    result = "OnTime Minute";
                    break;
                case "0100010":
                    result = "OnTime Hour";
                    break;
                case "0100011":
                    result = "OnTime Day";
                    break;
            }
        }
        else if ((VIF == "0100100") | (VIF == "0100101") | (VIF == "0100110") | (VIF == "0100111"))
        {
            switch (VIF)
            {
                case "0100100":
                    result = "OperatingTime Second";
                    break;
                case "0100101":
                    result = "OperatingTime Minute";
                    break;
                case "0100110":
                    result = "OperatingTime Hour";
                    break;
                case "0100111":
                    result = "OperatingTime Day";
                    break;
            }
        }
        else if ((VIF == "0101000") | (VIF == "0101001") | (VIF == "0101010") | (VIF == "0101011") | (VIF == "0101100") | (VIF == "0101101") | (VIF == "0101110") | (VIF == "0101111"))
        {
            result = "Power W";
        }
        else if ((VIF == "0110000") | (VIF == "0110001") | (VIF == "0110010") | (VIF == "0110011") | (VIF == "0110100") | (VIF == "0110101") | (VIF == "0110110") | (VIF == "0110111"))
        {
            result = "Power J/h";
        }
        else if ((VIF == "0111000") | (VIF == "0111001") | (VIF == "0111010") | (VIF == "0111011") | (VIF == "0111100") | (VIF == "0111101") | (VIF == "0111110") | (VIF == "0111111"))
        {
            result = "VolumeFlow m^3/h";
        }
        else if ((VIF == "1000000") | (VIF == "1000001") | (VIF == "1000010") | (VIF == "1000011") | (VIF == "1000100") | (VIF == "1000101") | (VIF == "1000110") | (VIF == "1000111"))
        {
            result = "VolumeFlow m^3/m";
        }
        else if ((VIF == "1001000") | (VIF == "1001001") | (VIF == "1001010") | (VIF == "1001011") | (VIF == "1001100") | (VIF == "1001101") | (VIF == "1001110") | (VIF == "1001111"))
        {
            result = "Volume Flow m^3/s";
        }
        else if ((VIF == "1010000") | (VIF == "1010001") | (VIF == "1010010") | (VIF == "1010011") | (VIF == "1010100") | (VIF == "1010101") | (VIF == "1010110") | (VIF == "1010111"))
        {
            result = "MassFlow kg/h";
        }
        else if ((VIF == "1011000") | (VIF == "1011001") | (VIF == "1011010") | (VIF == "1011011"))
        {
            result = "FlowTemperature C";
        }
        else if ((VIF == "1011100") | (VIF == "1011101") | (VIF == "1011110") | (VIF == "1011111"))
        {
            result = "ReturnTemperature C";
        }
        else if ((VIF == "1100000") | (VIF == "1100001") | (VIF == "1100010") | (VIF == "1100011"))
        {
            result = "TemperatureDifference k";
        }
        else if ((VIF == "1100100") | (VIF == "1100101") | (VIF == "1100110") | (VIF == "1100111"))
        {
            result = "ExternalTemperature C";
        }
        else if ((VIF == "1101000") | (VIF == "1101001") | (VIF == "1101010") | (VIF == "1101011"))
        {
            result = "Pressure bar";
        }
        else if ((VIF == "1101100") | (VIF == "1101101"))
        {
            if (VIF == "1101100")
            {
                result = "TimePoint Date";
            }
            if (VIF == "1101101")
            {
                result = "TimePoint Date&Time";
            }
        }
        else if (VIF == "1101110")
        {
            result = "UnitsforHCA D";
        }
        else if (VIF == "1101111")
        {
            result = "Reserved NA";
        }
        else if ((VIF == "1110000") | (VIF == "1110001") | (VIF == "1110010") | (VIF == "1110011"))
        {
            switch (VIF)
            {
                case "1110000":
                    result = "Ave.Duration Second";
                    break;
                case "1110001":
                    result = "Ave.Duration Minute";
                    break;
                case "1110010":
                    result = "Ave.Duration Hour";
                    break;
                case "1110011":
                    result = "Ave.Duration Day";
                    break;
            }
        }
        else if ((VIF == "1110100") | (VIF == "1110101") | (VIF == "1110110") | (VIF == "1110111"))
        {
            switch (VIF)
            {
                case "1110100":
                    result = "Actual.Duration Second";
                    break;
                case "1110101":
                    result = "Actual.Duration Minute";
                    break;
                case "1110110":
                    result = "Actual.Duration Hour";
                    break;
                case "1110111":
                    result = "Actual.Duration Day";
                    break;
            }
        }
        else
        {
            switch (VIF)
            {
                case "1111000":
                    result = "FabricationNo NA";
                    break;
                case "1111001":
                    result = "Identification no";
                    break;
                case "1111010":
                    result = "BusAddress NA";
                    break;
                case "1111101":
                    result = "ExtensionVIF.Code NA";
                    break;
            }
        }
        return result;
    }

    private string FindVIForDIFE(string BinaryValue)
    {
        string text = "";
        if (BinaryValue == "0")
        {
            return "VIF";
        }
        return "DIFE";
    }

    private string CalulateFunctionField(string BinaryValue)
    {
        string result = "";
        switch (BinaryValue)
        {
            case "00":
                result = "Instant";
                break;
            case "10":
                result = "Minimum";
                break;
            case "01":
                result = "Maximum";
                break;
            case "11":
                result = "Error";
                break;
        }
        return result;
    }

    private string CalulateDataField(string BinaryValue)
    {
        string result = "";
        switch (BinaryValue)
        {
            case "0000":
                result = "0/NoData";
                break;
            case "0001":
                result = "2/Integer";
                break;
            case "0010":
                result = "4/Integer";
                break;
            case "0011":
                result = "6/Integer";
                break;
            case "0100":
                result = "8/Integer";
                break;
            case "0101":
                result = "8/Real";
                break;
            case "0110":
                result = "12/Integer";
                break;
            case "0111":
                result = "16/Integer";
                break;
            case "1000":
                result = "0/NoData";
                break;
            case "1001":
                result = "2/BCD";
                break;
            case "1010":
                result = "4/BCD";
                break;
            case "1011":
                result = "6/BCD";
                break;
            case "1100":
                result = "8/BCD";
                break;
            case "1101":
                result = "8/Variable Lenght";
                break;
            case "1110":
                result = "12/BCD";
                break;
            case "1111":
                result = "16/Special Function";
                break;
        }
        return result;
    }

    private string HextoBinary(string s, int lastHowmuchCharacter)
    {
        string text = string.Join(string.Empty, s.Select((char c) => Convert.ToString(Convert.ToInt32(c.ToString(), 16), 2).PadLeft(4, '0')));
        return text.Substring(text.Length - lastHowmuchCharacter);
    }

    private string ByteArrayToHexString(byte[] data)
    {
        StringBuilder stringBuilder = new StringBuilder(data.Length * 3);
        foreach (byte b in data)
        {
            stringBuilder.Append(Convert.ToString(b, 16).PadLeft(2, '0').PadRight(3, ' '));
        }
        return stringBuilder.ToString().ToUpper();
    }

    private void stopPollingToolStripMenuItem_Click(object sender, EventArgs e)
    {
        try
        {
            MyTimer.Stop();
            Timer_ValveSchedule.Stop();
            MemoryCheck.Stop();
            for (int i = 0; i < GlobalDeclaration.TransDetails.Count; i++)
            {
                int typeId = GlobalDeclaration.TransDetails[i].TypeId;
                if (GlobalDeclaration.TransDetails[i].GatewayType == "MBUS")
                {
                    if (GlobalDeclaration.TransDetails[i].TransmitMode == "Serial")
                    {
                        if ((MBUSSerialPorts[typeId] as SerialPort).IsOpen)
                        {
                            (MBUSSerialPorts[typeId] as SerialPort).Close();
                        }
                    }
                    else if (GlobalDeclaration.TransDetails[i].TransmitMode == "TCP")
                    {
                        MBUSTcpClients[typeId].Close();
                    }
                }
                ObjBMS.Port_Disable();
            }
            nfi.Visible = false;
            Close();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS", "stopPollingToolStripMenuItem_Click", GlobalDeclaration.strLogPath);
            nfi.Visible = false;
            Close();
        }
    }

    private void frmMain_Activated(object sender, EventArgs e)
    {
        if (byteload == 0)
        {
            base.Visible = false;
            byteload = 1;
        }
    }

    private void frmMain_MinimumSizeChanged(object sender, EventArgs e)
    {
        byteload = 1;
        base.WindowState = FormWindowState.Minimized;
        nfi.Visible = false;
        base.Visible = false;
    }

    private void tsExit_Click(object sender, EventArgs e)
    {
        nfi.Visible = false;
        Close();
    }

    private void tsStart_Click(object sender, EventArgs e)
    {
        tsStop.Visible = true;
        Application.DoEvents();
        Form1_Load(sender, e);
        tsStart.Visible = false;
    }

    private void RealtimeDataPushtoCloud(string _DEV, string _DT, string _CLIENT, string _DATA)
    {
        if (GlobalDeclaration._PushDatatoCloud != 1)
        {
            return;
        }
        try
        {
            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(GlobalDeclaration._CloudIP);
            httpWebRequest.ContentType = "application/json";
            httpWebRequest.Method = "POST";
            using (StreamWriter streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
            {
                string value = new JavaScriptSerializer().Serialize(new
                {
                    DEV = _DEV,
                    DT = _DT,
                    CLIENT = _CLIENT,
                    DATA = _DATA
                });
                streamWriter.Write(value);
            }
            HttpWebResponse httpWebResponse = (HttpWebResponse)httpWebRequest.GetResponse();
            using StreamReader streamReader = new StreamReader(httpWebResponse.GetResponseStream());
            string text = streamReader.ReadToEnd();
        }
        catch (Exception)
        {
            StoreRawData(_DEV, _DT, _CLIENT, _DATA);
        }
    }

    private void StoreRawData(string _DEV, string _DT, string _CLIENT, string _DATA)
    {
        try
        {
            string cmdText = "Insert into [MBUS_STOREDDATA] ([DEV],[DT],[CLIENT],[DATA]) Values('" + _DEV + "','" + _DT + "','" + _CLIENT + "','" + _DATA + "')";
            SqlCommand sqlCommand = new SqlCommand(cmdText, GlobalDeclaration._ASIMBUSDATA);
            int num = sqlCommand.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU", "StoreRawData Device ID :" + _DEV + " MeterName :" + _CLIENT, GlobalDeclaration.strLogPath);
        }
    }

    private async Task StoredDataPushtoCloud()
    {
        SqlDataAdapter sqladapter = new SqlDataAdapter();
        DataTable dt = new DataTable();
        try
        {
            if (GlobalDeclaration.ErrorTest)
            {
                GlobalDeclaration.StringLogger(string.Concat("Time :", DateTime.Now, " Cloud IP :", GlobalDeclaration._CloudIP), "MBUS TEST", "StoredDataPushtoCloud", GlobalDeclaration.strLogPath);
            }
            string strQuery2 = "SELECT * FROM MBUS_STOREDDATA";
            SqlCommand sqlCmd2 = new SqlCommand(strQuery2, GlobalDeclaration._ASIMBUSDATA);
            sqladapter.SelectCommand = sqlCmd2;
            sqladapter.Fill(dt);
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                try
                {
                    HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(GlobalDeclaration._CloudIP);
                    httpWebRequest.ContentType = "application/json";
                    httpWebRequest.Method = "POST";
                    using (StreamWriter streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
                    {
                        string json = new JavaScriptSerializer().Serialize(new
                        {
                            DEV = dt.Rows[i]["DEV"].ToString(),
                            DT = dt.Rows[i]["DT"].ToString(),
                            CLIENT = dt.Rows[i]["CLIENT"].ToString(),
                            DATA = dt.Rows[i]["DATA"].ToString()
                        });
                        streamWriter.Write(json);
                    }
                    HttpWebResponse httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
                    using (StreamReader streamReader = new StreamReader(httpResponse.GetResponseStream()))
                    {
                        streamReader.ReadToEnd();
                    }
                    strQuery2 = "Delete from MBUS_STOREDDATA Where DEV = '" + dt.Rows[i]["DEV"].ToString() + "' and DT = '" + dt.Rows[i]["DT"].ToString() + "' and CLIENT = '" + dt.Rows[i]["CLIENT"].ToString() + "' and DATA = '" + dt.Rows[i]["DATA"].ToString() + "'";
                    sqlCmd2 = new SqlCommand(strQuery2, GlobalDeclaration._ASIMBUSDATA);
                    sqlCmd2.ExecuteNonQuery();
                }
                catch (Exception)
                {
                }
            }
        }
        catch (Exception ex3)
        {
            Exception ex = ex3;
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU", "StoredDataPushtoCloud ", GlobalDeclaration.strLogPath);
        }
    }

    private async Task ReadTemplate(int TemplateId)
    {
        string Format = "";
        char Separator = ',';
        string FilePath = "";
        string MeterIds = "";
        bool IsSysTime = true;
        bool IsMeterId = true;
        try
        {
            List<Import> DColumns = new List<Import>();
            string Query3 = ("Select [Format],[DateFormat],[Separator],[FilePath],[MeterID] From [SETTINGS_DataInterface] Where [TemplateID]=" + TemplateId) ?? "";
            SqlCommand GetCommand2 = new SqlCommand(Query3, GlobalDeclaration._ASIMBUSSETTINGSCnx);
            SqlDataReader rdr = GetCommand2.ExecuteReader();
            while (rdr.Read())
            {
                Format = rdr["Format"].ToString();
                FilePath = rdr["FilePath"].ToString();
                MeterIds = rdr["MeterID"].ToString();
                Separator = Convert.ToChar(rdr["Separator"].ToString());
                rdr["DateFormat"].ToString();
            }
            string[] MeterAr = MeterIds.Split(',');
            if (Format.Contains("LogtTime"))
            {
                IsSysTime = false;
            }
            if (Format.Contains("MeterID"))
            {
                IsMeterId = false;
            }
            string[] ArrFormat = Format.Split(',');
            for (int l = 0; l < ArrFormat.Length; l++)
            {
                if (ArrFormat[l] != "[Dummy]")
                {
                    DColumns.Add(new Import
                    {
                        ColumnIndex = l,
                        ColumnName = ArrFormat[l]
                    });
                }
            }
            DataTable dt = new DataTable();
            if (IsMeterId)
            {
                dt.Columns.Add("MeterId");
            }
            if (IsSysTime)
            {
                dt.Columns.Add("LogTime");
            }
            for (int k = 0; k < DColumns.Count; k++)
            {
                dt.Columns.Add(DColumns[k].ColumnName);
            }
            string[] Lines = File.ReadAllLines(FilePath);
            int len = ((!IsMeterId) ? Lines.Length : (MeterAr.Length + 1));
            for (int j = 1; j < len; j++)
            {
                string[] Fields = Lines[j].Split(Separator);
                dt.Rows.Add();
                if (IsMeterId)
                {
                    dt.Rows[j - 1]["MeterId"] = MeterAr[j - 1];
                }
                if (IsSysTime)
                {
                    dt.Rows[j - 1]["LogTime"] = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt");
                }
                for (int n = 0; n < DColumns.Count; n++)
                {
                    string val = Fields[DColumns[n].ColumnIndex];
                    dt.Rows[j - 1][DColumns[n].ColumnName] = val;
                }
            }
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string MeterID = dt.Rows[i]["MeterId"].ToString();
                string Time = dt.Rows[i]["LogTime"].ToString();
                string ColsName = "";
                string ColsValue = "";
                string UpdateValues = "";
                for (int m = 0; m < dt.Columns.Count; m++)
                {
                    string HeadName = dt.Columns[m].ColumnName.ToString();
                    if (HeadName != "MeterId" && HeadName != "LogTime")
                    {
                        ColsName = ColsName + "," + dt.Columns[m].ColumnName.ToString();
                        ColsValue = ColsValue + "," + dt.Rows[i][m].ToString();
                        UpdateValues = ("," + dt.Columns[m].ColumnName.ToString() + "=" + dt.Rows[i][m].ToString()) ?? "";
                    }
                }
                Query3 = "Insert into Meter" + MeterID + "([LogTime]" + ColsName + ") Values('" + Time + "' " + ColsValue + ")";
                Query3 = string.Concat(Query3, "Update [MASTERDATA_REALTIME] Set LogTime='" + Time + "',[Active]=1 " + UpdateValues + " where [MeterID]=" + MeterID);
                GetCommand2 = new SqlCommand(Query3, GlobalDeclaration._ASIMBUSDATA);
                GetCommand2.ExecuteNonQuery();
            }
        }
        catch (Exception ex2)
        {
            Exception ex = ex2;
            GlobalDeclaration.StringLogger(ex.Message.ToString(), "MBUS DCU", "ReadTemplate ", GlobalDeclaration.strLogPath);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && components != null)
        {
            components.Dispose();
        }
        base.Dispose(disposing);
    }

    private void InitializeComponent()
    {
        this.components = new System.ComponentModel.Container();
        this.nfi = new System.Windows.Forms.NotifyIcon(this.components);
        this.contextMenuStrip1 = new System.Windows.Forms.ContextMenuStrip(this.components);
        this.tsStop = new System.Windows.Forms.ToolStripMenuItem();
        this.tsStart = new System.Windows.Forms.ToolStripMenuItem();
        this.tsExit = new System.Windows.Forms.ToolStripMenuItem();
        this.toolTip1 = new System.Windows.Forms.ToolTip(this.components);
        this.contextMenuStrip1.SuspendLayout();
        this.SuspendLayout();
        // 
        // nfi
        // 
        this.nfi.BalloonTipIcon = System.Windows.Forms.ToolTipIcon.Info;
        this.nfi.ContextMenuStrip = this.contextMenuStrip1;
        this.nfi.Text = "SMART - MBUS POLL";
        this.nfi.Visible = true;
        // 
        // contextMenuStrip1
        // 
        this.contextMenuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.tsStop,
            this.tsStart,
            this.tsExit});
        this.contextMenuStrip1.Name = "contextMenuStrip1";
        this.contextMenuStrip1.Size = new System.Drawing.Size(139, 70);
        // 
        // tsStop
        // 
        this.tsStop.Name = "tsStop";
        this.tsStop.Size = new System.Drawing.Size(138, 22);
        this.tsStop.Text = "St&op Polling";
        this.tsStop.Click += new System.EventHandler(this.stopPollingToolStripMenuItem_Click);
        // 
        // tsStart
        // 
        this.tsStart.Name = "tsStart";
        this.tsStart.Size = new System.Drawing.Size(138, 22);
        this.tsStart.Text = "St&art Polling";
        this.tsStart.Click += new System.EventHandler(this.tsStart_Click);
        // 
        // tsExit
        // 
        this.tsExit.Name = "tsExit";
        this.tsExit.Size = new System.Drawing.Size(138, 22);
        this.tsExit.Text = "E&xit";
        this.tsExit.Visible = false;
        this.tsExit.Click += new System.EventHandler(this.tsExit_Click);
        // 
        // frmMain
        // 
        this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
        this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
        this.ClientSize = new System.Drawing.Size(256, 61);
        this.Font = new System.Drawing.Font("Segoe UI", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
        this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
        this.MaximizeBox = false;
        this.MinimizeBox = false;
        this.Name = "frmMain";
        this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
        this.Text = "DCU";
        this.MinimumSizeChanged += new System.EventHandler(this.frmMain_MinimumSizeChanged);
        this.Activated += new System.EventHandler(this.frmMain_Activated);
        this.Load += new System.EventHandler(this.Form1_Load);
        this.contextMenuStrip1.ResumeLayout(false);
        this.ResumeLayout(false);

    }


}
