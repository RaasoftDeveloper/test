using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;

namespace MBUS_DCU;

internal static class Program
{
	[STAThread]
	private static void Main()
	{
		string text = Environment.CurrentDirectory + "\\LOGS\\DCU";
		Application.EnableVisualStyles();
		Application.SetCompatibleTextRenderingDefault(defaultValue: false);
		DBSettings();
		if (!checkSingleInstance())
		{
			Application.Run(new frmMain());
		}
	}

	private static bool checkSingleInstance()
	{
		string processName = Process.GetCurrentProcess().ProcessName;
		Process[] processesByName = Process.GetProcessesByName(processName);
		if (processesByName.Length > 1)
		{
			return true;
		}
		return false;
	}

	private static void DBSettings()
	{
		OpenDBSettingsConfig();
		string connectionString = ("Data Source = " + GlobalDeclaration._SQLServerName + "; Initial Catalog = " + GlobalDeclaration._SQLDBName + "; Integrated Security = False; User ID = " + GlobalDeclaration._SQLUName + "; Password = " + GlobalDeclaration._SQLPwd) ?? "";
		GlobalDeclaration._ASIMBUSSETTINGSCnx = new SqlConnection(connectionString);
		if (GlobalDeclaration._ASIMBUSSETTINGSCnx.State == ConnectionState.Closed)
		{
			GlobalDeclaration._ASIMBUSSETTINGSCnx.Open();
		}
		string connectionString2 = ("Data Source = " + GlobalDeclaration._SQLServerName + "; Initial Catalog = " + GlobalDeclaration._SQLClientDBName + "; Integrated Security = False; User ID = " + GlobalDeclaration._SQLUName + "; Password = " + GlobalDeclaration._SQLPwd) ?? "";
		GlobalDeclaration._ASIMBUSDATA = new SqlConnection(connectionString2);
		if (GlobalDeclaration._ASIMBUSDATA.State == ConnectionState.Closed)
		{
			GlobalDeclaration._ASIMBUSDATA.Open();
		}
	}

	private static void OpenDBSettingsConfig()
	{
		try
		{
			string path = AppDomain.CurrentDomain.BaseDirectory + "\\DBSettings.txt";
			string text = "";
			int num = 0;
			if (File.Exists(path))
			{
				StreamReader streamReader = new StreamReader(path);
				for (text = streamReader.ReadLine(); text != null; text = streamReader.ReadLine())
				{
					num++;
					if (text == null)
					{
						break;
					}
					switch (num)
					{
					case 1:
						GlobalDeclaration._SQLServerName = text;
						break;
					case 2:
						GlobalDeclaration._SQLPwd = text;
						break;
					case 3:
						GlobalDeclaration._SQLDBName = text;
						break;
					case 4:
						GlobalDeclaration._SQLUName = text;
						break;
					case 5:
						GlobalDeclaration._SQLClientDBName = text;
						break;
					case 6:
						if (text != "")
						{
							GlobalDeclaration.ErrorTest = true;
						}
						break;
					}
				}
				streamReader.Close();
			}
			else
			{
				MessageBox.Show("Database Config Not Found..!", "Warning", MessageBoxButtons.OK, MessageBoxIcon.Hand);
			}
		}
		catch (Exception ex)
		{
			MessageBox.Show(ex.ToString());
		}
	}
}
