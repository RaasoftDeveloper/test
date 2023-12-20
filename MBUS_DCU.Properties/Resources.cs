using System.CodeDom.Compiler;
using System.ComponentModel;
using System.Diagnostics;
using System.Drawing;
using System.Globalization;
using System.Resources;
using System.Runtime.CompilerServices;

namespace MBUS_DCU.Properties;

[GeneratedCode("System.Resources.Tools.StronglyTypedResourceBuilder", "4.0.0.0")]
[DebuggerNonUserCode]
[CompilerGenerated]     
//commit
internal class Resources
{
	private static ResourceManager resourceMan;

	private static CultureInfo resourceCulture;

	[EditorBrowsable(EditorBrowsableState.Advanced)]
	internal static ResourceManager ResourceManager
	{
		get
		{
			if (resourceMan == null)
			{
				ResourceManager resourceManager = new ResourceManager("MBUS_DCU.Properties.Resources", typeof(Resources).Assembly);
				resourceMan = resourceManager;
			}
			return resourceMan;
		}
	}

	[EditorBrowsable(EditorBrowsableState.Advanced)]
	internal static CultureInfo Culture
	{
		get
		{
			return resourceCulture;
		}
		set
		{
			resourceCulture = value;
		}
	}

	internal static Icon Comm
	{
		get
		{
			object @object = ResourceManager.GetObject("Comm", resourceCulture);
			return (Icon)@object;
		}
	}

	internal static Icon Green
	{
		get
		{
			object @object = ResourceManager.GetObject("Green", resourceCulture);
			return (Icon)@object;
		}
	}

	internal static Icon Grey
	{
		get
		{
			object @object = ResourceManager.GetObject("Grey", resourceCulture);
			return (Icon)@object;
		}
	}

	internal static Icon Red
	{
		get
		{
			object @object = ResourceManager.GetObject("Red", resourceCulture);
			return (Icon)@object;
		}
	}

	internal Resources()
	{
	}
}
