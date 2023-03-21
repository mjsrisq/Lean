using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Configuration;
using ConfigurationBuilder = Microsoft.Extensions.Configuration.ConfigurationBuilder;

namespace Future_Adjustments
    {
    public static class ConfigHelper
    {
		public class StratConfig
		{
			public string ConnectionString { get; set; }
			public string BarColName { get; set; }
			public string CloseName { get; set; }
			public string OpenName { get; set; }
			public string HighName { get; set; }
			public string LowName { get; set; }
			public int BarCalbHistory { get; set; }
			public int RollOverBuffer { get; set; }
			public DateTime BarFrom { get; set; }
			public DateTime BarTo { get; set; }
			public bool Intercept { get; set; }
			public double EntryBuffer { get; set; }
			public ContractData.Interval Interval {get;set;}
			public List<string> EntryList { get; set; }
		}

		public static string GetByName(string configKeyName)
        {
            var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

            IConfigurationSection section = config.GetSection(configKeyName);

            return section.Value;
        }

		public static StratConfig GetStratConfig()
		{
			var config = new ConfigurationBuilder()
				.SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			var section = config.GetSection(nameof(StratConfig));
			return section.Get<StratConfig>();
		}
    }
}
