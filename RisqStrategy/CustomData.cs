using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MathNet.Numerics.LinearAlgebra;
using Microsoft.Extensions.Configuration;
using QLNet;
using ConfigurationBuilder = Microsoft.Extensions.Configuration.ConfigurationBuilder;


namespace RisqData
{
    public class DataSpecs
    {
        public class DataConfig
        {
            public string metadata_PATH { get; set; }

            public List<Sec> Assets { get; set; }


        }

        public class Sec
        {
            public string RollOverData_NameFile { get; set; }
            public string RollOverData_PathFile { get; set; }
            public string RIC_product { get; set; }
            public List<string> File_Location { get; set; }
            public string Symbol { get; set; }

        }

        public static string GetByName(string configKeyName)
        {
            var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

            IConfigurationSection section = config.GetSection(configKeyName);

            return section.Value;
        }

        public static DataConfig GetDataConfig()
        {

        var test = "C://Users/mjs/source/repos/Lean/RisqStrategy/";

            var config = new ConfigurationBuilder()
                .SetBasePath(test)
                .AddJsonFile("appsettings.json")
                .Build();

            var section = config.GetSection("DataConfig");
            return config.GetSection("DataConfig").Get<DataConfig>();
        }
    }
}
