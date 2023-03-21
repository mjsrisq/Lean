using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using CsvHelper.Configuration;

namespace Future_Adjustments{
    public static class CsvReadWrite
    {
        public static List<T> CsvImport<T>(string path)
        {
            try
            {
                Stream stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using (var reader = new StreamReader(stream))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    return csv.GetRecords<T>().ToList();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }

        public static T RetriveLastCsvEntry<T>(string path) where T : class
        {
            if (!File.Exists(path))
            {
                return null;
            }

            var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using (var reader = new StreamReader(fs))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                var records = csv.GetRecords<T>();
                object lastEntry;

                try
                {
                    lastEntry = records.LastOrDefault();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"No history{ ex.Message}");
                    lastEntry = null;
                }

                var last = (T)lastEntry;

                if (last != null)
                {
                    return last;
                }

                return null;
            }
        }

        public static void SaveNewDataCsv<T1, TMap>(List<T1> records, string filePath) where TMap : ClassMap
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false, //Don't write the header again.
            };

            bool append = File.Exists(filePath);
            config.HasHeaderRecord = !append;

            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(filePath));
                using (var stream = File.Open(filePath, FileMode.Append))
                using (var writer = new StreamWriter(stream))
                using (var csv = new CsvWriter(writer, config))
                {
                    csv.Context.RegisterClassMap<TMap>();
                    csv.WriteRecords(records);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                // throw;
            }
        }
    }

    public static class ContractTimeSeriesData
    {
        public class ContractData
        {
            public DateTime TimeStamp { get; set; }
            public double Volume { get; set; }
        }

        public class TickData : ContractData
        {
            public double? Value { get; set; }
        }

        public class BarData : ContractData
        {
            public double? Open { get; set; }
            public double? High { get; set; }
            public double? Low { get; set; }
            public double? Close { get; set; }
        }

        public sealed class TickDataMap : ClassMap<TickData>
        {
            public TickDataMap()
            {
                Map(m => m.TimeStamp).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Value);
                Map(m => m.Volume);
            }
        }

        public sealed class BarDataMap : ClassMap<BarData>
        {
            public BarDataMap()
            {
                Map(m => m.TimeStamp).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Open);
                Map(m => m.High);
                Map(m => m.Low);
                Map(m => m.Close);
                Map(m => m.Volume);
            }
        }
    }

    public static class EikonRicTrade
    {
         public class RicDataTrade
        {
            public DateTime TimeStamp { get; set; }
            public double Volume { get; set; }
        }

        public class TickDataTrade : RicDataTrade
        {
            public double? Value { get; set; }
        }

        public class BarDataTrade : RicDataTrade
        {
            public double? Open { get; set; }
            public double? High { get; set; }
            public double? Low { get; set; }
            public double? Close { get; set; }
        }

        public sealed class TickDataTradeMap : ClassMap<TickDataTrade>
        {
            public TickDataTradeMap()
            {
                Map(m => m.TimeStamp).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Value);
                Map(m => m.Volume);
            }
        }

        public sealed class BarDataTradeMap : ClassMap<BarDataTrade>
        {
            public BarDataTradeMap()
            {
                Map(m => m.TimeStamp).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Open);
                Map(m => m.High);
                Map(m => m.Low);
                Map(m => m.Close);
                Map(m => m.Volume);
            }
        }

        /// <summary>
        /// Eikon API Ric scrape with resolution of view last, trade etc.
        /// </summary>        
        public class RicMetaDataTrade
        {
            public string Ric { get; set; }
            public string View { get; set; }
            public string Interval { get; set; }
            public DateTime TradeDateFromUtc { get; set; }
            public int DataPoints { get; set; }
            public DateTime RunFromLocal { get; set; }
            public DateTime RunToLocal { get; set; }
            public TimeSpan RunFreq { get; set; }
            public string Notes { get; set; }
            public string Scrape { get;set;}
            public string SaveDir { get; set; }
            public string Zone { get; set; }
            public string Period { get; set; }
        }

        public class RicLogger
        {
            public string Ric { get; set; }
            public string View { get; set; }
            public string Interval { get; set; }
            public DateTime TimeStamp { get; set; }
            public DateTime LastScrape { get; set; }
            public DateTime LastEntry { get; set; }
        }

        public sealed class RicLoggerMap : ClassMap<RicLogger>
        {
            public RicLoggerMap()
            {
                Map(m => m.Ric);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.View);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Interval);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.TimeStamp).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.LastScrape).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.LastEntry).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
            }
        }
    }

    public static class EikonRicView
    {
        public class RicDataView
        {
            public DateTime Date { get; set; }
        }

        public class TickDataView : RicDataView
        {
            public double Value { get; set; }
            public int Count { get; set; }
        }

        public class BarDataView : RicDataView
        {
            public double Open { get; set; }
            public double High { get; set; }
            public double Low { get; set; }
            public double Close { get; set; }
        }

        public sealed class TickDataViewMap : ClassMap<TickDataView>
        {
            public TickDataViewMap()
            {
                Map(m => m.Date).TypeConverterOption.Format("o");// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Value);
                Map(m => m.Count);
            }
        }

        public sealed class BarDataMap : ClassMap<BarDataView>
        {
            public BarDataMap()
            {
                Map(m => m.Date).TypeConverterOption.Format("o");//yyyy - MM - dd HH: mm:ss.fffffffK");
                Map(m => m.Open);
                Map(m => m.High);
                Map(m => m.Low);
                Map(m => m.Close);
            }
        }

        /// <summary>
        /// Eikon API Ric scrape with resolution of view last, trade etc.
        /// </summary>        
        public class RicMetaDataView
        {
            public string Ric { get; set; }
            public string View { get; set; }
            public string Interval { get; set; }
            public DateTime TradeDateFromUtc { get; set; }
            public int DataPoints { get; set; }
            public DateTime RunFromLocal { get; set; }
            public DateTime RunToLocal { get; set; }
            public TimeSpan RunFreq { get; set; }
            public string Notes { get; set; }
            public string Scrape { get; set; }
            public string SaveDir { get; set; }
            public string Zone { get; set; }
            public string Period { get; set; }
        }
    }
}

    