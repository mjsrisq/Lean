
using QuantConnect.Securities;
using System.Collections.Generic;
using System.Linq;
using QuantConnect.Parameters;
using QuantConnect.Indicators;
using System.IO;
using QuantConnect.Data;
using System;
using QuantConnect.Interfaces;
using Future_Adjustments;
using RisqData;

namespace QuantConnect.Algorithm.CSharp
{
    public class RISQPairsCointegration : QCAlgorithm
    {

        RollingWindow<double> WindowSimpleMovingAverage;
        [Parameter("LookBackWindow")]
        int WindowSize = 10; // Set a window size
        [Parameter("deviation")]
        double actionDeviation = (double)1.8;
        [Parameter("quantity")]
        double stake = 0.1;
        public override void Initialize()
        {
            SetStartDate(2021, 01, 01); // Set start date
            SetEndDate(2022, 01, 01); // Set end date

            SetCash(10000); // Set initial cash

            WindowSimpleMovingAverage = new RollingWindow<double>(WindowSize);
            
            _ = AddData<S1>("S1", Resolution.Daily).Symbol;
            _ = AddData<S2>("S2", Resolution.Daily).Symbol;

        }

        public void OnData(Slice data)
        {

            if (data.Values.Count != this.ActiveSecurities.Count)
            {
                return;
            }
            S1 data1 = (S1)data.Values[0];
            S2 data2 = (S2)data.Values[1];
            //Log(Convert.ToString(data.Time));
            Log(Convert.ToString(data2.Time));

            double[] ratioTS = data1.Series.Zip(data2.Series, (x, y) => Convert.ToDouble(x) / Convert.ToDouble(y)).ToArray();

            for (int i = ratioTS.Length - 1; i > 0; i--)
            {
                WindowSimpleMovingAverage.Add(ratioTS[i]);
            }

            if (!WindowSimpleMovingAverage.IsReady)
            {
                return;
            }

            double avgRatio = WindowSimpleMovingAverage.Average();
            double stdRatio = (double)Math.Sqrt(WindowSimpleMovingAverage.Select(x => Math.Pow((x - avgRatio), 2)).Sum() / (WindowSimpleMovingAverage.Count - 1));

            double actionDeviation = (double)1.8;

            double p1 = Convert.ToDouble(data1.Value);
            double p2 = Convert.ToDouble(data2.Value);

            double ratioNow = p1 / p2;

            double z = (ratioNow - avgRatio) / stdRatio;
            double previous_z = 0;

            var holdings = Math.Abs(Portfolio[data1.Symbol].Quantity) + Math.Abs(Portfolio[data2.Symbol].Quantity);



            if (holdings == 0)
            {

                if (z > actionDeviation)
                {

                    {
                        SetHoldings(data1.Symbol, -1 * stake);
                        SetHoldings(data2.Symbol, 1 * stake); // set amount
                    }
                }
                else if (z < -actionDeviation)
                {
                    if (holdings == 0)
                    {
                        SetHoldings(data1.Symbol, 1 * stake);
                        SetHoldings(data2.Symbol, -1 * stake);
                    }
                }
            }
            if (holdings != 0)
            {
                if (Math.Sign(previous_z) != Math.Sign(z))
                {
                    Liquidate(data1.Symbol);
                    Liquidate(data2.Symbol);
                }
            }
            previous_z = z;
        }
    }

    public class S1 : BaseData
    {
        int header = 0;
        public string[] Series;


        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLive)
        {

            var _DataConfig = DataSpecs.GetDataConfig();

            var metadata_PATH = _DataConfig.metadata_PATH;
            var RollOverData_NameFile = _DataConfig.Assets[0].RollOverData_NameFile;
            var RollOverData_PathFile = _DataConfig.Assets[0].RollOverData_PathFile;
            var RIC_product = _DataConfig.Assets[0].RIC_product;
            var File_Location = _DataConfig.Assets[0].File_Location;

            // Create dataframe with adjusted rollovers
            // In the future this should be a call to a database, server etc. 
            DataHandle AccessCSV = new DataHandle(metadata_PATH);


            string data_path = Path.Combine(Globals.DataFolder, RollOverData_NameFile);
            var source = data_path;
            if (!File.Exists(data_path))
            {
                var ListDocuments = new List<string> { RIC_product };
                var paths = File_Location;
                var AdjustedFuturePrices = AccessCSV.LocalCSV(ListDocuments, paths);

                // Create CSV with the adjusted rollovers 
                var path = AccessCSV.RollOverDataFrame(AdjustedFuturePrices, RollOverData_PathFile);
                source = Path.Combine(Globals.DataFolder, RollOverData_NameFile);
            }
            // Return path to reader 
            return new SubscriptionDataSource(source, SubscriptionTransportMedium.LocalFile);
        }

        public override BaseData Reader(
        SubscriptionDataConfig config,
        string line,
        DateTime date,
        bool isLive)
        {

            if (string.IsNullOrWhiteSpace(line) ||
            char.IsLetter(line[0]))
                return null;

            if (header == 0)
            {
                header++;
                return null; // The first line should be the header and should not be relevant 
            }



            var data = line.Split(',');
            int index_location = data.Length - 1;

            var Date = data[index_location]; // take date from the index 
            var series = data.Take(index_location); // remove the date 
            series = series.Where(x => Convert.ToDouble(x) != 0).ToArray(); // Remove 0s from the array T
            index_location = series.Count() - 1; // Update index of the last element of the array
            string[] s = series.ToArray();

            //DateTimeFormatInfo dtfi = CultureInfo.GetCultureInfo("en-UK").DateTimeFormat;

            return new S1()
            {
                // Make sure we only get this data AFTER trading day - don't want forward bias.

                //Time = DateTime.ParseExact(data[index_location].Split(' ')[0], "o", CultureInfo.InvariantCulture,
                //                     DateTimeStyles.None),
                Time = DateTime.Parse(Date.Split(' ')[0]),
                Symbol = Symbol,
                Series = s,
                //Open = Convert.ToDecimal(data[1]),
                //High = Convert.ToDecimal(data[2]),
                //Low = Convert.ToDecimal(data[3]),
                Value = Convert.ToDecimal(data[index_location])

            };
        }

    }

    public class S2 : BaseData
    {
        int header = 0;
        public string[] Series;


        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLive)
        {

            var _DataConfig = DataSpecs.GetDataConfig();

            var metadata_PATH = _DataConfig.metadata_PATH;
            var RollOverData_NameFile = _DataConfig.Assets[1].RollOverData_NameFile;
            var RollOverData_PathFile = _DataConfig.Assets[1].RollOverData_PathFile;
            var RIC_product = _DataConfig.Assets[1].RIC_product;
            var File_Location = _DataConfig.Assets[1].File_Location;

            // Create dataframe with adjusted rollovers
            // In the future this should be a call to a database, server etc. 
            DataHandle AccessCSV = new DataHandle(metadata_PATH);


            string data_path = Path.Combine(Globals.DataFolder, RollOverData_NameFile);
            var source = data_path;
            if (!File.Exists(data_path))
            {
                var ListDocuments = new List<string> { RIC_product };
                var paths = File_Location;
                var AdjustedFuturePrices = AccessCSV.LocalCSV(ListDocuments, paths);

                // Create CSV with the adjusted rollovers 
                var path = AccessCSV.RollOverDataFrame(AdjustedFuturePrices, RollOverData_PathFile);
                source = Path.Combine(Globals.DataFolder, RollOverData_NameFile);
            }
            // Return path to reader 
            return new SubscriptionDataSource(source, SubscriptionTransportMedium.LocalFile);
        }

        public override BaseData Reader(
        SubscriptionDataConfig config,
        string line,
        DateTime date,
        bool isLive)
        {

            if (string.IsNullOrWhiteSpace(line) ||
            char.IsLetter(line[0]))
                return null;

            if (header == 0)
            {
                header++;
                return null; // The first line should be the header and should not be relevant 
            }



            var data = line.Split(',');
            int index_location = data.Length - 1;

            var Date = data[index_location]; // take date from the index 
            var series = data.Take(index_location); // remove the date 
            series = series.Where(x => Convert.ToDouble(x) != 0).ToArray(); // Remove 0s from the array T
            index_location = series.Count() - 1; // Update index of the last element of the array
            string[] s = series.ToArray();

            //DateTimeFormatInfo dtfi = CultureInfo.GetCultureInfo("en-UK").DateTimeFormat;

            return new S2()
            {
                // Make sure we only get this data AFTER trading day - don't want forward bias.

                //Time = DateTime.ParseExact(data[index_location].Split(' ')[0], "o", CultureInfo.InvariantCulture,
                //                     DateTimeStyles.None),
                Time = DateTime.Parse(Date.Split(' ')[0]),
                Symbol = Symbol,
                Series = s,
                //Open = Convert.ToDecimal(data[1]),
                //High = Convert.ToDecimal(data[2]),
                //Low = Convert.ToDecimal(data[3]),
                Value = Convert.ToDecimal(data[index_location])

            };
        }

    }
}
