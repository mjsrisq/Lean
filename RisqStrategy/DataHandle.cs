
using static Future_Adjustments.ContractData;
using static Future_Adjustments.ContractMethods;
using System.Configuration;
using Microsoft.Data.Analysis;
using System.Collections.Generic;
using System;
using System.Collections;
using System.Linq;
using System.Collections.Specialized;


namespace Future_Adjustments
{

    public class DataHandle
    {
	
	    public string metaDataPath{get; set;}
	    string dateCol; // path where meta data is stored 

	    public DataHandle(string path)
	    {
		    /* We want to make a constructor that changes the file of the App.config to access the path in which 
		    we have the metadata. To do so we want to change the App.config file in runtime. 
		    This is achieved in the followig way.  */

		    // Note that we need to instantiate the object with a string called metaDataPath which contains the path of the metadata 

		    var x = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
		    var connectionStringsSection = (ConnectionStringsSection)x.GetSection("connectionStrings");
		    var y = connectionStringsSection.ConnectionStrings["cntMetaDataDebug"];
		    connectionStringsSection.ConnectionStrings["cntMetaDataProd"].ConnectionString = metaDataPath; // Set the metapath as the connection string 
		    x.Save();
		    ConfigurationManager.RefreshSection("connectionStrings");

		    // The connection string is suppose to give us access to the metadata file where we have a record of the rest of the files that we want to obtain
		    // and their paths. 
		
	    }

	    public Dictionary<CntMetaData, Dictionary<DateTime, DataFrame>> LocalCSV(List<string> ListFilePaths, List<string> paths)
	    {
		
		    var stratConfigClass = ConfigHelper.GetStratConfig();

		    var dateCol = stratConfigClass.BarColName;// "TimeStamp";
		    var priceCol = stratConfigClass.CloseName;// "Close";
		    var barCalbHistory = stratConfigClass.BarCalbHistory;// 30;
		    int rolloverBuffer = stratConfigClass.RollOverBuffer;// 2;
		    var barInterval = stratConfigClass.Interval;// ContractData.Interval.daily;
		    var tdFrom = stratConfigClass.BarFrom;// new DateTime(2021, 01, 01);
		    var tdTo = stratConfigClass.BarTo;// new DateTime(2022, 01, 01);
		    var barFrom = tdFrom.AddBar(-barCalbHistory, barInterval);
		    bool intercept = stratConfigClass.Intercept;// true;
		    double entryBuffer = stratConfigClass.EntryBuffer;// 0.1;
		    string[] ohlCols = new string[] { stratConfigClass.OpenName, stratConfigClass.HighName, // What this do ?
											    stratConfigClass .LowName};
											
		    // This section is needed for some of the metadata but the paths can not be accessed so need to pass them locally 
		    // List of paths should pass sorted the files that we want to use. There should be two for each future contract
		    // The current one and the one ahead  
		
		    List<CntMetaData>? cntMeta = new List<CntMetaData>();
		    var ricMetaPath = ConfigurationManager.ConnectionStrings["cntMetaDataDebug"].ConnectionString;
		    foreach(var path in ListFilePaths)
		    {
			    var _file = CsvReadWrite.CsvImport<CntMetaData>(ricMetaPath).Where(x => x.Path.Contains(path)).ToList()[0]; // match the path names with the name of the file 
			    cntMeta.Add(_file);

			    var index_of_file = CsvReadWrite.CsvImport<CntMetaData>(ricMetaPath).Where(x => x.Path.Contains(path)).ToList()[0].CntNo;
			    var parent_file = CsvReadWrite.CsvImport<CntMetaData>(ricMetaPath).Where(x => x.Path.Contains(path)).ToList()[0].CntParent;

			    var _AheadFile = CsvReadWrite.CsvImport<CntMetaData>(ricMetaPath).Where(x => x.CntNo == index_of_file+1 && x.CntParent == parent_file).ToList()[0];
			    cntMeta.Add(_AheadFile);

		    }

		    // What we get in cntMeta is a list of paths that are accesible for Miles but Linux we cannot access the drive like that so the paths are useless
		    // We simulate this with a list of paths to the files 

        
		    //var cntMeta = CsvReadWrite.CsvImport<CntMetaData>(ricMetaPath).Where(x => x.Include == "Y").ToList();
		    // TODO: Integrate this to AWS
		
		    // Local files, accessed in sorted list 
		
		    var cntDfs = ContractMethods.RetriveCntTimeSeriesData(cntMeta, true, paths); // We have changed the path so it uses the local one
            var cntData = cntDfs.Select(x => x.Key).ToList();
            var globalBars = DataFrameHelper.CreateTradeDateCurve("TimeStamp", barFrom, tdTo, barInterval);
            var globalBarsCol = globalBars[dateCol];
            var gloablBarsIndex = DataFrameHelper.DataFrameIndexTable<DateTime>(globalBars, dateCol);
            var trdBars = DataFrameHelper.CreateTradeDateCurve("TimeStamp", tdFrom, tdTo, barInterval);
		    var trdBarsCol = trdBars[dateCol];
            var dfDefault = DataFrameHelper.TimeSeriesDefaultDataFrame(globalBars, dateCol, priceCol);
		    var cntAheads = ContractMethods.GetContractsNextContract(cntMeta);

		    //Group Timeseries by Bar
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    cntDfs[cnt] = DataFrameHelper.TimeSeriesCleanTimeStamp(df, dateCol, priceCol, cnt.Interval);
		    }
		    //Get Fixed Contract Meta Data
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    cntDfs[cnt] = DataFrameHelper.TimeSeriesDataFramesConvertToGlobalDates(df, globalBars, dateCol);
		    }
		    //Rollover Dates
		    var rollovers = new Dictionary<CntMetaData, Dictionary<DateTime, DateTime>>();
		    foreach (var cnt in cntData)
		    {
			    var rollover = ContractMethods.GetNextRollOverDates(barFrom, tdTo, cnt);
			    rollovers[cnt] = rollover;
		    }

		    //Clean Close Data
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    var rollover = rollovers[cnt];
			    cntDfs[cnt] = DataFrameHelper.BackFillContinuousContract(rollover, df, priceCol, dateCol, rolloverBuffer);
		    }
		    //Fill OHLC gaps with Close
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    var rollover = rollovers[cnt];
			    cntDfs[cnt] = DataFrameHelper.CrossFillClosePrice(df, priceCol, ohlCols, dateCol);
		    }

		    //Continous Futures backward ratio calc
		    var deltas = new Dictionary<CntMetaData, DataFrame>();
		    foreach (var cnt in cntData)
		    {
			    deltas[cnt] = DataFrameHelper.FuturesAdjustedRatioCalc(dfDefault, cnt, cntAheads, cntDfs, dateCol,
			    priceCol, rollovers, rolloverBuffer);
		    }
		    //Calc historical continous abs price for each TD.
		    var cntAdjustedFutures = new Dictionary<CntMetaData, Dictionary<DateTime, DataFrame>>();
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    var delta = deltas[cnt];

			    cntAdjustedFutures.Add(cnt, DataFrameHelper.FuturesAdjustedPriceCalcBarRange(globalBarsCol, trdBarsCol, priceCol, gloablBarsIndex, df, delta));
		    }

		

		    return cntAdjustedFutures;

		
	    }

	    public Dictionary<CntMetaData, Dictionary<DateTime, DataFrame>> GetCsv(List<string> LocalFilePaths = null)
        {	
		    /*We need a file called App.config that contains the informatio information about some variables. 
		    Without this file the program will not run.*/

		    // ConfigHelper helps us access and set the information from appconfig.json

		    //var setting = ConfigHelper.GetByName("StratConfig");
		    var stratConfigClass = ConfigHelper.GetStratConfig();

		    var dateCol = stratConfigClass.BarColName;// "TimeStamp";
		    var priceCol = stratConfigClass.CloseName;// "Close";
		    var barCalbHistory = stratConfigClass.BarCalbHistory;// 30;
		    int rolloverBuffer = stratConfigClass.RollOverBuffer;// 2;
		    var barInterval = stratConfigClass.Interval;// ContractData.Interval.daily;
		    var tdFrom = stratConfigClass.BarFrom;// new DateTime(2021, 01, 01);
		    var tdTo = stratConfigClass.BarTo;// new DateTime(2022, 01, 01);
		    var barFrom = tdFrom.AddBar(-barCalbHistory, barInterval);//
		    bool intercept = stratConfigClass.Intercept;// true;
		    double entryBuffer = stratConfigClass.EntryBuffer;// 0.1;
		    string[] ohlCols = new string[] { stratConfigClass.OpenName, stratConfigClass.HighName, // What this do ?
											    stratConfigClass .LowName};
		
		    // Now that we have the information set we can request those files that we are interested in.
		    // In this test example we request those files that are market with a Y in the column include.
		    // This condition should be changed to search for the name, RIC of the data that we want to use. 
		
		    var ricMetaPath = ConfigurationManager.ConnectionStrings["cntMetaDataDebug"].ConnectionString;
            var cntMeta = CsvReadWrite.CsvImport<CntMetaData>(ricMetaPath).Where(x => x.Include == "Y").ToList();
		    // TODO: Integrate this to AWS
            var cntDfs = ContractMethods.RetriveCntTimeSeriesData(cntMeta, true, LocalFilePaths); // We have changed the path so it uses the local one
            var cntData = cntDfs.Select(x => x.Key).ToList();
            var globalBars = DataFrameHelper.CreateTradeDateCurve("TimeStamp", barFrom, tdTo, barInterval);
            var globalBarsCol = globalBars[dateCol];
            var gloablBarsIndex = DataFrameHelper.DataFrameIndexTable<DateTime>(globalBars, dateCol);
            var trdBars = DataFrameHelper.CreateTradeDateCurve("TimeStamp", tdFrom, tdTo, barInterval);
		    var trdBarsCol = trdBars[dateCol];
            var dfDefault = DataFrameHelper.TimeSeriesDefaultDataFrame(globalBars, dateCol, priceCol);
		    var cntAheads = ContractMethods.GetContractsNextContract(cntMeta);

            //Group Timeseries by Bar
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    cntDfs[cnt] = DataFrameHelper.TimeSeriesCleanTimeStamp(df, dateCol, priceCol, cnt.Interval);
		    }
		    //Get Fixed Contract Meta Data
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    cntDfs[cnt] = DataFrameHelper.TimeSeriesDataFramesConvertToGlobalDates(df, globalBars, dateCol);
		    }
		    //Rollover Dates
		    var rollovers = new Dictionary<CntMetaData, Dictionary<DateTime, DateTime>>();
		    foreach (var cnt in cntData)
		    {
			    var rollover = ContractMethods.GetNextRollOverDates(barFrom, tdTo, cnt);
			    rollovers[cnt] = rollover;
		    }

		    //Clean Close Data
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    var rollover = rollovers[cnt];
			    cntDfs[cnt] = DataFrameHelper.BackFillContinuousContract(rollover, df, priceCol, dateCol, rolloverBuffer);
		    }
		    //Fill OHLC gaps with Close
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    var rollover = rollovers[cnt];
			    cntDfs[cnt] = DataFrameHelper.CrossFillClosePrice(df, priceCol, ohlCols, dateCol);
		    }

		    //Continous Futures backward ratio calc
		    var deltas = new Dictionary<CntMetaData, DataFrame>();
		    foreach (var cnt in cntData)
		    {
			    deltas[cnt] = DataFrameHelper.FuturesAdjustedRatioCalc(dfDefault, cnt, cntAheads, cntDfs, dateCol,
			    priceCol, rollovers, rolloverBuffer);
		    }
		    //Calc historical continous abs price for each TD.
		    var cntAdjustedFutures = new Dictionary<CntMetaData, Dictionary<DateTime, DataFrame>>();
		    foreach (var cnt in cntData)
		    {
			    var df = cntDfs[cnt];
			    var delta = deltas[cnt];

			    cntAdjustedFutures.Add(cnt, DataFrameHelper.FuturesAdjustedPriceCalcBarRange(globalBarsCol, trdBarsCol, priceCol, gloablBarsIndex, df, delta));
		    }

		

		    return cntAdjustedFutures;
        }

	    public string RollOverDataFrame(Dictionary<CntMetaData, Dictionary<DateTime, DataFrame>> cntAdjustedFutures, string CsvPath)
	    {
		    var RolledOverAsset = cntAdjustedFutures.ElementAt(0).Value;
		    var TimeSeriesDatesColumns = RolledOverAsset.ElementAt(0).Value.Columns[0];
		    PrimitiveDataFrameColumn<DateTime> IndexColumn = new PrimitiveDataFrameColumn<DateTime>("Index");
		    List<PrimitiveDataFrameColumn<double>> ListTimeSeriesColumns = new List<PrimitiveDataFrameColumn<double>>();
		    for(int i = 0; i < (int)TimeSeriesDatesColumns.Length; i++)
		    {
			    PrimitiveDataFrameColumn<double> DataColumn = new PrimitiveDataFrameColumn<double>(TimeSeriesDatesColumns[i].ToString());
			    foreach(KeyValuePair<DateTime, DataFrame> TimeSeries in RolledOverAsset)
			    {	
				    if(i == 0)
				    {
					    var x = TimeSeries.Key;
					    IndexColumn.Append(Convert.ToDateTime(x)); //DateTime
				    }
				    var y = TimeSeries.Value.Columns[1][i];
				    DataColumn.Append(Convert.ToDouble(y)); //DataFrame
				
			    }
			    ListTimeSeriesColumns.Add(DataColumn);
		    }
		    DataFrame df = new DataFrame(ListTimeSeriesColumns);
		    df.Columns.Add(IndexColumn); // Index column

		    char sep = char.Parse(",");

		    DataFrame.WriteCsv(df, CsvPath, sep, true);

		    return CsvPath;
	    }
    }
}
