
using System.Globalization;
using Microsoft.Data.Analysis;
using static Future_Adjustments.ContractData;
using System.Collections.Generic;
using System;
using System.Collections;
using System.Linq;


namespace Future_Adjustments{

    public static class DataFrameHelper
    {
        public static void AddValueColIfMissing (DataFrame df , List<string> colNames, string colName, int rowCount) 
        {
            if (colNames.Contains(colName))
            {
                df.Columns.Add(new PrimitiveDataFrameColumn<double>(colName, rowCount));
            }
        }

        public static DataFrame LoadCsv(string fileName, int rowsFromSt)
        {
            var colNames = new string[] { "TimeStamp", "Open", "High", "Low", "Close", "Volume" };
            var dataTypes = new Type[] { typeof(DateTime), typeof(double), typeof(double), typeof(double), typeof(double), typeof(double) };

            return DataFrame.LoadCsv(fileName, ',', true, colNames, dataTypes, rowsFromSt);
        }

        //public static DataFrame DataFrameSubSet

        public static DataFrame CreateTradeDateCurve(string dateCol, DateTime sd, DateTime ed, 
                    ContractData.Interval interval)
        {
			switch (interval)
			{
				case ContractData.Interval.intraday1Minute:
                    throw new NotImplementedException();
				case ContractData.Interval.hour:
                    throw new NotImplementedException();
                case ContractData.Interval.daily:
                    var date = new PrimitiveDataFrameColumn<DateTime>(dateCol,
                 Enumerable.Range(0, (int)ed.Subtract(sd).TotalDays + 1).Select(offset => sd.AddDays(offset)).ToList());
                    return new DataFrame(date);
				case ContractData.Interval.weekly:
                    throw new NotImplementedException();
                case ContractData.Interval.monthly:
                    throw new NotImplementedException();
                case ContractData.Interval.quaterly:
                    throw new NotImplementedException();
                case ContractData.Interval.yearly:
                    throw new NotImplementedException();
                default:
                    throw new NotImplementedException(interval.ToString());
            }
        }

        public static DataFrame TimeSeriesDefaultDataFrame(DataFrame df, string dateCol, string priceCol)
		{
            var dfDate = df[dateCol].Clone();
            var dfPrice = new PrimitiveDataFrameColumn<double>(priceCol, dfDate.Length);
            return new DataFrame(dfDate, dfPrice);
		}

        public static DataFrame TimeSeriesDefaultDataFrame(DataFrameColumn dfCol, string priceCol)
        {
            var dfDate = dfCol.Clone();
            var dfPrice = new PrimitiveDataFrameColumn<double>(priceCol, dfDate.Length);
            return new DataFrame(dfDate, dfPrice);
        }

        /// <summary>
        /// Match the bar resolution to the the Interval required. Group and take last price.
        /// </summary>
        public static DataFrame TimeSeriesCleanTimeStamp(DataFrame dF, string dateCol, string priceCol, Interval? barInterval)
        {
            if (dF == null) return null;
            var dfDateCol = dF[dateCol].Clone();
            var dfPriceCol = dF[priceCol];
            var dfNew = dF.Clone();

			for (int i = 0; i < dF.Rows.Count; i++)
			{
                var dt = (DateTime)dF[dateCol][i];

				switch (barInterval)
				{
					case Interval.tick:
                        throw new NotImplementedException();
                    case Interval.intraday1Minute:
                        throw new NotImplementedException();
                    case Interval.hour:
                        throw new NotImplementedException();
                    case Interval.daily:
                        dt = new DateTime(dt.Date.Ticks);
						break;
					case Interval.weekly:
                        throw new NotImplementedException();
                    case Interval.monthly:
                        throw new NotImplementedException();
                    case Interval.quaterly:
                        throw new NotImplementedException();
                    case Interval.yearly:
                        throw new NotImplementedException();
                }
                dfNew[dateCol][i] = dt;
			}
            //var newDf = new DataFrame(dfDateCol, dfPriceCol);

		    var dfTimeStampClean = dfNew.GroupBy(dateCol).First().OrderBy(dateCol); //Dups

            return dfTimeStampClean;
			//return globalDates.Merge<DateTime>(dF, dateCol, dateCol, leftSuffix: "", joinAlgorithm: JoinAlgorithm.Left);
		}

		public static DataFrame TimeSeriesDataFramesConvertToGlobalDates(DataFrame dF, DataFrame globalDates, string dateCol)
        {
            if (dF == null) return null; //todo create a null data frame 
            dF = dF.GroupBy(dateCol).First().OrderBy(dateCol); //Dups


            return globalDates.Merge<DateTime>(dF, dateCol, dateCol, leftSuffix: "", joinAlgorithm: JoinAlgorithm.Left);
        }

        /// <summary>
        /// Merge a continous Trade Date with copy forward of missing values.
        /// </summary>
        public static DataFrame CleanTimeSeriesDataFrames(DataFrame dF, DataFrame globalDates, string dateCol, 
                    string valueCol, bool backFill)
        {
            if (dF == null) return null;
            dF = dF.GroupBy(dateCol).First().OrderBy(dateCol);
		    var res = globalDates.Merge<DateTime>(dF, dateCol, dateCol, joinAlgorithm: JoinAlgorithm.Left);

            if (!backFill) return res;
            for (int i = 1; i < res[valueCol].Length; i++)
            {
                if (res[valueCol][i] == null)
                {
                    res[valueCol][i] = res[valueCol][i - 1];
                }
            }
            return res;
        }

        public static DataFrame BackFillContinuousContract(Dictionary<DateTime, DateTime> rollOvers, DataFrame dF, 
                        string valueCol, string dateCol, int rolloverBuffer)
        {
            if (dF == null) return null;

            for (int i = 1; i < dF[valueCol].Length; i++)
            {
                if (dF[valueCol][i] == null && dF[valueCol][i - 1] != null)
                {
                    var td = (DateTime)dF[dateCol][i];
                    var ro = rollOvers[td];
                    var tdPrev = (DateTime)dF[dateCol][i -1];
                    var roPrev = rollOvers[tdPrev];
                    var roLast = rollOvers.Last(x => x.Value < ro).Value;

                    if (ContractMethods.RolloverZone(ro, roLast, td, rolloverBuffer))
					{
                        dF[valueCol][i] = null;
                    }
					else
					{
                        dF[valueCol][i] = dF[valueCol][i - 1];
                    }
                }
            }
            return dF;
        }

        /// <summary>
        /// If any of OHL is null then covert 
        /// </summary>
		public static DataFrame CrossFillClosePrice(DataFrame dF, string priceCol, string[] nullCols, string dateCol)
		{
			if (dF == null) return null;

			for (int i = 1; i < dF[dateCol].Length; i++)
			{
                //Check all values exist to be backfilled
                var price = dF[priceCol][i];
                if (price == null) continue;

                bool fill = false;
				for (int j = 0; j < nullCols.Length; j++)
				{
                    var colName = nullCols[j];  
                    if (dF[colName][i] == null)
					{
                        fill = true;
                        continue;
					}
                }

                if (!fill) continue;

                for (int j = 0; j < nullCols.Length; j++)
                {
                    var colName = nullCols[j];
                    dF[colName][i] = dF[priceCol][i - 1];
                }
            }
			return dF;
		}


		private static CultureInfo GetInvariantCulture()
		{
			return CultureInfo.InvariantCulture;
		}

        /// <summary>
        /// Converts continuous contract to the fixed contract by trade date.
        /// </summary>
        public static Dictionary<string, DataFrame> ConvertContinousToFixedContract(Dictionary<CntMetaData, 
                            DataFrame> dfs, string dateCol, string valueCol, 
                        Dictionary<CntMetaData, Dictionary<DateTime, DateTime>> tdRollovers)
	    {
            //Default dataframe

            var dfCol = new PrimitiveDataFrameColumn<DateTime>(dateCol);
            var dfValue = new PrimitiveDataFrameColumn<double>(valueCol);
            var dfDefault = Tuple.Create(dfCol, dfValue);// new Tuple<PrimitiveDataFrameColumn, PrimitiveDataFrameColumn>;//> new DataFrame(new DataFrameColumn[] { dfCol, dfValue });
            var fixedCnts = new Dictionary<string, Tuple<PrimitiveDataFrameColumn<DateTime>, PrimitiveDataFrameColumn<double>>>();

            foreach (var kvp in dfs)
			{
                var cnt = kvp.Key;
                if (cnt.CntExpiry != CntExpiry.Continous) continue;

                var df = kvp.Value;
                //Each TD refers to a fixed contract, 

                for (int i = 1; i < df[dateCol].Length; i++)
                {
                    double val;
                  //  val = (float)df[valueCol][i];

                    if (df[valueCol] == null || df[valueCol][i] == null)
                    { 
                        val = double.NaN;
                    }
                    else 
			        {
                        val = (double)(float)df[valueCol][i];

                    }

                    var td = (DateTime)df[dateCol][i];
                    var ro = tdRollovers[cnt][td];
                    var sd = ContractMethods.StartDateByCntType(cnt.CntPeriod, cnt.CntNo, ro, cnt.RolloverExpiryType);
                    var fixedName = ContractMethods.ContractFixedName(cnt, sd);

					if (!fixedCnts.TryGetValue(fixedName, 
                        out Tuple<PrimitiveDataFrameColumn<DateTime>, PrimitiveDataFrameColumn<double>> dfFixed))
					{
						dfFixed = dfDefault;
						fixedCnts[fixedName] = dfFixed;
					}

					dfFixed.Item1.Append(td); 
                    dfFixed.Item2.Append(val);
                }
			}

            //Convert to DF
            var res = new Dictionary<string, DataFrame>();
            foreach (var kvp in fixedCnts)
            {
                var df = new DataFrame(new DataFrameColumn[] { kvp.Value.Item1, kvp.Value.Item2 });
                res[kvp.Key] = df;
            }
            return res;
		}

        public static Dictionary<CntMetaData, DataFrame> ContinousFutureConversionDelta(Dictionary<CntMetaData, CntMetaData> futureAhead, 
                                    Dictionary<CntMetaData, DataFrame> dfs, string dateCol, string valueCol,
                            Dictionary<CntMetaData, Dictionary<DateTime, DateTime>> tdRollovers, int rolloverBuffer = 1)
        {
            //Default dataframe
            var dfCol = new PrimitiveDataFrameColumn<DateTime>(dateCol);
            var dfValue = new PrimitiveDataFrameColumn<double>(valueCol);
            var dfDefault = Tuple.Create(dfCol, dfValue);
            var fixedCnts = new Dictionary<CntMetaData, Tuple<PrimitiveDataFrameColumn<DateTime>, PrimitiveDataFrameColumn<double>>>();


            foreach (var kvp in dfs)
            {

                var cnt = kvp.Key;
                if (cnt.CntExpiry != CntExpiry.Continous) continue;
                //Get Next Cnt Ahead for the correct 
                var dfsAhead = dfs.FirstOrDefault(x => x.Key.CntParent == cnt.CntParent && x.Key.CntNo == cnt.CntNo + 1);
                if (dfsAhead.IsDefault()) continue;

                //Combine DF for Cont,  Cont Ahead, Delta Results.

                var cntAhead = dfsAhead.Key;
                var df = kvp.Value;
                var dfNext = dfsAhead.Value;
                //Each TD refers to a fixed contract, 

                DateTime tdCntAhead;
                DateTime td;
                DateTime tdPrev = DateTime.MinValue;
                DateTime tdPrevCntAhead = DateTime.MinValue;

                double val;
                double valCntAhead;
                double valPrev = double.NaN;
                double valCntAheadPrev  = double.NaN;
                double delta;
                DateTime roPrev = DateTime.MinValue;
                //Need to know what the previous RO date was for reference.
                td = (DateTime)df[dateCol][1];
                var rollOvers = tdRollovers[cnt];
                var ro = rollOvers[td];

                var roLast = rollOvers.Last(x => x.Value < ro).Value;


                for (int i = 0; i < df[dateCol].Length; i++)
                {
                    //Both contracts need to have data, incase of the next rollover will require the next contract prev day
                    td = (DateTime)df[dateCol][i];
                    tdCntAhead = (DateTime)dfNext[dateCol][i];

                    ////CHECKS/////////////////
                    if (td != tdCntAhead)
					{
                        throw new Exception($"TDs do not match for all contracts {cnt.Cnt} and {dfsAhead.Key}");
					}
                    
                    ro = tdRollovers[cnt][td];

                    //Rollover could be inaccurate, so with the buffer we ignore values around the rollover period
                    if (td > ro.AddDays(-rolloverBuffer) || td < roLast.AddDays(rolloverBuffer))
                    {
                        continue;
                    }

                    //We skip any invalid values
                    if (df[valueCol] == null || df[valueCol][i] == null ||
                        dfNext[valueCol] == null || dfNext[valueCol][i] == null)
                    {
                        continue;
                    }

                    ///////////////
                    val = (float)df[valueCol][i];
                    valCntAhead = (float)dfNext[valueCol][i];

                    //On fist value we need a previous value to compare to
                    if (roPrev >= roLast)
                    {
                        if (ro != roPrev) // we had a rollover since the last valid price, so use the contract ahead previous price
                        {
                            delta = val - valCntAheadPrev;
                        }
                        else
                        {
                            delta = val - valPrev;
                        }

                        //Add delta to the results
                        if (!fixedCnts.TryGetValue(cnt,
                            out Tuple<PrimitiveDataFrameColumn<DateTime>, PrimitiveDataFrameColumn<double>> dfFixed))
                        {
                            fixedCnts[cnt] = Tuple.Create(new PrimitiveDataFrameColumn<DateTime>(dateCol),
                                                          new PrimitiveDataFrameColumn<double>(valueCol));
                            dfFixed = fixedCnts[cnt];
                        }

                        dfFixed.Item1.Append(td);
                        dfFixed.Item2.Append(delta);

                        //What date was the last rollover?
                        if (ro > roPrev)
                        {
                            roLast = roPrev; //Change of rollovre
                        }
                    }
                    roPrev = ro;
                    valPrev = val;
                    valCntAheadPrev = valCntAhead;
                    tdPrev = td;    
                }
            }

            //Convert to DF
            var res = new Dictionary<CntMetaData, DataFrame>();
            foreach (var kvp in fixedCnts)
            {
                var dfDelta = new DataFrame(new DataFrameColumn[] { kvp.Value.Item1, kvp.Value.Item2 });
                res[kvp.Key] = dfDelta;
            }
            return res;
        }

        /// <summary>
        /// 
        /// </summary>
        public static DataFrame FuturesAdjustedRatioCalc(DataFrame resDefault, CntMetaData cnt, Dictionary<CntMetaData, 
                        CntMetaData> futureAhead, Dictionary<CntMetaData, DataFrame> dfs, string dateCol, string valueCol, Dictionary<CntMetaData,
                        Dictionary<DateTime, DateTime>> tdRollovers,  int rolloverBuffer)
        {
            var res = resDefault.Clone();
            var cntAhead = futureAhead[cnt];
            if (cntAhead == null) return res;

            var df = dfs[cnt];
            var dfNext = dfs[cntAhead];

            //Each TD refers to a fixed contract, 
            DateTime tdCntAhead;
            DateTime td;
            DateTime tdPrev = DateTime.MinValue;
            DateTime tdPrevCntAhead = DateTime.MinValue;

            double val;
            double valCntAhead;
            double valPrev = double.NaN;
            double valCntAheadPrev = double.NaN;
            double delta;
            DateTime roPrev = DateTime.MinValue;
            double deltaDefault = 1;
            //Need to know what the previous RO date was for reference.
            td = (DateTime)df[dateCol][1];
            var rollOvers = tdRollovers[cnt];
            var ro = rollOvers[td];
            var roLast = rollOvers.Last(x => x.Value < ro).Value;

            for (int i = 0; i < df[dateCol].Length; i++)
            {
                //Both contracts need to have data, incase of the next rollover will require the next contract prev day
                td = (DateTime)df[dateCol][i];
                tdCntAhead = (DateTime)dfNext[dateCol][i];
                res[valueCol][i] = deltaDefault;

                ////CHECKS/////////////////
                if (td != tdCntAhead)
                {
                    throw new Exception($"TDs do not match for all contracts {cnt.Cnt} and {cntAhead.Cnt}");
                }

                ro = tdRollovers[cnt][td];

                //Rollover could be inaccurate, so with the buffer we ignore values around the rollover period
                if (ContractMethods.RolloverZone(ro, roLast, td, rolloverBuffer)) continue;
                //We skip any invalid values
                if (df[valueCol] == null || df[valueCol][i] == null ||
                    dfNext[valueCol] == null || dfNext[valueCol][i] == null)
                {
                    continue;
                }

                ///////////////
                val = (double)df[valueCol][i];
                valCntAhead = (double)dfNext[valueCol][i];

                //On fist value we need a previous value to compare to
                if (roPrev >= roLast)
                {
                    if (ro != roPrev) // we had a rollover since the last valid price, so use the contract ahead previous price
                    {
                        delta = val / valCntAheadPrev;
                    }
                    else //same contract as td-1
                    {
                        delta = val / valPrev; //todo this should be on the D-1 when calc it for a specfic tradedate and ref
                        //price , we need to expec the reference price to be valid and then the delta have a valid
                        //value on the day when we had a valid traded close price.
                    }

                    if (ro > roPrev)
                    {
                        roLast = roPrev; //Change of rollovre
                    }
                    res[valueCol][i] = delta;
                }

                roPrev = ro;
                valPrev = val;
                valCntAheadPrev = valCntAhead;
                tdPrev = td;
            }
            return res;
        }


        public static Dictionary<DateTime, DataFrame> FuturesAdjustedPriceCalcBarRange(DataFrameColumn globalDateCol,
                       DataFrameColumn tbDateCol, string priceCol, Dictionary<DateTime, int> gloablBarsIndex, 
                       DataFrame yDf, DataFrame yDelta)
        {
            //Create a new DF Index for this period.
            var yDfDefault = TimeSeriesDefaultDataFrame(globalDateCol, priceCol);
            //Continuous Future Calc.
            var barPrices = yDf[priceCol];
            var res = new Dictionary<DateTime, DataFrame>();
            for (int x = 0; x < tbDateCol.Length; x++)
            {
                var bar = (DateTime)tbDateCol[x];
                var barIndex = gloablBarsIndex[bar];

                double barPriceActual;
                if (barPrices[barIndex] == null)
                {
                    barPriceActual = double.NaN;
                }
                else
                {
                    barPriceActual = (double)barPrices[barIndex];
                }
                var xContinous = FutureContinousPriceHistoryAbsBarDate(yDfDefault, barIndex, barPriceActual, yDelta, priceCol);

                res.Add(bar, xContinous);

            }
            return res;
        }

        public static Dictionary<DateTime, DataFrame> FuturesAdjustedPriceCalcBarRange(DataFrameColumn globalDateCol,
                     DataFrameColumn tbDateCol, string priceRefCol, string[] priceAdjustCols , Dictionary<DateTime, int> gloablBarsIndex,
                     DataFrame yDf, DataFrame yDelta)
        {
            //Create a new DF Index for this period.
            var yDfDefault = TimeSeriesDefaultDataFrame(globalDateCol, priceRefCol);
            //Continuous Future Calc.
            var barPrices = yDf[priceRefCol];
            var res = new Dictionary<DateTime, DataFrame>();
            for (int x = 0; x < tbDateCol.Length; x++)
            {
                var bar = (DateTime)tbDateCol[x];
                var barIndex = gloablBarsIndex[bar];

                double barPriceActual;
                if (barPrices[barIndex] == null)
                {
                    barPriceActual = double.NaN;
                }
                else
                {
                    barPriceActual = (double)barPrices[barIndex];
                }
                var xContinous = FutureContinousPriceHistoryAbsBarDate(yDfDefault, barIndex, barPriceActual, yDelta, 
                            priceRefCol);

                res.Add(bar, xContinous);

            }
            return res;
        }

        public static DataFrame FutureContinousPriceHistoryAbsBarDate(DataFrame resDefault, int barIndex, double barPriceActual, 
                                DataFrame dfFuturesDelta, string priceCol)
		{
            var res = resDefault.Clone();
            var deltaPrice = dfFuturesDelta[priceCol];
            var deltaAhead = (double)(deltaPrice[barIndex] ?? double.NaN);

            res[priceCol][barIndex] = barPriceActual;

            for (long i = barIndex - 1; i >= 0; i--)
			{
                if (dfFuturesDelta[priceCol][i] == null || double.IsNaN(deltaAhead)) continue;

                double delta = (double)dfFuturesDelta[priceCol][i];
                //Valid Delta, meaning this price is valid and the delta ahead trade date is as well.
                barPriceActual /= deltaAhead;

                res[priceCol][i] = barPriceActual;

                deltaAhead = delta;
            }
            return res;
        }

        //public static DataFrame FutureContinousPriceHistoryAbsBarDate(DataFrame resDefault, int barIndex, double barPriceActual,
        //                       DataFrame dfFuturesDelta, string[] priceAdjustCols)
        //{
        //    var res = resDefault.Clone();
        //    var deltaPrice = dfFuturesDelta[priceCol];
        //    var deltaAhead = (double)(deltaPrice[barIndex] ?? double.NaN);

        //    res[priceCol][barIndex] = barPriceActual;

        //    for (long i = barIndex - 1; i >= 0; i--)
        //    {
        //        if (dfFuturesDelta[priceCol][i] == null || double.IsNaN(deltaAhead)) continue;

        //        double delta = (double)dfFuturesDelta[priceCol][i];
        //        //Valid Delta, meaning this price is valid and the delta ahead trade date is as well.
        //        barPriceActual /= deltaAhead;

        //        res[priceCol][i] = barPriceActual;

        //        deltaAhead = delta;
        //    }
        //    return res;
        //}

        public static Dictionary<T, int> DataFrameIndexTable<T>(DataFrame df, string colName)
		{
            var res = new Dictionary<T, int>();

            var dfCol = df[colName];
			for (int i = 0; i < dfCol.Length; i++)
			{
                var value = (T)dfCol[i];
                res.Add(value, i);
			}
            return res;
        }

        /// <summary>
        /// Given trade date of a continous contract, todays contract reference matches the history
        /// </summary>
        public static void MapContinousContractToTradeDate()
		{
		}

        /// <summary>
        /// Converts dataframe to EMA 
        /// </summary>
        static void ConversionEma(int points, DataFrame dF, string dateTime, string valueCol)
        {
        }

	}

	public static class Extensions
    {
        public static bool IsDefault<T>(this T value) where T : struct
        {
            bool isDefault = value.Equals(default(T));

            return isDefault;
        }
    }
}