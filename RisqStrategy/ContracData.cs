using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper.Configuration;
using Microsoft.Data.Analysis;
using Nager.Date;
using static Future_Adjustments.ContractData;

namespace Future_Adjustments{
    public class ContractData
    {
        // public ContractData()
        // {

        // }

        public class CntMetaData : IEquatable<CntMetaData>
		{
            //[DisplayName("Cnt")]
			public string CntParent { get; set; }
            public string Cnt { get; set; }
            public string View { get; set; }
            public Interval Interval { get; set; }
			public double Tick { get; set; }	
            public DateTime? TradeDateFromUtc { get; set; }
            public int? DataPoints { get; set; }
            public Currency? Currency { get; set; }
            public CntUnit? CntUnit { get; set; }
            public int? CntSize { get; set; }
            public CntPeriod? CntPeriod { get; set; }
            public CntExpiry? CntExpiry { get; set; }
            public int CntNo { get; set; }
            public string Include { get; set; }
            public string Xs { get; set; }
            public string Ys { get; set; }
            public string Notes { get; set; }
            public string Path { get; set; }
            public RolloverDayMask RolloverDayMask { get; set; }
            public RolloverExpiryType RolloverExpiryType { get; set; }
            public int? RolloverMinOffset { get; set; }
            public HolidayCalander HolidayCalander { get; set; }
            public RolloverHolidayConstraint RolloverHolidayConstraint { get; set; }

			public override bool Equals(object obj)
			{
				return Equals(obj as CntMetaData);
			}

			public bool Equals(CntMetaData other)
			{
                return other != null &&
                       Cnt == other.Cnt &&
                       View == other.View &&
                       Interval == other.Interval &&
                       Tick == other.Tick &&
                       CntNo == other.CntNo;
			}

            public bool IsTheNextFutureOf(CntMetaData other)
			{
                return other != null &&
                    CntParent == other.CntParent &&
                    View == other.View &&
                    Interval == other.Interval &&
                    Tick == other.Tick &&
                    CntNo == other.CntNo + 1;
            }

			public override string ToString()
			{
				return $"{Cnt} {Interval} {Tick}";
			}
		}

	    public class CntFixed
		{
            public CntMetaData CntMetaDataRef { get; set; }
            public DateTime StartDate { get; set; }
            public DateTime EndDate { get; set; }
            public DateTime Rollover { get; set; }
            public DateTime TradeDate { get; set; }
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

        public sealed class CntMetaDataMap : ClassMap<CntMetaData>
        {
            public CntMetaDataMap()
            {

                Map(m => m.CntParent);
                Map(m => m.Cnt);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.View);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Interval);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Tick);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Currency);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.CntUnit);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.CntExpiry);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.Include);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.CntPeriod);// yyyy-MM-dd HH:mm:ss.fffffffK");
                Map(m => m.RolloverDayMask);
              //  Map(m => m.RolloverTradingDayType);
                Map(m => m.RolloverExpiryType);
                Map(m => m.RolloverMinOffset);
                Map(m => m.HolidayCalander);
                Map(m => m.RolloverHolidayConstraint);
            }
        }

	
        public enum CntPeriod
        {
            H,
            D,
            WE,
            W,
            M,
            Q,
            S,
            Win,
            Y
        }

        public enum Currency
        {
            Euros,
            Gbp,
            Usd
        }

        public enum CntUnit
        {
            Tonne,
            Mw,
            Th,
            Euromw
        }

        public enum CntType
        {
            Hourly,
            Total
        }

        public enum CntExpiry
        {
            Continous,
            Fixed
        }

        public enum Interval
        {
            tick,
            intraday1Minute,
            hour,
            daily,
            weekly,
            monthly,
            quaterly,
            yearly
        }

        public enum RolloverTradingDayType
        {
            None,
            Business,
            Any
        }

        public enum HolidayCalander
        {
            None,
            DE,
            GB
        }

        public enum RolloverHolidayConstraint
        {
            None,
            LastBusinessWeek,
            BusinessDay
        }

        public enum RolloverDayMask
        {
            None,
            MTWTFSS,
            MTWTFxx,
            Mxxxxxx
        }

        public enum RolloverExpiryType
        {
            None,
            NextContractDeliveryStart,
            ThisContractDeliveryStart
        }
	}

    public static class ContractMethods
    {
        public static DateTime AddBar(this DateTime bar, int bars, Interval interval)
        {
			switch (interval)
			{
				case Interval.tick:
					throw new NotImplementedException();
				case Interval.intraday1Minute:
					throw new NotImplementedException();
				case Interval.hour:
                    throw new NotImplementedException();
                case Interval.daily:
                    return bar.AddDays(bars);
				case Interval.weekly:
					throw new NotImplementedException();
				case Interval.monthly:
					throw new NotImplementedException();
				case Interval.quaterly:
					throw new NotImplementedException();
				case Interval.yearly:
					throw new NotImplementedException();
				default:
					break;
			}
            return DateTime.MinValue;//TODO compier error without?
		}

		public static string ContractFixedName(CntMetaData cnt, DateTime sd)
        {
            return cnt.CntParent + "-" + FixedContractDateCode(cnt.CntPeriod, sd);
            //cnt.CntParent + sd.ToString();
        }
		public static Dictionary<string, List<CntMetaData>>GetParentContracts(List<CntMetaData> cnts)
		{
			return cnts.GroupBy(x => x.CntParent).ToDictionary(cnt => cnt.Key, cnt => cnt.ToList());
		}

        public static Dictionary<CntMetaData, ContractTimeSeriesData.ContractData> RetriveCntTimeSeriesDataCsvHelper(List<CntMetaData> contracts)
		{
            //Get The Historical Time Series Into DataFrame
            var cntData = new Dictionary<CntMetaData, ContractTimeSeriesData.ContractData>();

            foreach (var item in contracts)
            {
                var type = item.Interval;
                var tsPath = item.Path;
                var rows = item.DataPoints == null ? -1 : (int)item.DataPoints;

                switch (type)
                {
                    case Interval.tick:
                        //var tickData = DataFrameHelper.LoadCsv(tsPath, rows); //mindful this is top to bottom
                        //cntData.Add(item, tickData);

                        break;
                    case Interval.intraday1Minute:
                    case Interval.hour:
                    case Interval.daily:
                    case Interval.weekly:
                    case Interval.monthly:
                    case Interval.quaterly:
                    case Interval.yearly:

                        var barData = CsvReadWrite.CsvImport<ContractTimeSeriesData.BarData>(tsPath);
                        //var barData = DataFrameHelper.LoadCsv(tsPath, rows);  //mindful this is top to bottom
                      //  cntData.Add(item, barData);

                        break;
                    default:
                        break;
                }
            }
            return cntData;
        }

        public static Dictionary<CntMetaData, DataFrame> RetriveCntTimeSeriesData(List<CntMetaData> contracts, bool IsLocal=false, List<string> ListFilePaths = null)
        {
            //Get The Historical Time Series Into DataFrame
            var cntData = new Dictionary<CntMetaData, DataFrame>();

            // For each contract with metadata we need its corresponding local path 
            for(int i = 0; i < contracts.Count(); i++)
            {
                var type = contracts[i].Interval; // Frequency of data: daily, hourly, minutes, ticks 
                
                string tsPath;
                if(IsLocal) // if local take the paths from the list provided, otherwise they are in the metadata variable 
                {
                    tsPath = ListFilePaths[i];
                }
                else
                {
                    tsPath = contracts[i].Path;
                }
            
                var rows = contracts[i].DataPoints == null ? -1 : (int)contracts[i].DataPoints;

                switch (type)
                {
                    case Interval.tick:
                        
                        var tickData = DataFrameHelper.LoadCsv(tsPath, rows); //mindful this is top to bottom
                        cntData.Add(contracts[i], tickData);

                        break;
                    case Interval.intraday1Minute:
                    case Interval.hour:
                        throw new NotImplementedException();
                        break;
                    case Interval.daily:
                        var barData = DataFrameHelper.LoadCsv(tsPath, rows);  //mindful this is top to bottom
                        cntData.Add(contracts[i], barData);
                        break;
                    case Interval.weekly:
                    case Interval.monthly:
                    case Interval.quaterly:
                    case Interval.yearly:
                        throw new NotImplementedException();
                        
                    default:
                        break;
                }
            }

            return cntData;
        }
        
		//Gets csv data, converts to DF and unions all DF of the same time series.
        public static Dictionary<CntMetaData, DataFrame> CollateTimeSeriesData(string metaDataPath)
        {
            var contracts = CsvReadWrite.CsvImport<CntMetaData>(metaDataPath).Where(c => c.Include == "Y").ToList();
            var cntDf = RetriveCntTimeSeriesData(contracts);

            if (cntDf == null) return null;
            //Combine DF into same timeseries
            // var merge = 
            return null;  //todo
        }

        public static bool BusinessDay(DateTime time)
        {
            switch (time.DayOfWeek)
            {
                case DayOfWeek.Monday:
                case DayOfWeek.Tuesday:
                case DayOfWeek.Wednesday:
                case DayOfWeek.Thursday:
                case DayOfWeek.Friday:
                    return true;
                case DayOfWeek.Sunday:
                case DayOfWeek.Saturday:
                    return false;
                default:
                    throw new Exception();
            }
        }

        public static bool DateIsMask(RolloverDayMask mask, DateTime date)
        {
			switch (mask)
			{
				case RolloverDayMask.None:
				case RolloverDayMask.MTWTFSS:
					return true;
				case RolloverDayMask.MTWTFxx:
					return date.DayOfWeek == DayOfWeek.Monday ||
						date.DayOfWeek == DayOfWeek.Tuesday ||
						date.DayOfWeek == DayOfWeek.Wednesday ||
						date.DayOfWeek == DayOfWeek.Thursday ||
						date.DayOfWeek == DayOfWeek.Friday;
				case RolloverDayMask.Mxxxxxx:
					return date.DayOfWeek == DayOfWeek.Monday;
				default:
                    throw new NotImplementedException();
			}
		}

        public static Dictionary<CntMetaData, CntMetaData> GetContractsNextContract(List<CntMetaData> cntMetaDatas)
		{
            var res = new Dictionary<CntMetaData, CntMetaData>();

            foreach (var cnt in cntMetaDatas)
			{
				switch (cnt.CntExpiry)
				{
					case CntExpiry.Continous:
                        var cntAhead = cntMetaDatas.FirstOrDefault(x => x.IsTheNextFutureOf(cnt));
                        if (cntAhead == default(CntMetaData))
						{
                            res.Add(cnt, cntAhead);
                            continue;
                        };
                        res.Add(cnt, cntAhead);
                        break;
					case CntExpiry.Fixed:
                        res.Add(cnt, cnt);
                        break;
					default:
                        throw new Exception();
				}

				if (cnt.CntExpiry == CntExpiry.Fixed)
				{
                    continue;
				}

			}

            return res;
		}

        public static bool RolloverZone(DateTime roNext, DateTime roPrev, DateTime barDate, int rolloverBuffer)
		{
            if (barDate > roNext.AddDays(-rolloverBuffer) || barDate < roPrev.AddDays(rolloverBuffer))
			{
                return true;
			}
			return false;
		}

		public static DateTime RolloverPreviousReferencePoint(DateTime td, CntMetaData cnt)
        {
            return ContractPreviousEndDate(cnt.CntPeriod, td);
        }

        public static DateTime RolloverFutureReferencePoint(DateTime td, CntMetaData cnt)
		{
            //give today td, what is continous contract no 1 reference point for rollover?
            //  return EndDateByCntType(cnt.CntPeriod, cntsAhead, td);
            return ContractNextEndDate(cnt.CntPeriod, td);

        //switch (cnt.RolloverExpiryType)
			//{
			//	case RolloverExpiryType.None:
			//	case RolloverExpiryType.ThisContractDeliveryStart:
			//		return EndDateByCntType(cnt.CntPeriod, 0, td, cnt.RolloverExpiryType);
			//	case RolloverExpiryType.NextContractDeliveryStart:
			//		return EndDateByCntType(cnt.CntPeriod, 1, td, cnt.RolloverExpiryType);
			//	//return StartDateByCntType(cnt.CntPeriod, 1, td);
			//	default:
			//		throw new NotImplementedException();
			//}
		}
		/// <summary>
		/// Give that td is the latest rollover day possible.
		/// </summary>
		public static DateTime CalcRolloverDateFromReference(CntMetaData cntMetaData, DateTime refDate)
        {
			var dateIsMask = DateIsMask(cntMetaData.RolloverDayMask, refDate);
			var holidayConstr = IsHolidayConstraint(refDate, cntMetaData, 0);
            var minOffset = cntMetaData.RolloverMinOffset;
			int offset = 0;
            //exit when mask matches, offset is min and no holiday constraint
			//while (offset < minOffset || !dateIsMask || !holidayConstr)
			while (!(offset >= minOffset && dateIsMask && !holidayConstr))
            {
				if (!holidayConstr && dateIsMask) // only on valid days and non holidays if there is a holiday calander.
				{
					offset++;	
				}

                refDate = refDate.AddDays(-1);
				dateIsMask = DateIsMask(cntMetaData.RolloverDayMask, refDate);
				if (dateIsMask)
				{
                    holidayConstr = IsHolidayConstraint(refDate, cntMetaData, offset);
                }
            }
			return refDate;
		}

		public static bool IsHolidayConstraint(DateTime td, CntMetaData cntMetaData, int offset)
		{
            //if (DateSystem.IsWeekend(td, cntMetaData.HolidayCalander.ToString())) return false;

            switch (cntMetaData.RolloverHolidayConstraint)
			{
				case RolloverHolidayConstraint.None:
					return false;
				case RolloverHolidayConstraint.LastBusinessWeek:
                    //Constraint only on last buinsess week and not on weekends
                    if (offset > 0) return false;
                    
                    var firstDow = DateSystem.FindDay(td.AddDays(-6), DayOfWeek.Monday);
					var lastDow = DateSystem.FindDay(firstDow, DayOfWeek.Friday);
                    //assume last week and in the week now.
					td = firstDow;
					while (td <= lastDow)
					{
						if (DateSystem.IsPublicHoliday(td, cntMetaData.HolidayCalander.ToString()))
						{
							return true;
						};
						td = td.AddDays(1);
					}
					return false;
				case RolloverHolidayConstraint.BusinessDay:
					return DateSystem.IsPublicHoliday(td, cntMetaData.HolidayCalander.ToString());
				default:
					throw new Exception();

			}
		}

		public static DateTime StartDateByCntType(CntPeriod? cntPeriod, int cntNo, DateTime td,
            RolloverExpiryType roType)
        {
            // rollover is always before the start of the Cnt + 
            int future = cntNo;

			switch (roType)
			{
				case RolloverExpiryType.ThisContractDeliveryStart:
                case RolloverExpiryType.None:
                    break;
				case RolloverExpiryType.NextContractDeliveryStart:
                    future--;
                    break;
			}

			switch (cntPeriod)
			{
				case CntPeriod.H:
					throw new Exception();
				case CntPeriod.D:
					throw new Exception();
				case CntPeriod.WE:
					throw new Exception();
				case CntPeriod.W:
					throw new Exception();
				case CntPeriod.M:
					return new DateTime(td.Year, td.Month, 1).AddMonths(future);
				case CntPeriod.Q:
					var firstMonthofQtr = (int)Math.Ceiling((decimal)td.Month / 3) * 3 - 2;
					return new DateTime(td.Year, firstMonthofQtr, 1).AddMonths(future * 3);
				case CntPeriod.S:
					throw new Exception();
				case CntPeriod.Win:
					throw new Exception();
				case CntPeriod.Y: 
					return new DateTime(td.Year + future, 1, 1);
					//throw new Exception();
				default:
					throw new Exception();
			}
		}

		public static DateTime EndDateFromStartDate(CntPeriod? cntPeriod, DateTime startDate)
		{
			switch (cntPeriod)
			{
				case CntPeriod.H:
					throw new Exception();
				case CntPeriod.D:
					throw new Exception();
				case CntPeriod.WE:
					throw new Exception();
				case CntPeriod.W:
					throw new Exception();
				case CntPeriod.M:
					return new DateTime(startDate.Year + 1, 1, 1).AddDays(-1);
				case CntPeriod.Q:
					return new DateTime(startDate.Year, startDate.Month, 1).AddMonths(3).AddDays(-1);
				case CntPeriod.S:
					throw new Exception();
				case CntPeriod.Win:
					throw new Exception();
				case CntPeriod.Y:
					return new DateTime(startDate.Year + 1, 1, 1).AddDays(-1);
				default:
					throw new Exception();
			}
		}

        public static DateTime ContractPreviousEndDate(CntPeriod? cntPeriod, DateTime td)
        {
            switch (cntPeriod)
            {
                case CntPeriod.H:
                    throw new Exception();
                case CntPeriod.D:
                    throw new Exception();
                case CntPeriod.WE:
                    throw new Exception();
                case CntPeriod.W:
                    throw new Exception();
                case CntPeriod.M:
                    return new DateTime(td.Year, td.Month, 1).AddMonths(0).AddDays(-1);
                case CntPeriod.Q:
                    var firstMonthofQtr = (int)Math.Ceiling((decimal)td.Month / 3) * 3 - 2;
                    return new DateTime(td.Year, firstMonthofQtr, 1).AddMonths(3  * 0).AddDays(-1);
                case CntPeriod.S:
                    throw new Exception();
                case CntPeriod.Win:
                    throw new Exception();
                case CntPeriod.Y:
                    return new DateTime(td.Year + 0, 1, 1).AddDays(-1);
                //throw new Exception();
                default:
                    throw new Exception();
            }
        }

        public static DateTime ContractNextEndDate(CntPeriod? cntPeriod, DateTime td)
        {
            switch (cntPeriod)
            {
                case CntPeriod.H:
                    throw new Exception();
                case CntPeriod.D:
                    throw new Exception();
                case CntPeriod.WE:
                    throw new Exception();
                case CntPeriod.W:
                    throw new Exception();
                case CntPeriod.M:
                    return new DateTime(td.Year, td.Month, 1).AddMonths(1).AddDays(-1);
                case CntPeriod.Q:
                    var firstMonthofQtr = (int)Math.Ceiling((decimal)td.Month / 3) * 3 - 2;
                    return new DateTime(td.Year, firstMonthofQtr, 1).AddMonths(3 * 1).AddDays(-1);
                case CntPeriod.S:
                    throw new Exception();
                case CntPeriod.Win:
                    throw new Exception();
                case CntPeriod.Y:
                    return new DateTime(td.Year + 1, 1, 1).AddDays(-1);
                //throw new Exception();
                default:
                    throw new Exception();
            }
        }

        public static DateTime EndDateByCntType(CntPeriod? cntPeriod, int cntNo, DateTime td, RolloverExpiryType roType)
		{
            // rollover is always before the start of the Cnt + 
            int future = cntNo;

            switch (roType)
            {
                case RolloverExpiryType.ThisContractDeliveryStart:
                case RolloverExpiryType.None:
                    break;
                case RolloverExpiryType.NextContractDeliveryStart:
                    future--;
                    break;
            }

            switch (cntPeriod)
            {
                case CntPeriod.H:
                    throw new Exception();
                case CntPeriod.D:
                    throw new Exception();
                case CntPeriod.WE:
                    throw new Exception();
                case CntPeriod.W:
                    throw new Exception();
                case CntPeriod.M:
                    return new DateTime(td.Year, td.Month, 1).AddMonths(future + 1).AddDays(-1);
                case CntPeriod.Q:
                    var firstMonthofQtr = (int)Math.Ceiling((decimal)td.Month / 3) * 3 - 2;
                    return new DateTime(td.Year, firstMonthofQtr, 1).AddMonths(future * 3).AddDays(-1);
                case CntPeriod.S:
                    throw new Exception();
                case CntPeriod.Win:
                    throw new Exception();
                case CntPeriod.Y:
                    return new DateTime(td.Year + future + 1, 1, 1).AddDays(-1);
                //throw new Exception();
                default:
                    throw new Exception();
            }
        }

        public static string FixedContractDateCode(CntPeriod? cntPeriod, DateTime startDate)
        {
            switch (cntPeriod)
            {
                case CntPeriod.H:
                    throw new Exception();
                case CntPeriod.D:
                    throw new Exception();
                case CntPeriod.WE:
                    throw new Exception();
                case CntPeriod.W:
                    throw new Exception();
                case CntPeriod.M:
                    return startDate.ToString("MMM dd");
                case CntPeriod.Q:
                    var qtr = (int)Math.Ceiling((decimal)startDate.Month / 3);
                    return startDate.ToString($"Q{qtr} yyyy");
                case CntPeriod.S:
                    throw new Exception();
                case CntPeriod.Win:
                    throw new Exception();
                case CntPeriod.Y:
                    return startDate.ToString("yyyy");
                default:
                    throw new Exception();
            }
        }

        public static void GetCntFixed(DateTime td, CntMetaData continousCnt)
		{

            var refDate = RolloverFutureReferencePoint(td, continousCnt);
            var rollOver = CalcRolloverDateFromReference(continousCnt, refDate);
            var sd = StartDateByCntType(continousCnt.CntPeriod, (int)continousCnt.CntNo, rollOver, continousCnt.RolloverExpiryType);
            var ed = EndDateFromStartDate((CntPeriod)continousCnt.CntPeriod, sd);
        }

        /// <summary>
        /// Returns a dictionary of tradedate, starting from the previous rollover date and the future rollover date.
        /// </summary>
        public static Dictionary<DateTime, DateTime> GetNextRollOverDates(DateTime tdFrom, DateTime tdTo, CntMetaData cnt)
        {
            var res = new Dictionary<DateTime, DateTime>();
           // var td = tdFrom;
            //var refDate = td;

            //Find the previous rollover date to understand when this contract was 
            var refDate = RolloverPreviousReferencePoint(tdFrom, cnt);// ContractPreviousEndDate
            var rollover = CalcRolloverDateFromReference(cnt, refDate);

            var td = rollover;

            while (td <= tdTo)
            {
                //Next delivery start in the future 
                refDate = RolloverFutureReferencePoint(refDate, cnt);
                rollover = CalcRolloverDateFromReference(cnt, refDate);

				for (int i = 0; i < rollover.Subtract(td).Days + 1; i++)
				{
                    res.Add(td.AddDays(i), rollover);
                    //td = td.AddDays(1);
                }

                td = rollover.AddDays(1);
                refDate = refDate.AddDays(1);
            }
            return res;

        }

        /// <summary>
        /// Returns a dictionary of tradedate, starting from the previous rollover date and the future rollover date.
        /// </summary>
        public static Dictionary<DateTime, DateTime> GetNextRollOverDates(DataFrameColumn global, CntMetaData cnt)
        {
            var res = new Dictionary<DateTime, DateTime>();
            // var td = tdFrom;
            //var refDate = td;

            //Find the previous rollover date to understand when this contract was 
            var refDate = RolloverPreviousReferencePoint((DateTime)global[0], cnt);// ContractPreviousEndDate
            var rollover = CalcRolloverDateFromReference(cnt, refDate);

            var td = rollover;

            while (td <= (DateTime)global[global.Length - 1])
            {
                //Next delivery start in the future 
                refDate = RolloverFutureReferencePoint(refDate, cnt);
                rollover = CalcRolloverDateFromReference(cnt, refDate);

                for (int i = 0; i < rollover.Subtract(td).Days + 1; i++)
                {
                    res.Add(td.AddDays(i), rollover);
                    //td = td.AddDays(1);
                }

                td = rollover.AddDays(1);
                refDate = refDate.AddDays(1);
            }
            return res;

        }

        /// <summary>
        /// Given a tradedate and contract continous no.  What was the same contract number in the past?
        /// </summary>
        public static Dictionary<DateTime, int> GetHistoryLookupContractNo(DateTime td, CntMetaData cnt,
                            Dictionary<DateTime, DateTime> tdRollOver)
		{
            if (tdRollOver == null) return null;

            var res= new Dictionary<DateTime, int>();
            var cntNo = cnt.CntNo;
            var firstTd = tdRollOver.First().Key;
            var lastRollOver = tdRollOver[td];

            while (td > firstTd)
			{
                var ro = tdRollOver[td];

                if (ro < lastRollOver)//new rollover
                {
                    cntNo++;
                    lastRollOver = ro;
                }

                res[td] = cntNo;
            }
            return res;
		}
	}
}

