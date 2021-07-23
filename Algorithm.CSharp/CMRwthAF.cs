using System.Collections.Generic;
using System;
using QuantConnect.Data.Market;
using QuantConnect.Indicators;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Algorithm.Selection;
using QuantConnect.Algorithm.Framework.Selection;
using QuantConnect.Data.UniverseSelection;
using System.Linq;
using QuantConnect.Orders;

namespace QuantConnect.Algorithm.CSharp
{
    public class CMRwithAF : QCAlgorithm
    {
        private static Dictionary<Symbol, decimal> yestclose;
        private static Dictionary<Symbol, decimal> yesthigh;
        private static Dictionary<Symbol, decimal> todayhigh;
        private static Dictionary<Symbol, decimal> recentclose;
        private static Dictionary<Symbol, decimal> percgain;
        private static Dictionary<Symbol, decimal> candlow;
        private static Dictionary<Symbol, decimal> candhigh;
        private static List<Symbol> FilterListSym;
        private List<Symbol> entryList;
        private decimal yhigh;
        private decimal thigh;
        private decimal chigh;
        private decimal clow;
        private List<string> tickers;
        QuantConnect.Symbol[] symbollist;
        public override void Initialize()
        {
            SetStartDate(2021, 07, 08);  //Set Start Date
            SetEndDate(2021, 07, 17);
            SetTimeZone("Asia/Calcutta");//Set End Date
            SetAccountCurrency("INR");
            SetBrokerageModel(Brokerages.BrokerageName.Zerodha);
            SetCash(500000);

            tickers = new List<string>() { "AARTIIND","ACC","ADANIENT","ADANIPORTS","ALKEM","AMARAJABAT","AMBUJACEM",
                "APLLTD","APOLLOHOSP","APOLLOTYRE","ASHOKLEY","ASIANPAINT","AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO",
                "BAJAJFINSV","BAJFINANCE","BALKRISIND","BANDHANBNK","BANKBARODA","BATAINDIA","BEL","BERGEPAINT",
                "BHARATFORG","BHARTIARTL","BHEL","BIOCON","BOSCHLTD","BPCL","BRITANNIA","CADILAHC","CANBK","CHOLAFIN",
                "CIPLA","COALINDIA","COFORGE","COLPAL","CONCOR","CUB","CUMMINSIND","DABUR","DEEPAKNTR","DIVISLAB","DLF",
                "DRREDDY","EICHERMOT","ESCORTS","EXIDEIND","FEDERALBNK","GAIL","GLENMARK","GMRINFRA","GODREJCP",
                "GODREJPROP","GRANULES","GRASIM","GUJGASLTD","HAVELLS","HCLTECH","HDFC","HDFCAMC","HDFCBANK","HDFCLIFE",
                "HEROMOTOCO","HINDALCO","HINDPETRO","HINDUNILVR","IBULHSGFIN","ICICIBANK","ICICIGI","ICICIPRULI",
                "IDFCFIRSTB","IGL","INDIGO","INDUSINDBK","INFY","IOC","IRCTC","ITC","JINDALSTEL","JSWSTEEL",
                "JUBLFOOD","KOTAKBANK","L&TFH","LALPATHLAB","LICHSGFIN","LT","LTI","LTTS","LUPIN","M&M","M&MFIN",
                "MANAPPURAM","MARICO","MARUTI","MCDOWELL-N","MFSL","MGL","MINDTREE","MOTHERSUMI","MPHASIS","MRF",
                "MUTHOOTFIN","NAM-INDIA","NATIONALUM","NAUKRI","NAVINFLUOR","NESTLEIND","NMDC","NTPC","ONGC","PAGEIND",
                "PEL","PETRONET","PFC","PFIZER","PIDILITIND","PIIND","PNB","POWERGRID","PVR","RAMCOCEM","RBLBANK",
                "RECLTD","RELIANCE","SAIL","SBILIFE","SBIN","SHREECEM","SIEMENS","SRF","SRTRANSFIN","SUNPHARMA","SUNTV",
                "TATACHEM","TATACONSUM","TATAMOTORS","TATAPOWER","TATASTEEL","TCS","TECHM","TITAN","TORNTPHARM",
                "TORNTPOWER","TRENT","TVSMOTOR","UBL","ULTRACEMCO","UPL","VEDL","VOLTAS","WIPRO","ZEEL"};


            //var symbols = new List<QuantConnect.Symbol>();

            symbollist = new QuantConnect.Symbol[tickers.Count];
            int i = 0;
            foreach (string t in tickers)
            {
                var temp = AddEquity(t, Resolution.Minute, Market.India);
                symbollist[i] = temp.Symbol;
                temp.SetLeverage(5);
                i++;
            }

            Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                TimeRules.At(10, 0, 5), () => { PerformFilter(); });
            Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                TimeRules.At(15, 15, 0), () => { Liquidate(); });

        }


        public void PerformFilter()
        {
            var starttime = new DateTime(2015, 5, 20, 9, 45, 0).TimeOfDay;
            var endtime = new DateTime(2015, 5, 20, 10, 0, 0).TimeOfDay;

            yestclose = new Dictionary<Symbol, decimal>();
            recentclose = new Dictionary<Symbol, decimal>();
            todayhigh = new Dictionary<Symbol, decimal>();
            yesthigh = new Dictionary<Symbol, decimal>();
            percgain = new Dictionary<Symbol, decimal>();
            FilterListSym = new List<Symbol>();
            candlow = new Dictionary<Symbol, decimal>();
            candhigh = new Dictionary<Symbol, decimal>();
            entryList = new List<Symbol>();
            yestclose.Clear();
            recentclose.Clear();
            todayhigh.Clear();
            yesthigh.Clear();
            percgain.Clear();
            candlow.Clear();
            candhigh.Clear();
            FilterListSym.Clear();
            Log("Starting to process scheduled event : Filter");
            Log($"Historical Data for : {Time.Date}");
            foreach (var symb in symbollist)
            {
                var history = History(symb, 375 + 45, Resolution.Minute);
                yhigh = 0;
                thigh = 0;
                chigh = 0;
                clow = 100000;
                //int i = 0;
                foreach (var slice in history)
                {

                    //Log($"{slice.Time} : {slice} : value no {i}");
                    //i++;
                    if (slice.High > yhigh && slice.EndTime.Date < Time.Date)
                    {
                        yhigh = slice.High;
                    }

                    if (slice.EndTime.Date == Time.Date &&
                        slice.EndTime.TimeOfDay > starttime &&
                        slice.EndTime.TimeOfDay < endtime && slice.High > chigh)
                    {
                        chigh = slice.High;
                    }

                    if (slice.EndTime.Date == Time.Date &&
                        slice.EndTime.TimeOfDay > starttime &&
                        slice.EndTime.TimeOfDay < endtime && slice.Low < clow)
                    {
                        clow = slice.Low;
                    }

                    if (slice.High > thigh && slice.EndTime.Date == Time.Date && slice.EndTime.TimeOfDay <= starttime)
                    {
                        thigh = slice.High;
                    }

                    if (slice.EndTime.Hour == 15 && slice.EndTime.Date < Time.Date && !yestclose.ContainsKey(symb))
                    {

                        yestclose.Add(symb, slice.Close);
                    }

                    if (slice.EndTime.Hour == 10 && slice.EndTime.Minute == 00 && slice.EndTime.Date == Time.Date && !recentclose.ContainsKey(symb))
                    {
                        recentclose.Add(symb, slice.Close);
                    }
                    //Log($"History of {symb} at {Time.Date} is : Timestamp :{slice.EndTime} --- Close :{slice.Close}");
                }
                yesthigh.Add(symb, yhigh);
                todayhigh.Add(symb, thigh);
                candhigh.Add(symb, chigh);
                candlow.Add(symb, clow);
            }

            foreach (var symb in symbollist)
            {
                if (recentclose.ContainsKey(symb) && yestclose.ContainsKey(symb))
                {
                    percgain.Add(symb, recentclose[symb] / yestclose[symb] - 1);
                }

            }

            var Sortedgain = percgain.OrderByDescending(x => x.Value);
            var temp_list = Sortedgain.Take(10).Select(x => x.Key).ToList();

            foreach (Symbol s in temp_list)
            {
                if (todayhigh[s] > yesthigh[s])
                {
                    FilterListSym.Add(s);
                }
            }

            //Logging Top 10 Symbols
            int i = 0;
            foreach (KeyValuePair<Symbol, decimal> gainer in Sortedgain.Take(10))
            {
                string cross_high = "No";
                if (FilterListSym.Contains(gainer.Key))
                {
                    cross_high = "Yes";
                }
                Log($"{i} . {gainer.Key}");
                Log($"Crossed Yesterday's High : {cross_high}");
                Log($"Percentage Gain : {gainer.Value * 100}");
                Log($"15 Min Candle Low : {candlow[gainer.Key]}");
                Log($"15 Min Candle High : {candhigh[gainer.Key]}");
                i++;

            }

            foreach (Symbol sym in FilterListSym)
            {
                int qty = Convert.ToInt32(250000 / candhigh[sym]);
                StopMarketOrder(sym, -qty, candlow[sym]);
            }
        }

        public override void OnData(Slice slice)
        {
            if (slice != null)
            {
                //
            }

        }

        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            var order = Transactions.GetOrderById(orderEvent.OrderId);
            if (orderEvent.Status == OrderStatus.Filled && !entryList.Contains(orderEvent.Symbol))
            {
                entryList.Add(orderEvent.Symbol);
                var sym = orderEvent.Symbol;
                int qty = Convert.ToInt32(250000 / candhigh[sym]);
                StopMarketOrder(sym, qty, candhigh[sym]);
            }
        }
    }
}
