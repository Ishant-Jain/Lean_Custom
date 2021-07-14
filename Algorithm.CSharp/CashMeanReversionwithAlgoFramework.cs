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
using QuantConnect.Algorithm.Framework.Alphas;
using QuantConnect.Algorithm.Framework.Portfolio;
using QuantConnect.Algorithm.Framework.Execution;
using QuantConnect.Algorithm.Framework;
using QuantConnect.Securities;
using QuantConnect.Algorithm.Framework.Risk;
using QuantConnect.Scheduling;

namespace QuantConnect.Algorithm.CSharp
{
    public class CashMeanReversionwithAlgoFramnework : QCAlgorithm
    {
        public static Dictionary<Symbol, Double> qty_list;
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
        private static List<QuantConnect.Symbol> symbollist;
        public override void Initialize()
        {
            SetStartDate(2021, 07, 01);  //Set Start Date
            SetEndDate(2021, 07, 06);
            SetTimeZone("Asia/Calcutta");//Set End Date
            SetAccountCurrency("INR");
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

            //Setting Custom Universe
            SetUniverseSelection(new ScheduledUniverseSelectionModel(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                                    TimeRules.At(10, 0, 5), TopDailyGainersFilter));
            UniverseSettings.Resolution = Resolution.Minute;

            //Setting Custom Alpha
            var Alpha_Time = new DateTime(2020, 01, 01, 10, 1, 0).TimeOfDay;
            AddAlpha(new TopGainers(InsightType.Price, Alpha_Time));

            //Setting Custom Portfolio Model
            SetPortfolioConstruction(new MyPortfolioModel());

            //Setting Custom Risk Model
            AddRiskManagement(new MyRiskModel());

            //Setting Custom Execution Model
            SetExecution(new MyExecutionModel());

            //Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
            //TimeRules.At(15, 15, 0), () => { Liquidate(); });
        }

        //Universe Selection Filter
        public IEnumerable<Symbol> TopDailyGainersFilter(DateTime dateTime)
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

            symbollist = new List<Symbol>();
            foreach (var symb in tickers)
            {
                symbollist.Add(QuantConnect.Symbol.Create(symb, SecurityType.Equity, Market.NSE));
            }

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

                    if (slice.EndTime.Hour == 15 && slice.EndTime.Minute == 30)
                    {

                        yestclose.Add(symb, slice.Close);
                    }

                    if (slice.EndTime.Hour == 10 && slice.EndTime.Minute == 00 && slice.EndTime.Date == Time.Date)
                    {
                        recentclose.Add(symb, slice.Close);
                    }

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

            return FilterListSym;
        }

        //Alpha Model
        public partial class TopGainers : AlphaModel
        {
            private readonly InsightType _type;
            private readonly HashSet<Security> _securities;
            private bool traded = false;
            private TimeSpan _ScheduledTime;

            public TopGainers(InsightType type, TimeSpan ScheduleTime)
            {
                _type = type;
                _ScheduledTime = ScheduleTime;
                _securities = new HashSet<Security>();
            }

            public override IEnumerable<Insight> Update(QCAlgorithm algorithm, Slice data)
            {
                foreach (var security in _securities)
                {
                    if (security.Price != 0 && ShouldEmitInsight(algorithm, security.Symbol) && traded == false && algorithm.Time.TimeOfDay == _ScheduledTime)
                    {
                        yield return new Insight(security.Symbol, TimeSpan.FromMinutes(298), _type, InsightDirection.Down);
                    }
                }
            }

            public override void OnSecuritiesChanged(QCAlgorithm algorithm, SecurityChanges changes)
            {
                NotifiedSecurityChanges.UpdateCollection(_securities, changes);
                traded = false;
            }

            protected virtual bool ShouldEmitInsight(QCAlgorithm algorithm, Symbol symbol)
            {
                bool answer = true;
                if (algorithm.Portfolio[symbol].Invested)
                {
                    traded = true;
                    answer = false;
                }
                return answer;
            }
        }

        //Portfolio Construction Model
        public partial class MyPortfolioModel : PortfolioConstructionModel
        {
            private readonly PortfolioBias _portfolioBias;

            public MyPortfolioModel(Func<DateTime, DateTime?> rebalancingFunc,
                PortfolioBias portfolioBias = PortfolioBias.LongShort)
                : base(rebalancingFunc)
            {
                _portfolioBias = portfolioBias;
            }

            public MyPortfolioModel(Func<DateTime, DateTime> rebalancingFunc,
                PortfolioBias portfolioBias = PortfolioBias.LongShort)
                : this(rebalancingFunc != null ? (Func<DateTime, DateTime?>)(timeUtc => rebalancingFunc(timeUtc)) : null, portfolioBias)
            {
            }

            public MyPortfolioModel(TimeSpan timeSpan,
                PortfolioBias portfolioBias = PortfolioBias.LongShort)
                : this(dt => dt.Add(timeSpan), portfolioBias)
            {
            }

            public MyPortfolioModel(Resolution resolution = Resolution.Daily,
                PortfolioBias portfolioBias = PortfolioBias.LongShort)
                : this(resolution.ToTimeSpan(), portfolioBias)
            {
            }

            protected override Dictionary<Insight, double> DetermineTargetPercent(List<Insight> activeInsights)
            {
                var result = new Dictionary<Insight, double>();
                var count = activeInsights.Count(x => x.Direction != InsightDirection.Flat && RespectPortfolioBias(x));
                qty_list = new Dictionary<Symbol, double>();
                Double sum_quant = 0;

                foreach (var ins in activeInsights)
                {
                    Double qty = Convert.ToDouble(250000 / candhigh[ins.Symbol]);
                    qty_list.Add(ins.Symbol, qty);
                    sum_quant += qty;
                }
                foreach (var ins in activeInsights)
                {
                    Double percent = qty_list[ins.Symbol] / sum_quant;
                    result[ins] = (double)((int)(RespectPortfolioBias(ins) ? ins.Direction : InsightDirection.Flat) * percent);
                }
                return result;
            }

            protected bool RespectPortfolioBias(Insight insight)
            {
                return _portfolioBias == PortfolioBias.LongShort || (int)insight.Direction == (int)_portfolioBias;
            }
        }

        //Risk Management Model
        public partial class MyRiskModel : RiskManagementModel
        {
            public override IEnumerable<IPortfolioTarget> ManageRisk(QCAlgorithm algorithm, IPortfolioTarget[] targets)
            {
                foreach (var kvp in algorithm.Securities)
                {
                    Symbol symbol = kvp.Key;
                    var security = kvp.Value;
                    if (algorithm.Portfolio[security.Symbol].Invested)
                    {
                        decimal StopPrice = candhigh[symbol];
                        var CurrentPrice = security.Close;
                        if (CurrentPrice >= StopPrice)
                        {
                            algorithm.Log($"Stop Loss Applied for {security.Symbol} at {CurrentPrice} as Stop Loss was : {StopPrice}");
                            yield return new PortfolioTarget(security.Symbol, 0);
                        }
                    }
                }
            }
        }

        //Execution Model
        public class MyExecutionModel : ExecutionModel
        {
            private readonly PortfolioTargetCollection _targetsCollection = new PortfolioTargetCollection();
            public override void Execute(QCAlgorithm algorithm, IPortfolioTarget[] targets)
            {
                _targetsCollection.AddRange(targets);
                if (_targetsCollection.Count > 0)
                {
                    foreach (var target in _targetsCollection.OrderByMarginImpact(algorithm))
                    {
                        // calculate remaining quantity to be ordered
                        var quantity = OrderSizing.GetUnorderedQuantity(algorithm, target);
                        if (quantity != 0)
                        {
                            algorithm.MarketOrder(target.Symbol, quantity);
                        }
                    }

                    _targetsCollection.ClearFulfilled(algorithm);
                }
            }
        }

        public override void OnSecuritiesChanged(SecurityChanges changes)
        {
            if (changes.AddedSecurities.Count > 0)
            {
                foreach (var s in changes.AddedSecurities)
                {
                    Debug($"added {s.Symbol.Value} at {Time}");
                }
            }

            if (changes.RemovedSecurities.Count > 0)
            {
                foreach (var s in changes.RemovedSecurities)
                {
                    Debug($"removed {s.Symbol.Value} at {Time}");
                }
            }

            foreach (var x in ActiveSecurities.Keys)
            {
                Debug($"Active: {x.Value} at {Time}");
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
            if (orderEvent.Status == OrderStatus.Filled)
            {
                var status = orderEvent.Direction;
                var symbol = orderEvent.Symbol;
                var fill_price = orderEvent.FillPrice;
                Log($"On {Time.TimeOfDay} : Order Filled for {symbol} : {status} at : {fill_price}");
            }
        }

    }
}
