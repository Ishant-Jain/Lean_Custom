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
using MathNet;

namespace QuantConnect.Algorithm.CSharp
{
    public class LiveStrategy : QCAlgorithm
    {
        private static Dictionary<Symbol, decimal> yestclose;
        private static Dictionary<Symbol, decimal> yesthigh;
        private static Dictionary<Symbol, decimal> todayhigh;
        private static Dictionary<Symbol, decimal> recentclose;
        private static Dictionary<Symbol, decimal> percgain;
        private static Dictionary<Symbol, decimal> candlow;
        private static Dictionary<Symbol, decimal> candhigh;
        private static List<Symbol> FilterListSym;
        private static Dictionary<Symbol, OrderTicket> Sell_Tickets;
        private static Dictionary<Symbol, OrderTicket> Stop_Loss_Tickets;
        private static Dictionary<Symbol, decimal> Sym_Quantity;
        private List<Symbol> entryList;
        private decimal yhigh;
        private decimal thigh;
        private decimal chigh;
        private decimal clow;
        private List<string> tickers;
        QuantConnect.Symbol[] symbollist;
        public override void Initialize()
        {
            SetStartDate(2020, 01, 02); //Set Start Date
            SetEndDate(2020, 01, 10);
            SetTimeZone("Asia/Calcutta"); //Set End Date
            SetAccountCurrency("INR");
            SetBrokerageModel(Brokerages.BrokerageName.Zerodha);
            SetCash(10000000);
            //UniverseSettings.Leverage = 3;

            tickers = new List<string>() {"AARTIIND","ACC","ADANIENT","ADANIPORTS","ALKEM","AMARAJABAT","AMBUJACEM",
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
                //temp.SetLeverage(3);
                i++;
            }
            Sym_Quantity = new Dictionary<Symbol, decimal>();
            DefaultOrderProperties = new ZerodhaOrderProperties(exchange: Exchange.NSE);

            Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                TimeRules.At(10, 1, 0), () => { PerformFilter(); });
            Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                TimeRules.At(14, 45, 0), () => { Exit_Positions(); });
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
            Sell_Tickets = new Dictionary<Symbol, OrderTicket>();
            Stop_Loss_Tickets = new Dictionary<Symbol, OrderTicket>();
            //Sym_Quantity = new Dictionary<Symbol, decimal>();
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
                var history = History(symb, 375 + 46, Resolution.Minute);
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

                    if (slice.EndTime.Hour == 15 && slice.EndTime.Minute == 30 && slice.EndTime.Date < Time.Date)
                    {
                        if (!yestclose.ContainsKey(symb))
                        {
                            yestclose.Add(symb, slice.Close);
                        }
                    }

                    if (slice.EndTime.Hour == 10 && slice.EndTime.Minute == 00 && slice.EndTime.Date == Time.Date)
                    {
                        if (!recentclose.ContainsKey(symb))
                        {
                            recentclose.Add(symb, slice.Close);
                        }
                    }
                    //Log($"History of {symb} at {Time.Date} is : Timestamp :{slice.EndTime} --- Close :{slice.Close}");
                }
                yesthigh.Add(symb, yhigh);
                todayhigh.Add(symb, thigh);
                candhigh.Add(symb, chigh);
                candlow.Add(symb, clow);
                //Log($"Receent close is {recentclose[symb]}");
                //Log($"yesterday's close is {yestclose[symb]}");
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
                //if (true)
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
            if (FilterListSym.Count() > 0)
            {
                Log($"Portfolio value : {Portfolio.Cash} -- Portfolio margin Remaining : {Portfolio.MarginRemaining}");
                decimal Equity_perc = Portfolio.Cash * 3 / FilterListSym.Count;
                decimal Min_Equity = Portfolio.Cash * 3 * 2 / 10;
                decimal Equity = Math.Min(Equity_perc, Min_Equity);

                foreach (Symbol sym in FilterListSym)
                {
                    if (Securities[sym].Price > candlow[sym])
                    {
                        int qty = Convert.ToInt32(Math.Floor(Equity / candlow[sym]));
                        Log($"Stop Market Order Placed for {sym} : Quantity : {-qty} at Price : {candlow[sym]}");
                        Sell_Tickets.Add(sym, StopMarketOrder(sym, -qty, candlow[sym]));
                    }
                    else
                    {
                        int qty = Convert.ToInt32(Math.Floor(Equity / Securities[sym].Price));
                        Log($"Market Order Placed for {sym} : Quantity : {-qty} at Price : {Securities[sym].Close}");
                        MarketOrder(sym, -qty, asynchronous: true);
                    }
                }
            }
        }

        public override void OnData(Slice slice)
        {
            Show_Positions();
        }

        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            var order = Transactions.GetOrderById(orderEvent.OrderId);

            //1. When any Sell/Short Order is placed
            //2. Add Symbol to Entry List, so that Again Stop-Loss is not Placed
            //3. Remove that Symbol from Stop_Loss_Tickets Dictionary,
            //   so that at 03:15:00 P.M. when we Cancel the Non-Triggered Orders,
            //   we don't get Error from Broker.
            //4. Find Symbol,Quantity and Profit Target
            //5. Place Stop-Loss Order and Add the Order to the Stop_Loss_Tickets.
            //6. Place Profit-Target Order and Add the Order to the Profit_Target_Tickets.
            //7. Otherwise if Securities[sym].Price > candhigh[sym], Place a Mraket Order to Liquidate the Symbol.

            if (orderEvent.Status == OrderStatus.Filled && !entryList.Contains(orderEvent.Symbol) && orderEvent.Quantity<0)
            {
                entryList.Add(orderEvent.Symbol);
                var sym = orderEvent.Symbol;
                var qty = orderEvent.Quantity * -1;
                var stop_loss = candhigh[sym];
                Sym_Quantity.Add(sym, orderEvent.Quantity);

                //1. If StopMarketOrder is Filled then :
                //   Remove Ticket from Dictionary and Print Trigger Message
                //2. Else If MarketOrder get Triggered then there is no ticket to remove
                //   and trigger message is already printed above.

                try
                {
                    Sell_Tickets.Remove(sym);
                    Log($"Stop Market Order Triggered for {sym} : Quantity : {-qty} at Price {orderEvent.FillPrice}");
                }
                catch
                {
                    return;
                }

                // Activating Stop Loss for each Short Order Placed
                if (Securities[sym].Price < stop_loss)
                {
                    Log($"STOPLOSS ACTIVATED - Stop Market Order Placed for {sym} : Quantity : {qty} at TriggerPrice {stop_loss}");
                    Stop_Loss_Tickets.Add(sym, StopMarketOrder(sym, qty, stop_loss));
                }
                else
                {
                    Log($"STOPLOSS ACTIVATED ---{sym} has benn Liquidated by Market Order Placed at Price {Securities[sym].Price} for Quantity : {qty}");
                    MarketOrder(sym, qty, asynchronous: true);
                }

            }

            //1. When any Buy Order is placed
            //2. Check if Net Profit from that Symbol is POsitive or Negtaive
            //3. If Positive => We have Triggered Profit Order and Remove that Symbol
            //   from Profit_Target_Tickets Dictionary, so that at 03:15:00 P.M.
            //   when we Cancel the Non-Triggered Orders, we don't get Error from Broker.
            //4. If Negetive => We have Triggered Loss Order and Remove that Symbol
            //   from Stop_Loss_Tickets Dictionary, so that at 03:15:00 P.M.
            //   when we Cancel the Non-Triggered Orders, we don't get Error from Broker.
            if (orderEvent.Status == OrderStatus.Filled && orderEvent.Quantity > 0)
            {
                var sym = orderEvent.Symbol;
                var qty = orderEvent.Quantity;
                if (Portfolio[sym].NetProfit > 0)
                {
                    Log($"Liquidated in Profit for {sym} : Quantity : {qty} for Profit of : {Portfolio[sym].NetProfit}");
                }
                else
                {
                    Log($"StopLoss Triggered/Liquidated for {sym} : Quantity : {qty} for Loss of : {Portfolio[sym].NetProfit}");
                    Stop_Loss_Tickets.Remove(sym);
                }
                Sym_Quantity.Remove(sym);
                Log($"{sym}---->>POsition Exit");
            }
        }

        //Summary//
        //1. Show Current Status of Portfolio
        public void Show_Positions()
        {
            try
            {
                Log("========Custom Portfolio========");
                foreach (var kvp in Sym_Quantity)
                {
                    Log($"Symbol: {kvp.Key} -> Quantity: {kvp.Value}");
                }
                Log("================================");
            }
            catch (System.NullReferenceException)
            {
                Log("Custom Posrtfolio Not Yet Initialised");
            }

            Log("========Lean's Portfolio========");
            foreach (var kvp in Portfolio)
            {
                if (kvp.Value.Invested)
                {
                    Log($"Symbol: {kvp.Key} -> Quantity: {Sym_Quantity[kvp.Key]} -> Current Price: {kvp.Value.Price} -> Profit/Loss: {kvp.Value.UnrealizedProfit}");
                }
            }
            Log("================================");
        }

        //Summary//
        //1.Cancelling Non-Triggered Tickets for Short,Stop-Loss,Profit-Target.
        //2.If Portfolio has any Equity Left at 03:15:00 P.M. => Liquidating all
        public void Exit_Positions()
        {
            if (Sell_Tickets.Count > 0)
            {
                foreach (var kvp in Sell_Tickets)
                {

                    try
                    {
                        kvp.Value.Cancel();
                        Log($"----Stop Market Orders of {kvp.Value.Symbol} for short Cancelled-----");
                    }
                    catch
                    {
                        Log("Order Not Found");
                    }
                }
            }

            if (Stop_Loss_Tickets.Count > 0)
            {
                foreach (var kvp in Stop_Loss_Tickets)
                {
                    try
                    {
                        kvp.Value.Cancel();
                        Log($"----Stop Loss Orders of {kvp.Value.Symbol} for Exit Cancelled-----");
                    }
                    catch
                    {
                        Log("Order Not Found");
                    }
                }
            }

            foreach (var kvp in Sym_Quantity)
            {
                var qty = Math.Abs(kvp.Value);
                Log($"Liquidating Symbol: {kvp.Key} -> Quantity: {qty}");
                MarketOrder(kvp.Key, qty, asynchronous:true);
            }
        }
    }
}
