using System.Collections.Generic;
using System;
using QuantConnect.Data.Consolidators;
using QuantConnect.Indicators;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Algorithm.Selection;
using QuantConnect.Algorithm.Framework.Selection;
using QuantConnect.Data.UniverseSelection;
using System.Linq;
using QuantConnect.Orders;
using MathNet;
using QuantConnect.Data.Market;

namespace QuantConnect.Algorithm.CSharp
{
    public class Three_Days_Momentum_Strategy : QCAlgorithm
    {
        private List<string> tickers;
        QuantConnect.Symbol[] symbollist;
        private static DateTime day_1;
        private static DateTime day_2;
        private static DateTime day_3;
        private static decimal day_1_close;
        private static decimal day_2_close;
        private static decimal day_3_close;
        private static decimal current_close;
        private static Dictionary<Symbol,List<decimal>> Filtered_Sym;
        private static Dictionary<Symbol, int> Sym_Quantity;
        public override void Initialize()
        {
            SetStartDate(2020, 01, 01); //Set Start Date
            SetEndDate(2020, 12, 31);
            SetTimeZone("Asia/Calcutta"); //Set End Date
            SetAccountCurrency("INR");
            SetBrokerageModel(Brokerages.BrokerageName.Zerodha);
            SetCash(10000000);

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

            DefaultOrderProperties = new ZerodhaOrderProperties(exchange: Exchange.NSE);
            Sym_Quantity = new Dictionary<Symbol, int>();

            Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                TimeRules.At(09, 16, 0), () => { Exit_Position(); });
            Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                TimeRules.At(15, 29, 0), () => { Calc_Perc(); });

        }

        public void Calc_Perc()
        {
            Filtered_Sym = new Dictionary<Symbol, List<decimal>> { };
            Log($"Fuction Activated for {Time}");
            day_1 = Time - TimeSpan.FromDays(3);
            day_2 = Time - TimeSpan.FromDays(2);
            day_3 = Time - TimeSpan.FromDays(1);
            foreach (var symb in symbollist)
            {
                var history = History(symb, 750 + 374, Resolution.Minute);

                foreach (var slice in history)
                {
                    if (slice.EndTime == day_1)
                    {
                        day_1_close = slice.Close;
                    }

                    if (slice.EndTime == day_2)
                    {
                        day_2_close = slice.Close;
                    }

                    if (slice.EndTime == day_3)
                    {
                        day_3_close = slice.Close;
                    }

                    if (slice.EndTime == Time - TimeSpan.FromMinutes(1))
                    {
                        current_close = slice.Close;
                    }
                }
                decimal first_perc_gain = 0;
                decimal second_perc_gain = 0;
                decimal third_perc_gain = 0;
                if (day_1_close != 0 && day_2_close != 0 && current_close != 0)
                {
                    first_perc_gain = (day_2_close / day_1_close - 1) * 100;
                    second_perc_gain = (day_3_close / day_2_close - 1) * 100;
                    third_perc_gain = (current_close / day_3_close - 1) * 100;
                    //Log($"First Day Perc Gain for {symb} = {(day_2_close / day_1_close - 1) * 100}");
                    //Log($"Second Day Perc Gain for {symb} = {(current_close / day_2_close - 1) * 100}");
                }

                if (third_perc_gain > second_perc_gain && second_perc_gain > first_perc_gain)
                {
                    var gain_close = new List<decimal>() { third_perc_gain - second_perc_gain, current_close };
                    Filtered_Sym.Add(symb, gain_close);
                }
            }

            if (Filtered_Sym.Count > 0)
            {
                var Sortedgain = Filtered_Sym.OrderByDescending(x => x.Value[0]);
                var temp_list = Sortedgain.Take(5).Select(x => x.Key).ToList();

                int i = 1;
                Log($"The top 5 Gainer for {Time} :");
                foreach (var item in temp_list)
                {
                    Log($"{i}. {item} => {Filtered_Sym[item][0]}");
                    int qty = Convert.ToInt32(Portfolio.Cash / (temp_list.Count * Filtered_Sym[item][1]));
                    MarketOrder(item, qty);
                    Sym_Quantity.Add(item, qty);
                    Log($"Market order Placed for {item} =>Quantity {qty}");
                    i++;
                }

            }
        }

        public void Exit_Position()
        {
            if (Sym_Quantity.Count() > 0)
            {
                foreach (var kvp in Sym_Quantity)
                {
                    var sym = kvp.Key;
                    var qty = kvp.Value * -1;
                    MarketOrder(sym, qty);
                    Log($"{sym} Liquidated for Quantity {qty}");
                    Sym_Quantity.Remove(sym);
                }
            }
        }

        public override void OnData(Slice slice)
        {
            if (slice != null && Time.TimeOfDay == DateTime.Parse("2021-01-01 09:16:00").TimeOfDay)
            {
                Log("Portfolio in Morning");
                Show_Positions();
            }
            if (slice != null && Time.TimeOfDay >= DateTime.Parse("2021-01-01 15:28:00").TimeOfDay)
            {
                Log("Portfolio in Evening");
                Show_Positions();
            }

        }


        //Summary//
        //1. Show Current Status of Portfolio
        public void Show_Positions()
        {
            foreach (var kvp in Portfolio)
            {
                if (kvp.Value.Invested)
                {
                    Log($"Symbol: {kvp.Key} -> Quantity: {kvp.Value.Quantity} -> Current Price: {kvp.Value.Price} -> Profit/Loss: {kvp.Value.UnrealizedProfit}");
                }
            }
        }

    }
}
