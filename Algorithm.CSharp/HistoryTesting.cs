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
    public class HistoryTesting : QCAlgorithm
    {
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

            tickers = new List<string>() {"SRTRANSFIN","SUNPHARMA"};


            symbollist = new QuantConnect.Symbol[tickers.Count];
            int i = 0;
            foreach (string t in tickers)
            {
                var temp = AddEquity(t, Resolution.Minute, Market.India);
                symbollist[i] = temp.Symbol;
                //temp.SetLeverage(5);
                i++;
            }

            
            PerformFilter();
            //Schedule.On(DateRules.Every(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
                //TimeRules.At(15, 15, 0), () => { Liquidate(); });

        }


        public void PerformFilter()
        {
            Log("Starting to process scheduled event : Filter");
            Log($"Historical Data for : {Time.Date}");
            foreach (var symb in symbollist)
            {
                var history = History(symb, 375 + 45, Resolution.Minute);
                foreach (var slice in history)
                {
                    var stime = new DateTime(2021, 07, 20, 15, 30, 00).TimeOfDay;
                    if (slice.EndTime.TimeOfDay == stime)
                    {
                    }
                    Log($"History of {symb} at {Time.Date} is : Starttime : {slice.Time} -- EndTime :{slice.EndTime} --- Close :{slice.Close} -- IsfillForward : {slice.IsFillForward}");
                }


            }

        }

        public override void OnData(Slice slice)
        {
            if (slice != null)
            {
                //
            }

        }
    }
}
