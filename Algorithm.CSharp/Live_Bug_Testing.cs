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
    public class Live_Bug_Testing : QCAlgorithm
    {
        private static TimeSpan fill_time;
        public override void Initialize()
        {
            SetStartDate(2020, 01, 02); //Set Start Date
            SetEndDate(2020, 01, 05);
            SetTimeZone("Asia/Calcutta"); //Set End Date
            SetAccountCurrency("INR");
            SetBrokerageModel(Brokerages.BrokerageName.Zerodha);
            SetCash(989000);

            AddEquity("CONCOR", Resolution.Minute, Market.India);

            DefaultOrderProperties = new ZerodhaOrderProperties(exchange:Exchange.NSE);

        }

        public override void OnData(Slice slice)
        {
            //fill_time = new DateTime(2020,01,01,00,00,00).TimeOfDay;
            if (!Portfolio["CONCOR"].Invested)
            {
                MarketOrder("CONCOR", -54, asynchronous: true);
                fill_time = Time.TimeOfDay;
                Log($"Market Order PLaced for CONCOR at {fill_time}");
            }

            Console.WriteLine($"                     ");
            Console.WriteLine($"<<===============Portfolio Contains===============>>");
            Show_Positions();
            Console.WriteLine($"<<================================================>>");

            if (Time.TimeOfDay - fill_time > TimeSpan.Parse("00:02:00") & Portfolio["CONCOR"].Invested)
            {
                Log($"Liquidating CONCOR at {Time.TimeOfDay}");
                Console.WriteLine($"<<===============Portfolio Contains===============>>");
                Show_Positions();
                Console.WriteLine($"<<================================================>>");
                Liquidate();
                Quit();
            }

        }


        public void Show_Positions()
        {
            foreach (var kvp in Portfolio)
            {
                if (kvp.Value.Invested)
                {
                    Console.WriteLine($"Symbol: {kvp.Key} -> Quantity: {kvp.Value.Quantity}");
                }
            }
        }
    }
}
