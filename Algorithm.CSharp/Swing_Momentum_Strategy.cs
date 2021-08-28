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
    public class Swing_Momentum_Strategy : QCAlgorithm
    {
        private static Symbol sym;
        private static QuantConnect.Indicators.RelativeStrengthIndex rsi;
        public override void Initialize()
        {
            SetStartDate(2020, 01, 01); //Set Start Date
            SetEndDate(2020, 01, 31);
            SetTimeZone("Asia/Calcutta"); //Set End Date
            SetAccountCurrency("INR");
            SetBrokerageModel(Brokerages.BrokerageName.Zerodha);
            SetCash(10000000);
            
            sym = AddEquity("CONCOR", Resolution.Minute, Market.India).Symbol;
            var hourly = new TradeBarConsolidator(TimeSpan.FromMinutes(60));
            hourly.DataConsolidated += OnHour;
            SubscriptionManager.AddConsolidator("CONCOR", hourly);
            rsi = RSI("CONCOR", 2, MovingAverageType.Simple, Resolution.Hour);

            DefaultOrderProperties = new ZerodhaOrderProperties(exchange: Exchange.NSE);
        }

        public void OnHour(object sender, TradeBar bar)
        {
            Log(Time.ToString("u") + " " + bar.Close + "Rsi is " + rsi);
        }

        public override void OnData(Slice slice)
        {
            if (slice != null)
            {
                //Log($"At Time: {Time} the Close Price is=> {} having RSI : {rsi}");
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
                    //Log($"Symbol: {kvp.Key} -> Quantity: {Sym_Quantity[kvp.Key]} -> Current Price: {kvp.Value.Price} -> Profit/Loss: {kvp.Value.UnrealizedProfit}");
                }
            }
        }

    }
}
