/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Data.Analysis;
using System.Globalization;
using NodaTime;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Util;
using QuantConnect.Securities;

namespace QuantConnect.ToolBox.TrueDataImporter
{
    public class TrueDataImporterProgram
    {
        private static string csv_dir; 
        private static List<DateTime> DateList;
        private static DataFrame Sym_DataFrame;

        /// <summary>
        /// Global Data Feeds Data Importer Toolbox Project For LEAN Algorithmic Trading Engine.
        /// By @Ishant_Jain
        /// By @Ishant_Jain
        /// </summary>
        public static void TrueDataImporter(IList<string> tickers, string market, string resolution, string securityType, DateTime startDate, DateTime endDate)
        {
            
            if (resolution.IsNullOrEmpty() || tickers.IsNullOrEmpty())
            {
                Console.WriteLine("GDFLDataImporter ERROR: '--tickers=', --securityType, '--market' or '--resolution=' parameter is missing");
                Console.WriteLine("--tickers=eg JSWSTEEL,TCS,INFY");
                Console.WriteLine("--market=MCX/NSE/NFO/CDS/BSE");
                Console.WriteLine("--security-type=Equity/Future/Option/Commodity");
                Console.WriteLine("--resolution=Minute/Hour/Daily/Tick");
                Environment.Exit(1);
            }

            if (startDate >= endDate)
            {
                throw new ArgumentException("Invalid date range specified");
            }

            try
            {
                csv_dir = "D:\\True Data\\EQ_2020\\Updated\\";
                var dataDirectory = Config.Get("data-folder", "../../../Data");

                PrimitiveDataFrameColumn<DateTime> Timestamp = new PrimitiveDataFrameColumn<DateTime>("Timestamp");
                PrimitiveDataFrameColumn<decimal> Open = new PrimitiveDataFrameColumn<decimal>("Open");
                PrimitiveDataFrameColumn<decimal> High = new PrimitiveDataFrameColumn<decimal>("High");
                PrimitiveDataFrameColumn<decimal> Low = new PrimitiveDataFrameColumn<decimal>("Low");
                PrimitiveDataFrameColumn<decimal> Close = new PrimitiveDataFrameColumn<decimal>("Close");
                PrimitiveDataFrameColumn<double> Volume = new PrimitiveDataFrameColumn<double>("Volume");

                foreach (var ticks in tickers)
                {
                    var castResolution = (Resolution)Enum.Parse(typeof(Resolution), resolution);
                    var castSecurityType = (SecurityType)Enum.Parse(typeof(SecurityType), securityType);
                    var pairObject = Symbol.Create(ticks, castSecurityType, market);
                    var writer = new LeanDataWriter(castResolution, pairObject, dataDirectory);
                    IList<TradeBar> fileEnum = new List<TradeBar>();

                    if (castResolution == Resolution.Tick || castResolution == Resolution.Hour || castResolution == Resolution.Daily)
                    {
                        throw new ArgumentException("True Data Doesn't Contain Data Other than Minute Resolution. Please Use Resolution.Minute");
                    }

                    if (pairObject.ID.SecurityType != SecurityType.Forex || pairObject.ID.SecurityType != SecurityType.Cfd || pairObject.ID.SecurityType != SecurityType.Crypto || pairObject.ID.SecurityType == SecurityType.Base)
                    {
                        if (pairObject.ID.SecurityType == SecurityType.Forex || pairObject.ID.SecurityType == SecurityType.Cfd || pairObject.ID.SecurityType == SecurityType.Crypto || pairObject.ID.SecurityType == SecurityType.Base)
                        {
                            throw new ArgumentException("Invalid security type: " + pairObject.ID.SecurityType);
                        }

                        string filename = csv_dir + ticks.ToString() + ".csv";
                        Console.WriteLine($"From Path ===> {filename}");
                        DataFrame Sym_Dataframe = DataFrame.LoadCsv(filename);

                        foreach (DataFrameRow line in Sym_Dataframe.OrderBy("Timestamp").Rows)
                        {
                            if (DateTime.Parse(line[0].ToString()) >= startDate && DateTime.Parse(line[0].ToString()) < endDate)
                            {
                                Timestamp.Append(DateTime.Parse(line[0].ToString()));
                                Open.Append(Convert.ToDecimal(line[1]));
                                High.Append(Convert.ToDecimal(line[2]));
                                Low.Append(Convert.ToDecimal(line[3]));
                                Close.Append(Convert.ToDecimal(line[4]));
                                Volume.Append(Convert.ToDouble(line[5]));
                            }
                        }

                        DataFrame Filtered_Dataframe = new DataFrame(Timestamp, Open, High, Low, Close, Volume);
                        foreach (DataFrameRow line in Filtered_Dataframe.OrderBy("Timestamp").Rows)
                        {
                            var ts = Convert.ToDateTime(line[0]);
                            var open = Convert.ToDecimal(line[1]);
                            var high = Convert.ToDecimal(line[2]);
                            var low = Convert.ToDecimal(line[3]);
                            var close = Convert.ToDecimal(line[4]);
                            var volume = Convert.ToDecimal(line[5]);

                            var linedata = new TradeBar(ts, pairObject, open, high, low, close, volume);
                            fileEnum.Add(linedata);
                        }
                        Console.WriteLine($"======>Writing Data into Lean for {ticks}");
                        writer.Write(fileEnum);
                    }

                }

            }

            catch (Exception err)
            {
                Log.Error($"Message: {err.Message} Exception: {err.InnerException}");
            }

        }
    }
}
