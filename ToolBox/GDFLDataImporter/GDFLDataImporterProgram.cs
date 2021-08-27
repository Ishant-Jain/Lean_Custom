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

namespace QuantConnect.ToolBox.GDFLDataImporter
{
    public class GDFLDataImporterProgram
    {
        private static string csv_dir; 
        private static List<DateTime> DateList;
        private static DataFrame Combined_Dataframe;

        /// <summary>
        /// Global Data Feeds Data Importer Toolbox Project For LEAN Algorithmic Trading Engine.
        /// By @Ishant_Jain
        /// </summary>
        public static void GDFLDataImporter(IList<string> tickers, string market, string resolution, string securityType, DateTime startDate, DateTime endDate)
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

            try
            {
                PrimitiveDataFrameColumn<DateTime> Timestamp = new PrimitiveDataFrameColumn<DateTime>("TimeStamp");
                Combined_Dataframe = new DataFrame();
                csv_dir = "D:\\GDFL\\2020\\STOCK\\Merged\\Preprocessed\\";
                var dataDirectory = Config.Get("data-folder", "../../../Data");

                if (startDate >= endDate)
                {
                    throw new ArgumentException("Invalid date range specified");
                }

                DateList = new List<DateTime>();
                for (var dt = startDate; dt < endDate; dt = dt.AddDays(1))
                {
                    DateList.Add(dt);
                }


                foreach (DateTime dt in DateList)
                {
                    string filename = csv_dir + dt.Date.ToString("ddMMyyyy") + ".csv";
                    if (File.Exists(filename))
                    {
                        DataFrame Daily_Data = DataFrame.LoadCsv(filename);
                        //Console.WriteLine($"{filename}====> Successfully Loaded");
                        if (Combined_Dataframe.Columns.Count == 0)
                        {
                            Combined_Dataframe = Daily_Data.Clone();
                            continue;
                        }
                        foreach (DataFrameRow line in Daily_Data.Rows)
                        {
                            Combined_Dataframe.Append(line, inPlace: true);
                        }
                    }
                }
                foreach (DataFrameRow line in Combined_Dataframe.Rows)
                {
                    var ts = new TimeSpan(0, 0, 59);
                    Timestamp.Append(DateTime.Parse((line[1].ToString()).Split(" ")[0] + " " + (line[2].ToString()).Split(" ")[1] + (line[2].ToString()).Split(" ")[2]).Subtract(ts));
                }
                var length = Combined_Dataframe.Rows.Count;
                StringDataFrameColumn symbol = new StringDataFrameColumn("symbol", length);

                var Ticker = Combined_Dataframe.Columns[0];
                int i = 0;
                foreach (string t in Ticker)
                {
                    string[] split_ticker = t.Split('.');
                    if (split_ticker.Count() > 2)
                    {
                        symbol[i] = split_ticker[0] + split_ticker[1];
                    }
                    else
                    {
                        symbol[i] = split_ticker[0];
                    }
                    
                    i++;
                }
                var Open = Combined_Dataframe.Columns[3];
                var High = Combined_Dataframe.Columns[4];
                var Low = Combined_Dataframe.Columns[5];
                var Close = Combined_Dataframe.Columns[6];
                var Volume = Combined_Dataframe.Columns[7];

                DataFrame Cleaned_Dataframe = new DataFrame(Timestamp,symbol, Open, High, Low, Close, Volume);
                {
                    foreach (String ticks in tickers)
                    {
                        var castResolution = (Resolution)Enum.Parse(typeof(Resolution), resolution);
                        var castSecurityType = (SecurityType)Enum.Parse(typeof(SecurityType), securityType);
                        var pairObject = Symbol.Create(ticks, castSecurityType, market);
                        var writer = new LeanDataWriter(castResolution, pairObject, dataDirectory);
                        IList<TradeBar> fileEnum = new List<TradeBar>();

                        if (castResolution == Resolution.Tick || castResolution == Resolution.Hour || castResolution == Resolution.Daily)
                        {
                            throw new ArgumentException("GDFL Doesn't Contain Data Other than Minute Resolution. Please Use Resolution.Minute");
                        }

                        if (pairObject.ID.SecurityType != SecurityType.Forex || pairObject.ID.SecurityType != SecurityType.Cfd || pairObject.ID.SecurityType != SecurityType.Crypto || pairObject.ID.SecurityType == SecurityType.Base)
                        {
                            if (pairObject.ID.SecurityType == SecurityType.Forex || pairObject.ID.SecurityType == SecurityType.Cfd || pairObject.ID.SecurityType == SecurityType.Crypto || pairObject.ID.SecurityType == SecurityType.Base)
                            {
                                throw new ArgumentException("Invalid security type: " + pairObject.ID.SecurityType);
                            }

                            try
                            {
                                PrimitiveDataFrameColumn<bool> Tick_Filter = Cleaned_Dataframe.Columns[1].ElementwiseEquals(ticks);
                                DataFrame selected_ticks = Cleaned_Dataframe.Filter(Tick_Filter);
                                DataFrame sorted = selected_ticks.OrderBy("TimeStamp");
                                foreach (DataFrameRow line in sorted.Rows)
                                {
                                    var ts = Convert.ToDateTime(line[0]);
                                    var open = Convert.ToDecimal(line[2]);
                                    var high = Convert.ToDecimal(line[3]);
                                    var low = Convert.ToDecimal(line[4]);
                                    var close = Convert.ToDecimal(line[5]);
                                    var volume = Convert.ToDecimal(line[6]);

                                    var linedata = new TradeBar(ts, pairObject, open, high, low, close, volume);
                                    fileEnum.Add(linedata);
                                }
                                Console.WriteLine($"======>Writing Data into Lean for {ticks}");
                                writer.Write(fileEnum);
                            }

                            catch
                            {
                                Console.WriteLine($"{ticks} not found in Database");
                                continue;
                            }
                        }
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
