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

using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Packets;
using QuantConnect.Securities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QuantConnect.Configuration;
using QuantConnect.Util;
using Timer = System.Timers.Timer;
using RestSharp;

namespace QuantConnect.Brokerages.Binance
{
    /// <summary>
    /// Binance brokerage implementation
    /// </summary>
    [BrokerageFactory(typeof(BinanceBrokerageFactory))]
    public partial class BinanceBrokerage : BaseWebsocketsBrokerage, IDataQueueHandler
    {

        private IAlgorithm _algorithm;
        private readonly SymbolPropertiesDatabaseSymbolMapper _symbolMapper = new SymbolPropertiesDatabaseSymbolMapper(Market.Binance);

        // Binance allows 5 messages per second, but we still get rate limited if we send a lot of messages at that rate
        // By sending 3 messages per second, evenly spaced out, we can keep sending messages without being limited
        private readonly RateGate _webSocketRateLimiter = new RateGate(1, TimeSpan.FromMilliseconds(330));
        private long _lastRequestId;

        private LiveNodePacket _job;
        private string _webSocketBaseUrl;
        private Timer _keepAliveTimer;
        private Timer _reconnectTimer;
        private BinanceRestApiClient _apiClient;
        private BrokerageConcurrentMessageHandler<WebSocketMessage> _messageHandler;

        private const int MaximumSymbolsPerConnection = 512;
        private bool _isInitialized;

        public BinanceBrokerage() : base("Binance")
        {
        }

        /// <summary>
        /// Constructor for brokerage
        /// </summary>
        /// <param name="apiKey">api key</param>
        /// <param name="apiSecret">api secret</param>
        /// <param name="restApiUrl">The rest api url</param>
        /// <param name="webSocketBaseUrl">The web socket base url</param>
        /// <param name="algorithm">the algorithm instance is required to retrieve account type</param>
        /// <param name="aggregator">the aggregator for consolidating ticks</param>
        /// <param name="job">The live job packet</param>
        public BinanceBrokerage(string apiKey, string apiSecret, string restApiUrl, string webSocketBaseUrl, IAlgorithm algorithm, IDataAggregator aggregator, LiveNodePacket job)
            : base("Binance")
        {
            Initialize(webSocketBaseUrl, restApiUrl, new WebSocketClientWrapper(), null, apiKey, apiSecret, null, algorithm, null, aggregator, job);
        }

        #region IBrokerage

        /// <summary>
        /// Checks if the websocket connection is connected or in the process of connecting
        /// </summary>
        public override bool IsConnected => WebSocket.IsOpen;

        /// <summary>
        /// Creates wss connection
        /// </summary>
        public override void Connect()
        {
            if (IsConnected)
                return;

            _apiClient.CreateListenKey();
            _reconnectTimer.Start();

            WebSocket.Initialize($"{_webSocketBaseUrl}/{_apiClient.SessionId}");

            base.Connect();
        }

        /// <summary>
        /// Closes the websockets connection
        /// </summary>
        public override void Disconnect()
        {
            _reconnectTimer.Stop();

            WebSocket?.Close();
            _apiClient.StopSession();
        }

        /// <summary>
        /// Gets all open positions
        /// </summary>
        /// <returns></returns>
        public override List<Holding> GetAccountHoldings()
        {
            if (_algorithm.BrokerageModel.AccountType == AccountType.Cash)
            {
                return base.GetAccountHoldings(_job?.BrokerageData, _algorithm.Securities.Values);
            }
            return _apiClient.GetAccountHoldings();
        }

        /// <summary>
        /// Gets the total account cash balance for specified account type
        /// </summary>
        /// <returns></returns>
        public override List<CashAmount> GetCashBalance()
        {
            var account = _apiClient.GetCashBalance();
            var balances = account.Balances?.Where(balance => balance.Amount > 0).ToList();
            if (balances == null || !balances.Any())
                return new List<CashAmount>();

            return balances
                .Select(b => new CashAmount(b.Amount, b.Asset.LazyToUpper()))
                .ToList();
        }

        /// <summary>
        /// Gets all orders not yet closed
        /// </summary>
        /// <returns></returns>
        public override List<Order> GetOpenOrders()
        {
            var orders = _apiClient.GetOpenOrders();
            List<Order> list = new List<Order>();
            foreach (var item in orders)
            {
                Order order;
                switch (item.Type.LazyToUpper())
                {
                    case "MARKET":
                        order = new MarketOrder { Price = item.Price };
                        break;
                    case "LIMIT":
                    case "LIMIT_MAKER":
                        order = new LimitOrder { LimitPrice = item.Price };
                        break;
                    case "STOP_LOSS":
                    case "TAKE_PROFIT":
                        order = new StopMarketOrder { StopPrice = item.StopPrice, Price = item.Price };
                        break;
                    case "STOP_LOSS_LIMIT":
                    case "TAKE_PROFIT_LIMIT":
                        order = new StopLimitOrder { StopPrice = item.StopPrice, LimitPrice = item.Price };
                        break;
                    default:
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, -1,
                            "BinanceBrokerage.GetOpenOrders: Unsupported order type returned from brokerage: " + item.Type));
                        continue;
                }

                order.Quantity = item.Quantity;
                order.BrokerId = new List<string> { item.Id };
                order.Symbol = _symbolMapper.GetLeanSymbol(item.Symbol, SecurityType.Crypto, Market.Binance);
                order.Time = Time.UnixMillisecondTimeStampToDateTime(item.Time);
                order.Status = ConvertOrderStatus(item.Status);
                order.Price = item.Price;

                if (order.Status.IsOpen())
                {
                    var cached = CachedOrderIDs.Where(c => c.Value.BrokerId.Contains(order.BrokerId.First())).ToList();
                    if (cached.Any())
                    {
                        CachedOrderIDs[cached.First().Key] = order;
                    }
                }

                list.Add(order);
            }

            return list;
        }

        /// <summary>
        /// Places a new order and assigns a new broker ID to the order
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <returns>True if the request for a new order has been placed, false otherwise</returns>
        public override bool PlaceOrder(Order order)
        {
            var submitted = false;

            _messageHandler.WithLockedStream(() =>
            {
                submitted = _apiClient.PlaceOrder(order);
            });

            return submitted;
        }

        /// <summary>
        /// Updates the order with the same id
        /// </summary>
        /// <param name="order">The new order information</param>
        /// <returns>True if the request was made for the order to be updated, false otherwise</returns>
        public override bool UpdateOrder(Order order)
        {
            throw new NotSupportedException("BinanceBrokerage.UpdateOrder: Order update not supported. Please cancel and re-create.");
        }

        /// <summary>
        /// Cancels the order with the specified ID
        /// </summary>
        /// <param name="order">The order to cancel</param>
        /// <returns>True if the request was submitted for cancellation, false otherwise</returns>
        public override bool CancelOrder(Order order)
        {
            var submitted = false;

            _messageHandler.WithLockedStream(() =>
            {
                submitted = _apiClient.CancelOrder(order);
            });

            return submitted;
        }

        /// <summary>
        /// Gets the history for the requested security
        /// </summary>
        /// <param name="request">The historical data request</param>
        /// <returns>An enumerable of bars covering the span specified in the request</returns>
        public override IEnumerable<BaseData> GetHistory(Data.HistoryRequest request)
        {
            if (request.Resolution == Resolution.Tick || request.Resolution == Resolution.Second)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "InvalidResolution",
                    $"{request.Resolution} resolution is not supported, no history returned"));
                yield break;
            }

            if (request.TickType != TickType.Trade)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, "InvalidTickType",
                    $"{request.TickType} tick type not supported, no history returned"));
                yield break;
            }

            var period = request.Resolution.ToTimeSpan();

            foreach (var kline in _apiClient.GetHistory(request))
            {
                yield return new TradeBar()
                {
                    Time = Time.UnixMillisecondTimeStampToDateTime(kline.OpenTime),
                    Symbol = request.Symbol,
                    Low = kline.Low,
                    High = kline.High,
                    Open = kline.Open,
                    Close = kline.Close,
                    Volume = kline.Volume,
                    Value = kline.Close,
                    DataType = MarketDataType.TradeBar,
                    Period = period
                };
            }
        }

        /// <summary>
        /// Wss message handler
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        protected override void OnMessage(object sender, WebSocketMessage e)
        {
            _messageHandler.HandleNewMessage(e);
        }

        #endregion

        #region IDataQueueHandler

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            Initialize(job.BrokerageData["binance-websocket-url"], job.BrokerageData["binance-api-url"], new WebSocketClientWrapper(), 
                null, job.BrokerageData["binance-api-key"], job.BrokerageData["binance-api-secret"],
                null, null, null,
                Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager")), 
                job);
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
            SubscriptionManager.Subscribe(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            SubscriptionManager.Unsubscribe(dataConfig);
            _aggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Checks if this brokerage supports the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>returns true if brokerage supports the specified symbol; otherwise false</returns>
        private static bool CanSubscribe(Symbol symbol)
        {
            return !symbol.Value.Contains("UNIVERSE") &&
                   symbol.SecurityType == SecurityType.Crypto;
        }

        #endregion

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            _keepAliveTimer.DisposeSafely();
            _reconnectTimer.DisposeSafely();
            _apiClient.DisposeSafely();
            _webSocketRateLimiter.DisposeSafely();
        }

        /// <summary>
        /// Not used
        /// </summary>
        protected override bool Subscribe(IEnumerable<Symbol> symbols)
        {
            // NOP
            return true;
        }

        protected override void Initialize(string wssUrl, string restApiUrl, IWebSocket websocket, IRestClient restClient,
            string apiKey, string apiSecret, string passPhrase, IAlgorithm algorithm, IPriceProvider priceProvider,
            IDataAggregator aggregator, LiveNodePacket job)
        {
            if (!_isInitialized)
            {
                base.Initialize(wssUrl, restApiUrl, websocket, restClient, apiKey, apiSecret,
                passPhrase, algorithm, priceProvider, aggregator, job);
                _job = job;
                _algorithm = algorithm;
                _aggregator = aggregator;
                _webSocketBaseUrl = wssUrl;
                _messageHandler = new BrokerageConcurrentMessageHandler<WebSocketMessage>(OnUserMessage);

                var maximumWebSocketConnections = Config.GetInt("binance-maximum-websocket-connections");
                var symbolWeights = maximumWebSocketConnections > 0 ? FetchSymbolWeights() : null;

                var subscriptionManager = new BrokerageMultiWebSocketSubscriptionManager(
                    wssUrl,
                    MaximumSymbolsPerConnection,
                    maximumWebSocketConnections,
                    symbolWeights,
                    () => new BinanceWebSocketWrapper(null),
                    Subscribe,
                    Unsubscribe,
                    OnDataMessage,
                    new TimeSpan(23, 45, 0));

                SubscriptionManager = subscriptionManager;

                _apiClient = new BinanceRestApiClient(_symbolMapper,
                    algorithm?.Portfolio,
                    apiKey,
                    apiSecret,
                    restApiUrl);

                _apiClient.OrderSubmit += (s, e) => OnOrderSubmit(e);
                _apiClient.OrderStatusChanged += (s, e) => OnOrderEvent(e);
                _apiClient.Message += (s, e) => OnMessage(e);

                // User data streams will close after 60 minutes. It's recommended to send a ping about every 30 minutes.
                // Source: https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md#pingkeep-alive-a-listenkey
                _keepAliveTimer = new Timer
                {
                    // 30 minutes
                    Interval = 30 * 60 * 1000
                };
                _keepAliveTimer.Elapsed += (s, e) => _apiClient.SessionKeepAlive();

                WebSocket.Open += (s, e) => { _keepAliveTimer.Start(); };
                WebSocket.Closed += (s, e) => { _keepAliveTimer.Stop(); };

                // A single connection to stream.binance.com is only valid for 24 hours; expect to be disconnected at the 24 hour mark
                // Source: https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#general-wss-information
                _reconnectTimer = new Timer
                {
                    // 23.5 hours
                    Interval = 23.5 * 60 * 60 * 1000
                };
                _reconnectTimer.Elapsed += (s, e) =>
                {
                    Log.Trace("Daily websocket restart: disconnect");
                    Disconnect();

                    Log.Trace("Daily websocket restart: connect");
                    Connect();
                };
                _isInitialized = true;
            }
        }

        /// <summary>
        /// Subscribes to the requested symbol (using an individual streaming channel)
        /// </summary>
        /// <param name="webSocket">The websocket instance</param>
        /// <param name="symbol">The symbol to subscribe</param>
        private bool Subscribe(IWebSocket webSocket, Symbol symbol)
        {
            Send(webSocket,
                new
                {
                    method = "SUBSCRIBE",
                    @params = new[]
                    {
                        $"{symbol.Value.ToLowerInvariant()}@trade",
                        $"{symbol.Value.ToLowerInvariant()}@bookTicker"
                    },
                    id = GetNextRequestId()
                }
            );

            return true;
        }

        /// <summary>
        /// Ends current subscription
        /// </summary>
        /// <param name="webSocket">The websocket instance</param>
        /// <param name="symbol">The symbol to unsubscribe</param>
        private bool Unsubscribe(IWebSocket webSocket, Symbol symbol)
        {
            Send(webSocket,
                new
                {
                    method = "UNSUBSCRIBE",
                    @params = new[]
                    {
                        $"{symbol.Value.ToLowerInvariant()}@trade",
                        $"{symbol.Value.ToLowerInvariant()}@bookTicker"
                    },
                    id = GetNextRequestId()
                }
            );

            return true;
        }

        private void Send(IWebSocket webSocket, object obj)
        {
            var json = JsonConvert.SerializeObject(obj);

            _webSocketRateLimiter.WaitToProceed();

            Log.Trace("Send: " + json);

            webSocket.Send(json);
        }

        private long GetNextRequestId()
        {
            return Interlocked.Increment(ref _lastRequestId);
        }

        /// <summary>
        /// Event invocator for the OrderFilled event
        /// </summary>
        /// <param name="e">The OrderEvent</param>
        private void OnOrderSubmit(BinanceOrderSubmitEventArgs e)
        {
            var brokerId = e.BrokerId;
            var order = e.Order;
            if (CachedOrderIDs.ContainsKey(order.Id))
            {
                CachedOrderIDs[order.Id].BrokerId.Clear();
                CachedOrderIDs[order.Id].BrokerId.Add(brokerId);
            }
            else
            {
                order.BrokerId.Add(brokerId);
                CachedOrderIDs.TryAdd(order.Id, order);
            }
        }

        /// <summary>
        /// Returns the weights for each symbol (the weight value is the count of trades in the last 24 hours)
        /// </summary>
        private static Dictionary<Symbol, int> FetchSymbolWeights()
        {
            var dict = new Dictionary<Symbol, int>();

            try
            {
                const string url = "https://api.binance.com/api/v3/ticker/24hr";
                var json = url.DownloadData();

                foreach (var row in JArray.Parse(json))
                {
                    var ticker = row["symbol"].ToObject<string>();
                    var count = row["count"].ToObject<int>();

                    var symbol = Symbol.Create(ticker, SecurityType.Crypto, Market.Binance);

                    dict.Add(symbol, count);
                }
            }
            catch (Exception exception)
            {
                Log.Error(exception);
                throw;
            }

            return dict;
        }
    }
}
