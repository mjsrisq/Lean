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
using System.Collections.Generic;
using QuantConnect.Data;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Market;
using QuantConnect.Indicators;
using QuantConnect.Interfaces;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Demostrates the use of <see cref="VolumeRenkoConsolidator"/> for creating constant volume bar
    /// </summary>
    /// <meta name="tag" content="renko" />
    /// <meta name="tag" content="using data" />
    /// <meta name="tag" content="consolidating data" />
    public class VolumeRenkoConsolidatorAlgorithm : QCAlgorithm, IRegressionAlgorithmDefinition
    {
        private Symbol _spy, _ibm;
        private VolumeRenkoConsolidator _tradebarVolumeConsolidator, _tickVolumeConsolidator;
        private SimpleMovingAverage _sma = new SimpleMovingAverage(10);
        private bool _tickConsolidated = false;

        public override void Initialize()
        {
            SetStartDate(2013, 10, 7);
            SetEndDate(2013, 10, 11);
            SetCash(100000);

            _spy = AddEquity("SPY", Resolution.Minute).Symbol;
            _tradebarVolumeConsolidator = new VolumeRenkoConsolidator(1000000);
            _tradebarVolumeConsolidator.DataConsolidated += (sender, bar) => {
                _sma.Update(bar.EndTime, bar.Value);
                Debug($"SPY {bar.Time} to {bar.EndTime} :: O:{bar.Open} H:{bar.High} L:{bar.Low} C:{bar.Close} V:{bar.Volume}");
                if (bar.Volume != 1000000)
                {
                    throw new Exception("Volume of consolidated bar does not match set value!");
                }
            };

            _ibm = AddEquity("IBM", Resolution.Tick).Symbol;
            _tickVolumeConsolidator = new VolumeRenkoConsolidator(1000000);
            _tickVolumeConsolidator.DataConsolidated += (sender, bar) => {
                Debug($"IBM {bar.Time} to {bar.EndTime} :: O:{bar.Open} H:{bar.High} L:{bar.Low} C:{bar.Close} V:{bar.Volume}");
                if (bar.Volume != 1000000)
                {
                    throw new Exception("Volume of consolidated bar does not match set value!");
                }
                _tickConsolidated = true;
            };

            var history = History<TradeBar>(new[] {_spy}, 1000, Resolution.Minute);
            foreach (var slice in history)
            {
                _tradebarVolumeConsolidator.Update(slice[_spy]);
            }
        }

        public override void OnData(Slice slice)
        {
            // Update by TradeBar
            if (slice.Bars.ContainsKey(_spy))
            {
                _tradebarVolumeConsolidator.Update(slice.Bars[_spy]);
            }

            // Update by Tick
            if (slice.Ticks.ContainsKey(_ibm))
            {
                foreach (var tick in slice.Ticks[_ibm])
                {
                    _tickVolumeConsolidator.Update(tick);
                }
            }

            if (_sma.IsReady && _sma.Current.Value < Securities[_spy].Price)
            {
                SetHoldings(_spy, 1m);
            }
            else
            {
                SetHoldings(_spy, 0m);
            }
        }

        public override void OnEndOfAlgorithm()
        {
            if (!_tickConsolidated)
            {
                throw new Exception("Tick consolidator was never been called");
            }
        }

        /// <summary>
        /// This is used by the regression test system to indicate if the open source Lean repository has the required data to run this algorithm.
        /// </summary>
        public bool CanRunLocally { get; } = true;

        /// <summary>
        /// This is used by the regression test system to indicate which languages this algorithm is written in.
        /// </summary>
        public Language[] Languages { get; } = { Language.CSharp, Language.Python };

        /// <summary>
        /// Data Points count of all timeslices of algorithm
        /// </summary>
        public long DataPoints => 698706;

        /// <summary>
        /// Data Points count of the algorithm history
        /// </summary>
        public int AlgorithmHistoryDataPoints => 390;

        /// <summary>
        /// This is used by the regression test system to indicate what the expected statistics are from running the algorithm
        /// </summary>
        public Dictionary<string, string> ExpectedStatistics => new Dictionary<string, string>
        {
            {"Total Trades", "227"},
            {"Average Win", "0.25%"},
            {"Average Loss", "-0.05%"},
            {"Compounding Annual Return", "-48.349%"},
            {"Drawdown", "3.000%"},
            {"Expectancy", "-0.191"},
            {"Net Profit", "-0.841%"},
            {"Sharpe Ratio", "-0.956"},
            {"Probabilistic Sharpe Ratio", "41.341%"},
            {"Loss Rate", "87%"},
            {"Win Rate", "13%"},
            {"Profit-Loss Ratio", "5.15"},
            {"Alpha", "-2.225"},
            {"Beta", "1.009"},
            {"Annual Standard Deviation", "0.234"},
            {"Annual Variance", "0.055"},
            {"Information Ratio", "-33.229"},
            {"Tracking Error", "0.066"},
            {"Treynor Ratio", "-0.222"},
            {"Total Fees", "$767.26"},
            {"Estimated Strategy Capacity", "$4000000.00"},
            {"Lowest Capacity Asset", "SPY R735QTJ8XC9X"},
            {"Return Over Maximum Drawdown", "-19.135"},
            {"Portfolio Turnover", "47.249"},
            {"Total Insights Generated", "0"},
            {"Total Insights Closed", "0"},
            {"Total Insights Analysis Completed", "0"},
            {"Long Insight Count", "0"},
            {"Short Insight Count", "0"},
            {"Long/Short Ratio", "100%"},
            {"OrderListHash", "df05ecec41d8973c2ad3d85e015e652a"}
        };
    }
}
