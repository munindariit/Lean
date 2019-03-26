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

using QuantConnect.Algorithm.Framework;
using QuantConnect.Algorithm.Framework.Alphas;
using QuantConnect.Algorithm.Framework.Execution;
using QuantConnect.Algorithm.Framework.Portfolio;
using QuantConnect.Algorithm.Framework.Risk;
using QuantConnect.Algorithm.Framework.Selection;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using System.Collections.Generic;
using System.Linq;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Alpha Stream Template Algorithm uses framework components to define the algorithm.
    /// In this template, three popular options for Universe Selection Model are presented and
    /// the default Portfolio Construction, Execution and Risk Management models developed by
    /// the QuantConnect Team are selected.
    /// Finally, the skeleton of an Alpha Model is included for a quick start.
    /// </summary>
    public class AlphaStreamTemplateAlgorithm : QCAlgorithmFramework
    {
        /// <summary>
        /// Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.
        /// </summary>
        public override void Initialize()
        {
            SetStartDate(2017, 1, 1);   //Set Start Date
            SetCash(1000000);           //Set Strategy Cash

            // Set zero transaction fees
            SetSecurityInitializer(security => security.FeeModel = new ConstantFeeModel(0));

            // Set requested data resolution
            UniverseSettings.Resolution = Resolution.Minute;

            // Set algorithm framework models
            // Popular Option #1:
            // Sets a list of symbols manually
            var symbols = new[] { "SPY", "MSFT", "AAPL" }
                .Select(x => QuantConnect.Symbol.Create(x, SecurityType.Equity, Market.USA));
            SetUniverseSelection(new ManualUniverseSelectionModel(symbols));

            /*
            // Popular Option #2:
            // Use coarse universe selection
            SetUniverseSelection(new CoarseFundamentalUniverseSelectionModel(coarse =>
            {
                // Select securities wih fundamental data and price above $5
                // Take 50 securities with the highest dollar volume
                return (from cf in coarse
                        where cf.HasFundamentalData
                        where cf.Price > 5
                        orderby cf.DollarVolume descending
                        select cf.Symbol
                        ).Take(50);
            }));

            // Popular Option #3:
            // Use universe helper
            SetUniverseSelection(new QC500UniverseSelectionModel());
            */

            // Use your Custom Alpha Model
            SetAlpha(new AlphaModelTemplate());

            // Equally weigh securities in portfolio, based on insights
            SetPortfolioConstruction(new EqualWeightingPortfolioConstructionModel());

            // Set Immediate Execution Model
            SetExecution(new ImmediateExecutionModel());

            // Set Null Risk Management Model
            SetRiskManagement(new NullRiskManagementModel());
        }

        /// <summary>
        /// Alpha Model Template. In this template, insights are not emitted.
        /// </summary>
        private class AlphaModelTemplate : AlphaModel
        {
            private readonly HashSet<Security> _securities;

            /// <summary>
            /// Initialize an new instance of the AlphaModelTemplate
            /// </summary>
            public AlphaModelTemplate()
            {
                _securities = new HashSet<Security>();
            }

            /// <summary>
            /// AlphaModel.Update is the primary entry point for your algorithm. 
            /// Each new data point will be pumped in here and can be used to generate insights.
            /// Insights are the most valuable piece of information for hedge funds and, 
            /// in the algorithms context, they are used to construct your portfolio.
            /// </summary>
            /// <param name="algorithm">The algorithm instance</param>
            /// <param name="data">Slice object keyed by symbol containing the stock data</param>
            /// <returns>The new insights generated</returns>
            public override IEnumerable<Insight> Update(QCAlgorithmFramework algorithm, Slice data)
            {
                return Enumerable.Empty<Insight>();
            }

            /// <summary>
            /// Event fired each time the we add/remove securities from the data feed
            /// Use this method to initialize any class or indicator that is related to a particular security
            /// </summary>
            /// <param name="algorithm">The algorithm instance that experienced the change in securities</param>
            /// <param name="changes">The security additions and removals from the algorithm</param>
            public override void OnSecuritiesChanged(QCAlgorithmFramework algorithm, SecurityChanges changes)
            {
                // This helper method will update the _securities list 
                // according to the latest changes
                // Particularly helpful to control the which securities are tradable
                NotifiedSecurityChanges.UpdateCollection(_securities, changes);
            }
        }
    }
}