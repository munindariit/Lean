# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from clr import AddReference
AddReference("System")
AddReference("QuantConnect.Algorithm")
AddReference("QuantConnect.Algorithm.Framework")
AddReference("QuantConnect.Common")

from System import *
from QuantConnect import *
from QuantConnect.Orders import *
from QuantConnect.Algorithm import *
from QuantConnect.Algorithm.Framework import *
from QuantConnect.Algorithm.Framework.Alphas import *
from QuantConnect.Algorithm.Framework.Execution import *
from QuantConnect.Algorithm.Framework.Portfolio import *
from QuantConnect.Algorithm.Framework.Risk import *
from QuantConnect.Algorithm.Framework.Selection import *

class AlphaStreamTemplateAlgorithm(QCAlgorithmFramework):
    '''Alpha Stream Template Algorithm uses framework components to define the algorithm.
    In this template, three popular options for Universe Selection Model are presented and 
    the default Portfolio Construction, Execution and Risk Management models developed by
    the QuantConnect Team are selected.
    Finally, the skeleton of an Alpha Model is included for a quick start.'''

    def Initialize(self):
        ''' Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.'''

        self.SetStartDate(2017,1,7)   # Set Start Date
        self.SetCash(1000000)         # Set Strategy Cash

        # Set zero transaction fees
        self.SetSecurityInitializer(lambda security: security.SetFeeModel(ConstantFeeModel(0)))

        # Set requested data resolution
        self.UniverseSettings.Resolution = Resolution.Minute

        # Set algorithm framework models
        # Popular Option #1:
        # Sets a list of symbols manually
        symbols = [ Symbol.Create(x, SecurityType.Equity, Market.USA) 
                   for x in ["SPY", "MSFT", "AAPL"] ]
        self.SetUniverseSelection(ManualUniverseSelectionModel(symbols))

        # Popular Option #2:
        # Use coarse universe selection
        #self.SetUniverseSelection(CoarseFundamentalUniverseSelectionModel(self.SelectCoarse))

        # Popular Option #3:
        # Use universe helper
        #self.SetUniverseSelection(QC500UniverseSelectionModel())

        # Use your Custom Alpha Model
        self.SetAlpha(self.AlphaModelTemplate())

        # Equally weigh securities in portfolio, based on insights
        self.SetPortfolioConstruction(EqualWeightingPortfolioConstructionModel())

        # Immediate Execution Fill Model
        self.SetExecution(ImmediateExecutionModel())

        # Null Risk-Management Model
        self.SetRiskManagement(NullRiskManagementModel())


    def SelectCoarse(self, coarse):
        ''' Select securities wih fundamental data and price above $5
         Take 50 securities with the highest dollar volume'''
        filtered = [cf for cf in coarse if cf.HasFundamentalData and cf.Price > 5]
        sortedbyDollarVolume = sorted(filtered, key = lambda cf: cf.DollarVolume, reverse = True)
        return [cf.Symbol for cf in sortedbyDollarVolume][:50]

    class AlphaModelTemplate(AlphaModel):
        '''Alpha Model Template. In this template, insights are not emitted.'''

        def __init__(self):
            '''Initialize an new instance of the AlphaModelTemplate'''
            self.securities = list()

        def Update(self, algorithm, data):
            '''AlphaModel.Update is the primary entry point for your algorithm. 
            Each new data point will be pumped in here and can be used to generate insights.
            Insights are the most valuable piece of information for hedge funds and, 
            in the algorithms context, they are used to construct your portfolio.
            Args:
                algorithm: The algorithm instance
                data: Slice object keyed by symbol containing the stock data
            Returns:
                The new insights generated'''
            return []

        def OnSecuritiesChanged(self, algorithm, changes):
            '''Event fired each time the we add/remove securities from the data feed
            Use this method to initialize any class or indicator that is related to a particular security
            Args:
                algorithm: The algorithm instance that experienced the change in securities
                changes: The security additions and removals from the algorithm'''
            
            # Update the self.securities list according to the latest changes
            # Particularly helpful to control the which securities are tradable
            for removed in changes.RemovedSecurities:
                if removed in self.securities:
                    self.securities.remove(removed)

            for added in changes.AddedSecurities:
                if added not in self.securities:
                    self.securities.append(added)