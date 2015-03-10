-- Last Modified Date: 20140608-00
require "ocs"

local cmdln = require "cmdline"
local fo = require "fo"
local tim = require "tim"
local maps = require "maps"
local accpos = require "accpos"
local mandator = require "mandator"

local depositId = ""
local entryFrom = ""
local entryTo = ""
local log_file = ""
local db_file = ""
local format = "PDF"

cmdln.add{ name="--logFile", descr="", func=function(x) log_file=x end }
cmdln.add{ name="--dbFile", descr="", func=function(x) db_file=x end }
cmdln.add{ name="--depositid", descr="", func=function(x) depositId=x end }
--cmdln.add{ name="--format", descr="", func=function(x) format=x end }
cmdln.add{ name="--from", descr="", func=function(x) entryFrom=x end }
cmdln.add{ name="--to", descr="",  func=function(x) entryTo=x end }

cmdln.parse( arg, true )
print("depositId: " .. depositId)
print("entryFrom: " .. entryFrom)
print("entryTo: " .. entryTo)
print("log_file: " .. log_file)
print("db_file: " .. db_file)
print("format: " .. format)

ocs.createInstance( "LUA" )
local function process()
  local dubi = require "dubi" 

  --self defined common module
  local common = require "common"
  local easygetter = require "easygetter"
  local confirmreport = require "confirmreport"
  local debug_mode = false
  local orderMatchStatusToMLMapping = {[true]="M", [false]="P"}

  common.CheckValidTimeRange(entryFrom, entryTo)

  -- TODO check timeFrom timeTo here
  -- TODO check within 1 day between entryTime and toTime, otherwise geenration failed 

  local sql_column_text = ' TEXT DEFAULT "" '
  local sql_column_integer = ' INTEGER DEFAULT 0'
  local sql_column_real = ' REAL DEFAULT 0.0'
  local table_name_deposit = "deposit"
  local table_name_order = "DECIDE_order"
  local table_name_order2 = "DECIDE_order2"
  local database_tables = {
    {table_name_deposit, {'account_no'..sql_column_text, 
                'account_name'..sql_column_text, 
                'account_type'..sql_column_text, 
                'margin_method'..sql_column_text,
                --"total_comm_vat"..sql_column_real, 
                }},
    {table_name_order, {'border'..sql_column_text, 
              'order_no'..sql_column_text, 
              'trade_no'..sql_column_text, 
              'buy_sell'..sql_column_text, 
              'series'..sql_column_text,
              'position'..sql_column_text,
              'source_type'..sql_column_text,
              'vol'..sql_column_integer, 
          --not works
              'deal_price'..sql_column_real,  
              'trade_type'..sql_column_text,
              'avg_cost'..sql_column_real,
              'avg_amount'..sql_column_real,
              'avg_realized_pl'..sql_column_real,
              'act_cost'..sql_column_real,
              'act_mount'..sql_column_real,
              'act_realized_pl'..sql_column_real,
              'premium'..sql_column_real, 
              'com_vat_offline'..sql_column_real, 
              'com_vat_online'..sql_column_real, 
              'grand_total'..sql_column_real,
              
              'bdate'..sql_column_text, 
              'cdate'..sql_column_text,
              -- for investigation only
              'orderStat'..sql_column_text,
              'opOederStat'..sql_column_text,
              }},
     --{table_name_order2, { 'act_cost'..sql_column_real,}} 
  }           

  common.CreateTables(db_file, log_file,  database_tables, debug_mode)

  local DECIDE_deposit_obj = fo.Deposit( tonumber(depositId) )

  local depositItem = {}
  table.insert (depositItem, {'account_no', DECIDE_deposit_obj:getNumber()}) 
  table.insert (depositItem, {'account_name', DECIDE_deposit_obj:getName()})
  if (DECIDE_deposit_obj:hasAccountType()) then
    table.insert (depositItem, {'account_type', DECIDE_deposit_obj:getAccountType():getName() })
  end
  
  ----------------------------------
  --local position = DECIDE_deposit_obj:getPortfolio():getPositions() --- TEST
    --   for _,po in pairs(position) do 
      --    local pe = fo.PositionEvaluation { position=po }
        --  local Avgeprice = pe:getAvgePrice()
          --local Avgeprice = po:getOrders()
            --   table.insert(depositItem,{'act_cost',Avgeprice})
        --end
  --------------------------------------
  
  
  local orderVolLimit = easygetter.GetFirstActiveOVL(depositId)
  
  local marginTask = nil
  local margCalcSettings = nil
  local orderVolumeRisk = nil

  if (orderVolLimit ~= nil) then
    orderVolumeRisk = orderVolLimit:getUniqueRisk()
    if ( orderVolLimit:hasMarginTask() ) then
      marginTask = orderVolLimit:getMarginTask()
      margCalcSettings = marginTask:getMargCalcSettings()
    end
  end

  --local total_comm_vat = 0

  if(margCalcSettings ~= nil) then
    table.insert (depositItem, {'margin_method', margCalcSettings:getName()})
  end

  local orders = easygetter.GetOrders( depositId, entryFrom, entryTo )

  local depositList = {}

  local orderList = {}

  for _,no in pairs( orders ) do
    local order = fo.Order( no ) 
    local orderHandlingType = order:getHandlingType()
    if (orderHandlingType == 'TradingOrder' or orderHandlingType == 'BlockTrade') then  

      -- use any of the order leg(if for combination instrument order)

      local orderlegs = order:getOrderLegs()
      
      for _,orderleg in pairs(orderlegs) do
        local orderItem = {}
        local cdate = common.GetSettlementDateFromOrder(order);
         
        if( cdate ~= nil) then
          table.insert (orderItem, {'cdate', cdate:toString("%d".."/".."%m".."/".."%Y")}) ------%d%m%Y
        end
        table.insert (orderItem, {'opOederStat', order:getStatus()})
        table.insert (orderItem, {'series', orderleg:getContract():getContractCode()})
        --the buy_sell is the same as CNS-D1
        local buy_sell = orderleg:getOrderKind()
        table.insert (orderItem, {'buy_sell',buy_sell})
        local position_status = orderleg:getOpenClose()
        table.insert (orderItem, {'position',tostring(position_status)})
        table.insert (orderItem, {'border', order:getOrderId()})  
        table.insert (orderItem, {'bdate', order:getEntryTime():getDateInUtcOffset(common.offsetTimeZoneSecs):toString("%d".."/".."%m".."/".."%Y")})
        table.insert (orderItem, {'order_no', order:getExchangeOrderid()})
        --confirm with wern which vol will use
        local vol = orderleg:getTotalQty()
        table.insert (orderItem, {'vol',vol})
        ----table.insert (orderItem, {'deal_price', orderleg:getAvgExecPrice()})
        
        local execQty = orderleg:getExecQty()
        if (orderleg:getExecQty() ~= 0 ) then
          local dealprice = orderleg:getAvgExecPrice()
          table.insert (orderItem, {'deal_price',dealprice})
        end
        
        -- local contract = fo.getOrderContract( order )
        -- local ce = fo.ContractEvaluation{ contract=contract }
        -- local settle = ce:getSettle()------------<< Settle Price :name =  AvgCost
        -- local multi = ce:getTradingunit()
        
        -- --local actcost = ce:getContract():getFutures()
        -- ---local contract_size = order:getBufferedOperationQty()
        -- if (position_status == "Close")then
        -- table.insert (orderItem,{'avg_cost',settle})
        -- table.insert (orderItem,{'avg_amount',vol*settle*multi})--------settle*vol*multi
        -- elseif (position_status == "Open")then
        -- --table.insert (orderItem,{'avg_cost',"-"})
        -- --table.insert (orderItem,{'avg_amount',"-"})
        -- end
        
        
         local position = order:getDeposit():getPortfolio():getPositions()
         for _,po in pairs(position) do 
          local pe = fo.PositionEvaluation { position=po }----{ position=po }
          if (orderleg:getOpenClose() == "Close")then
                 local Avgeprice = pe:getAvgePrice()
                 --table.insert (orderItem,{'act_amount',vol*settle*multi})
                 table.insert(orderItem,{'act_cost',Avgeprice})
                 end
                 
                 end
          -- break
          -- --local Avgeprice = po:getOrders()
                
        -- end
        
        
        
         --------------avg_realized_pl =Deal_price*vol * Multiplier-----------
         
         -- if ( buy_sell == "Sell" and position_status =="Close") then
              -- local execQty = orderleg:getExecQty()
                -- if (orderleg:getExecQty() ~= 0 ) then
              -- local dealprice = orderleg:getAvgExecPrice()
              -- local case1 = (dealprice*vol*multi)-settle
              -- table.insert(orderItem,{'avg_realized_pl',case1}) --(Deal price*Deal Quantity*contract_size)-SettlePrice
                -- end
         -- elseif ( buy_sell == "Buy" and position_status =="Close") then
              -- local execQty = orderleg:getExecQty()
                -- if (orderleg:getExecQty() ~= 0 ) then
              -- local dealprice = orderleg:getAvgExecPrice()
              -- local case2 = settle-(dealprice*vol*multi)
         -- table.insert(orderItem,{'avg_realized_pl',case2})
         -- end
         -- end
         ----------------------------------------------------------------------
         local Premium = order:getInstrument():getIsin()
              --table.insert (orderItem,{'Premium',Premium})
              stringmatch  = string.match(Premium,'OPT')
                if (stringmatch == "FUT") then
              table.insert(orderItem,{'premium',"0.0"})
              elseif (stringmatch == "OPT") then
              
              table.insert(orderItem,{'premium',"---"})
               end
         
  --      error when generate
  --      table.insert (orderItem, {'trade_type', Meaning unclear  })
  --      table.insert (orderItem, {'avg_realized_pl', Not available as P/L values in DECIDE are calculated for position and not for trade.})
  --      table.insert (orderItem, {'act_cost', })
  --      table.insert (orderItem, {'act_realized_pl', Not available as P/L values in DECIDE are calculated for position and not for trade.})
  --      table.insert (orderItem, {'premium', })
  --      table.insert (orderItem, {'com_vat_offline', Separation of trades in Offline and Online trades is unclear})
  --      table.insert (orderItem, {'com_vat_online', Separation of trades in Offline and Online trades is unclear})
  --      table.insert (orderItem, {'grand_total', })
      
        --local comm_vat = 0
        for _,op in pairs( order:getOrderOperations() ) do
          
          if op:getTransactionType() == "Match" then
            --print("Match! : " .. order:getOrderId())
            table.insert (orderItem, {'orderStat', op:getTransactionType()})
            print(order:getOrderId() .. "status : " .. op:getTransactionType())
            local oplegs = op:getOrderOperationLegs()
            if (#oplegs ~= 1) then
              confirmreport.announceGenerationFailure("One order operation should and only should have one order opeation legs!")
              print("One order operation should and only should have one order opeation legs!")
              os.exit(1)
            elseif(oplegs[1]:getContract():getContractCode() == orderleg:getContract():getContractCode()) then
              local ta = oplegs[1]:getEffectiveTransaction()
              table.insert (orderItem, {'trade_no', ta:getTaNrSystem()})
              table.insert (orderItem, {'bdate', ta:getValueDate():toString("%d".."/".."%m".."/".."%Y")})
              if( ta:hasTradingChannel()) then
                table.insert (orderItem, {'source_type', ta:getTradingChannel():getName()})
              end
              if(ta : hasSourceInterface()) then
                table.insert(orderItem,{'trade_type',ta:getSourceInterface():getShortName()})
                end
            end
              --local posting = fo.Posting(ta,1)       
                --if (posting:hasFeeValue(nil,maps.NameToFee("Custodian")) ) then
                --  comm_vat = comm_vat + posting:getFeeAccountCurr(nil,maps.NameToFee("Custodian")):asDouble()
                --end 
                --local Premium = order:getInstrument():getIsin()
              ---table.insert (orderItem,{'Premium',Premium})
              --stringmatch  = string.match(Premium,'FUT')
                --if (stringmatch == "FUT") then
              --table.insert(orderItem,{'premium',"--"})
              --elseif (Premium == "OPT") then
                  --if ( oplegs:hasTransaction()) then
                  --local po = oplegs:getTransaction():getPosting()
                      --for _,post in pair(po) do
                      --local pt = fo.Posting{ po=post }
                      --table.insert(orderItem,{'premium',pt:getTurnoverCurr()})
                      --table.insert(orderItem,{'premium',"FAIL"})
                  --end
              --end
            end
        --table.insert (orderItem, {'comm_vat', comm_vat})
        --total_comm_vat = total_comm_vat + comm_vat
        
         
          end
          table.insert (orderList, orderItem)
        end
        
      end
    end
  --end
  --local orderList2 = {}
  --
  --local position_list = DECIDE_deposit_obj:getAccPositions()   -- get a list of positions
  --for _,position in pairs ( position_list ) do
  --  local data = accpos.getPositionValues( position, tim.TimeStamp.current() , tim.TimeStamp.current(), DECIDE_deposit_obj:getGeneralLedgerCurrencyType() )
  --  if ( data.effective == "Yes" ) then
  --    local orderItem2 = {}
  --    local contract = position:getContract()
  --    
  --    
  --      local  cost = easygetter.EvenAmountToDouble(data.endPosBookValue)
  --      table.insert (orderItem2, {'act_cost', cost})
  --    
  --    table.insert (orderList2, orderItem2)
  --  end
  --end
  --
  --common.InsertRecords(db_file, log_file, table_name_order2, orderList2, debug_mode)

  --table.insert (depositItem, {'total_comm_vat', total_comm_vat})
  table.insert (depositList, depositItem)

  common.InsertRecords(db_file, log_file, table_name_deposit, depositList, debug_mode)
  common.InsertRecords(db_file, log_file, table_name_order, orderList, debug_mode)
end
local inst = os.getenv( "OCSINST" )
local mand = mandator.Mandator( inst )
mandator.changeTo( mand, process )