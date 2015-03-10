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
  local confirmreport = require "confirmreport"
  --self defined common module
  local common = require "common"
  local M = require "customOrderData"
  local debug_mode = false
  local easygetter = require "easygetter"

  common.CheckValidTimeRange(entryFrom, entryTo)
  local orderMatchStatusToMLMapping = {[true]="M", [false]="P"}

  -- TODO check timeFrom timeTo here
  -- TODO check within 1 day between entryTime and toTime, otherwise geenration failed

  local sql_column_text = ' TEXT DEFAULT "" '
  local sql_column_integer = ' INTEGER DEFAULT 0'
  local sql_column_real = ' REAL DEFAULT 0.0'
  local table_name_deposit = "deposit"
  local table_name_order = "DECIDE_order"
  local database_tables = {
    {table_name_deposit, {	'client_name'..sql_column_text,
        'account_no'..sql_column_text,
        'account_name'..sql_column_text,
        'buy_limit'..sql_column_real,
        'cash_balance'..sql_column_real,
        'account_type'..sql_column_text,
        'account_status'..sql_column_text,
        'ao_code'..sql_column_text,
        'margin_method'..sql_column_text,
        'legs'..sql_column_text,
        'close_only'..sql_column_text,
        'margin_calculation_method'..sql_column_text,
        'non_cash_collateral'..sql_column_real,
        'call_margin_ammount'..sql_column_real,
        'FC_collateral'..sql_column_real,
        'port_current_initial_margin'..sql_column_real,
        'port_current_maintenance_margin'..sql_column_real,
        'credit_line'..sql_column_real,
        'paid_recv'..sql_column_real,
        'port_current_enforcing_margin'..sql_column_real,
        'projected_initial_margin'..sql_column_real,
        'projected_maintenance_margin'..sql_column_real,
        'outstanding_order_margin'..sql_column_real,
        'cash_collateral'..sql_column_real,
        'total_collateral'..sql_column_real,
        'credit_limit'..sql_column_real,
        "comm_vat"..sql_column_real,
        'total_real'..sql_column_real,
        'total_unreal'..sql_column_real,
        'options_mm'..sql_column_real,
        'previous_value'..sql_column_real,
        'liquidation_value'..sql_column_real,
        'withdrawal_port'..sql_column_real,
        'est_withdrawable'..sql_column_real}},
    {table_name_order, {'series'..sql_column_text,
        'last_price'..sql_column_real,
        'long_vol'..sql_column_real,
        'long_avai'..sql_column_real,
        'long_ost_order'..sql_column_real,
        'long_act_cost'..sql_column_real,
        'long_avg_cost'..sql_column_real,
        'long_avg_unreal'..sql_column_real,
        'long_unreal'..sql_column_real,
        'short_vol'..sql_column_real,
        'short_avail'..sql_column_real,
        'short_ost_order'..sql_column_real,
        'short_act_cost'..sql_column_real,
        'short_avg_cost'..sql_column_real,
        'short_avg_unreal'..sql_column_real,
        'short_unreal'..sql_column_real,
        'side'..sql_column_text
      }}
  }

  common.CreateTables(db_file, log_file,  database_tables, debug_mode)

  local depositItem = {}

  local DECIDE_deposit_obj = fo.Deposit( tonumber(depositId) )
  local client_obj = DECIDE_deposit_obj:getClient()
  table.insert (depositItem, {'client_name', client_obj:getName()})
  table.insert (depositItem, {'account_no', DECIDE_deposit_obj:getNumber()})
  table.insert (depositItem, {'account_name', DECIDE_deposit_obj:getNumber()})
  if (DECIDE_deposit_obj:hasAccountType()) then
    table.insert (depositItem, {'account_type', DECIDE_deposit_obj:getAccountType():getName() })
  end
  table.insert (depositItem, {'account_status', DECIDE_deposit_obj:isActive()})
  table.insert (depositItem, {'close_only', DECIDE_deposit_obj:getAccountStatus()})
  table.insert (depositItem, {'margin_calculation_method', 'Risk Array'})

  if( client_obj:hasAccountManager() ) then
    local account_manager = client_obj:getAccountManager()
    local trader_id = dubi.getSETTraderId( account_manager:getShortName())
    if( trader_id ~= nil) then
      -- ** pending to dev for multiple exchange
      table.insert (depositItem, {'ao_code', trader_id })
    end
  end

  if(DECIDE_deposit_obj:hasShortLongNetting())then
    table.insert (depositItem, {'legs','Y' })
  end
  table.insert (depositItem, {'legs','N' })

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

  if ( orderVolLimit ~= nil ) then
    table.insert (depositItem, {'buy_limit', easygetter.EvenAmountToDouble(orderVolLimit:getLimitAmount())})
    table.insert (depositItem, {'non_cash_collateral', easygetter.EvenAmountToDouble(orderVolLimit:getNonCashCollateral())})
    table.insert (depositItem, {'FC_collateral', easygetter.EvenAmountToDouble(orderVolLimit:getForeignCollateral())})
    table.insert (depositItem, {'credit_line', easygetter.EvenAmountToDouble(orderVolLimit:getCreditLine ())})
    local cash_collateral =  easygetter.EvenAmountToDouble(orderVolLimit:getLimitAmount())
    table.insert (depositItem, {'cash_collateral', easygetter.EvenAmountToDouble(orderVolLimit:getLimitAmount())})
    table.insert (depositItem, {'previous_value', easygetter.EvenAmountToDouble(orderVolLimit:getPreviousOptionM2M())})
    local withdrawal_port = easygetter.EvenAmountToDouble(orderVolLimit:getDepositWithdrawal())
    table.insert (depositItem, {'withdrawal_port', easygetter.EvenAmountToDouble(orderVolLimit:getDepositWithdrawal())})
  end

  if(orderVolumeRisk ~= nil) then
    table.insert (depositItem, {'call_margin_ammount', easygetter.EvenAmountToDouble(orderVolumeRisk:getCallAmount()) })
    local cash_balance = easygetter.EvenAmountToDouble(orderVolumeRisk:getCashBalance())
    table.insert (depositItem, {'cash_balance',easygetter.EvenAmountToDouble(orderVolumeRisk:getCashBalance()) })
    table.insert (depositItem, {'port_current_initial_margin', easygetter.EvenAmountToDouble(orderVolumeRisk:getPositionMargin()) })
    table.insert (depositItem, {'port_current_maintenance_margin', easygetter.EvenAmountToDouble(orderVolumeRisk:getMaintenanceMargin())})
    table.insert (depositItem, {'port_current_enforcing_margin', easygetter.EvenAmountToDouble(orderVolumeRisk:getForceMargin())})
    table.insert (depositItem, {'projected_initial_margin',  easygetter.EvenAmountToDouble(orderVolumeRisk:getExpectedMargin())})
    table.insert (depositItem, {'projected_maintenance_margin', easygetter.EvenAmountToDouble(orderVolumeRisk:getMaintenanceMargin())})
    table.insert (depositItem, {'outstanding_order_margin', easygetter.EvenAmountToDouble(orderVolumeRisk:getOrderMargin())})
    table.insert (depositItem, {'credit_limit',easygetter.EvenAmountToDouble(orderVolumeRisk:getCreditLimit()) })
    local total_real = easygetter.EvenAmountToDouble(orderVolumeRisk:getDailyFutureRPLGross())
    table.insert (depositItem, {'total_real',easygetter.EvenAmountToDouble(orderVolumeRisk:getDailyFutureRPLGross()) })
    local total_unreal =  easygetter.EvenAmountToDouble(orderVolumeRisk:getTotalFutureUPLGross())
    table.insert (depositItem, {'total_unreal', easygetter.EvenAmountToDouble(orderVolumeRisk:getTotalFutureUPLGross())})
    table.insert (depositItem, {'options_mm', easygetter.EvenAmountToDouble(orderVolumeRisk:getTotalOptionUPLGross())})
    table.insert (depositItem, {'liquidation_value', easygetter.EvenAmountToDouble(orderVolumeRisk:getLiquidationValue())})
    local paid_recv = easygetter.EvenAmountToDouble(orderVolumeRisk:getTurnoverVolumeSell()) - easygetter.EvenAmountToDouble(orderVolumeRisk:getTurnoverVolumeBuy())
    table.insert (depositItem, {'paid_recv', easygetter.EvenAmountToDouble(orderVolumeRisk:getTurnoverVolumeSell()) - easygetter.EvenAmountToDouble(orderVolumeRisk:getTurnoverVolumeBuy())})
    table.insert (depositItem, {'est_withdrawable',easygetter.EvenAmountToDouble(orderVolumeRisk:getEstimatedWithdrawable()) })
  end

  if ( orderVolLimit ~= nil and orderVolumeRisk ~= nil ) then
    table.insert (depositItem, {'total_collateral', 
        easygetter.EvenAmountToDouble(orderVolumeRisk:getCashBalance()) + 
        easygetter.EvenAmountToDouble(orderVolLimit:getNonCashCollateral()) + 
        easygetter.EvenAmountToDouble(orderVolLimit:getForeignCollateral()) })
  end

  if(vmargCalcSettings ~= nil ) then
    table.insert (depositItem, {'margin_method', margCalcSettings:getName()})
  end

  local orders = easygetter.GetOrders( depositId, entryFrom, entryTo )

  for _,no in pairs( orders ) do
    local order = fo.Order( no )

    local orderHandlingType = order:getHandlingType()
    if (orderHandlingType == 'TradingOrder' or orderHandlingType == 'BlockTrade') then
      local orderlegs = order:getOrderLegs()
      for _,orderLeg in pairs(orderlegs) do
        local orderItem = {}
        local comm_vat = 0
        local net = 0
        for _,op in pairs( order:getOrderOperations() ) do
          if op:getTransactionType() == "Match" then
            local oplegs = op:getOrderOperationLegs()
            if (#oplegs ~= 1) then
              confirmreport.announceGenerationFailure("One order operation should and only should have one order opeation legs!")
              print("One order operation should and only should have one order opeation legs!")
              os.exit(1)
            elseif(oplegs[1]:getContract():getContractCode() == orderLeg:getContract():getContractCode()) then
              local ta = oplegs[1]:getEffectiveTransaction()
              local posting = fo.Posting(ta,1)     	 
              if (posting:hasFeeValue(nil,maps.NameToFee("Custodian")) ) then
                local comm = easygetter.EvenAmountToDouble(posting:getFeeAccountCurr(nil,maps.NameToFee("Custodian")))
                local vat = easygetter.EvenAmountToDouble(posting:getFeeAccountCurr(nil,maps.NameToFee("Settlement")))
                comm_vat = comm_vat + comm + vat
              end 
              net = net + easygetter.EvenAmountToDouble(posting:getTurnoverAccountCurrNet())
            end
          end
        end
        table.insert (depositItem, {'comm_vat', comm_vat})
      end
    end
  end

  local depositList = {}
  table.insert (depositList, depositItem)

  common.InsertRecords(db_file, log_file, table_name_deposit, depositList, debug_mode)

  --print("cash_balance1:"..easygetter.EvenAmountToDouble(orderVolumeRisk:getCashBalance()))
  --print("cash_balance2:"..(cash_collateral+withdrawal_port+paid_recv+total_unreal+total_real+comm_vat))

  local orderList = {}

  local position_list = DECIDE_deposit_obj:getAccPositions()   -- get a list of positions
  for _,position in pairs ( position_list ) do
    local data = accpos.getPositionValues( position, tim.TimeStamp.current() , tim.TimeStamp.current(), DECIDE_deposit_obj:getGeneralLedgerCurrencyType() )
    --local data =DECIDE_deposit_obj:getGeneralLedgerCurrencyType()
    --if ( data.effective == "Yes" ) then
      local orderItem = {}
      local contract = position:getContract()
      -- Works
      series_id = contract:getContractCode()
      table.insert (orderItem, {'series',series_id})
      ----table.insert (orderItem, {'last_price', data.LastUnchecked})
      ------------------------------------------------------------------------Pong.
      local ce = fo.ContractEvaluation{ contract=contract }
      local lastprice =ce:getLastUnchecked()
      table.insert(orderItem,{'last_price',lastprice})
        ----------------------------------------------------------------------Pong.
      -- Works
      local on_hand = easygetter.EvenAmountToDouble(data.endQuantity)
      local average_price = easygetter.EvenAmountToDouble(data.endAveragePrice)
      --   table.insert (orderItem2, {'long_vol', on_hand})

      -- Works
      local position_short_long = position:getShortLong()
      table.insert (orderItem, {'side', position_short_long})

      --if ( position_short_long == "Long" ) then
        table.insert (orderItem, {'long_vol',position:getQuantity()})    ---PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        --		table.insert (orderItem, {'long_avg_cost', }) PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        --		table.insert (orderItem, {'long_avg_unreal', }) PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        --		table.insert (orderItem, {'long_unreal', }) PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        table.insert (orderItem, {'long_act_cost',average_price * on_hand * easygetter.EvenAmountToDouble(contract:getTickFactor(nil,tim.Date.current()))})
        local sellable = on_hand - easygetter.EvenAmountToDouble(data.unmatchedBuyOrders)
        table.insert (orderItem, {'long_avai', sellable})
        table.insert (orderItem, {'long_ost_order', easygetter.EvenAmountToDouble(data.unmatchedBuyOrders)})
      --elseif ( position_short_long == "Short" ) then
        table.insert (orderItem, {'short_vol',position:getQuantity()})------ PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        --		table.insert (orderItem, {'short_avg_cost', }) PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        --		table.insert (orderItem, {'short_avg_unreal', }) PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        --		table.insert (orderItem, {'short_unreal', }) PreConfirmation_Position_data.xls description on the field is not clear wait for sure
        table.insert (orderItem, {'short_act_cost',average_price * on_hand * easygetter.EvenAmountToDouble(contract:getTickFactor(nil,tim.Date.current()))})
        local sellable = on_hand - easygetter.EvenAmountToDouble(data.unmatchedSellOrders)
        table.insert (orderItem, {'short_avail', sellable})
        table.insert (orderItem, {'short_ost_order', easygetter.EvenAmountToDouble(data.unmatchedSellOrders)})
      --end
      table.insert (orderList, orderItem)
    --end
  end

  common.InsertRecords(db_file, log_file, table_name_order, orderList, debug_mode)
end
local inst = os.getenv( "OCSINST" )
local mand = mandator.Mandator( inst )
mandator.changeTo( mand, process )