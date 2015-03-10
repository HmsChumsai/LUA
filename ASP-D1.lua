-- Last Modified Date: 20140608-00
require "ocs"

local cmdln = require "cmdline"
local fo = require "fo"
local tim = require "tim"
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
print("log_file: " .. log_file)
print("db_file: " .. db_file)
print("format: " .. format)

ocs.createInstance( "LUA" )
local function process()
  local dubi = require "dubi"
  --self defined common module
  local common = require "common"
  local confirmreport = require "confirmreport"
  local easygetter = require "easygetter"
  local M = require "customOrderData"

  common.CheckValidTimeRange(entryFrom, entryTo)

  local debug_mode = false
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
  'account_type'..sql_column_text,
  'account_status'..sql_column_text,
  'ao_code'..sql_column_text}},
  {table_name_order, {'border'..sql_column_text,
  'bdate'..sql_column_text,
  'cdate'..sql_column_text,
  'order_no'..sql_column_text,
  'entry_time'..sql_column_text,
  'series'..sql_column_text,
  'long_short'..sql_column_text,
  'pos'..sql_column_text,
  'vol'..sql_column_integer,
  'price'..sql_column_real,
  'status'..sql_column_text,
  'match_vol'..sql_column_integer,
  'match_price'..sql_column_real,
  'stop_series'..sql_column_text,
  'stop_condition'..sql_column_text,
  'until'..sql_column_text,
  'validity'..sql_column_text,
  'gtn'..sql_column_text,
  'cancel_time'..sql_column_text}}
}

common.CreateTables(db_file, log_file,  database_tables, debug_mode)

local DECIDE_deposit_obj = fo.Deposit( tonumber(depositId) )

local depositItem = {}

table.insert (depositItem, {'account_no', DECIDE_deposit_obj:getNumber()})
table.insert (depositItem, {'account_name', DECIDE_deposit_obj:getName()})
if (DECIDE_deposit_obj:hasAccountType()) then
  table.insert (depositItem, {'account_type', DECIDE_deposit_obj:getAccountType():getName() })
end
table.insert (depositItem, {'account_status', DECIDE_deposit_obj:isActive()})

local client_obj = DECIDE_deposit_obj:getClient()

if( client_obj:hasAccountManager() ) then
  local account_manager = client_obj:getAccountManager()
  local trader_id = dubi.getSETTraderId( account_manager:getShortName())
  if( trader_id ~= nil) then
    table.insert (depositItem, {'ao_code', trader_id })
  end
  table.insert (depositItem, {'client_name', account_manager:getPerson():getName()})
end

local depositList = {}
table.insert (depositList, depositItem)

common.InsertRecords(db_file, log_file, table_name_deposit, depositList, debug_mode)

local orders = easygetter.GetOrders( depositId, entryFrom, entryTo )

local orderList = {}
print("------------------- Before Loop Orders ------------------")

for _,no in pairs( orders ) do
  local order = fo.Order( no )
  print("------------------- start ------------------")
  print("Order No : " .. no)
  for _,op in pairs( order:getOrderOperations() ) do
    print("OrderOp : " .. no)
    --if op:getTransactionType() == "Match" then
      local orderHandlingType = order:getHandlingType()
      if (orderHandlingType == 'TradingOrder' or orderHandlingType == 'BlockTrade') then
        local orderItem = {}
        --local orderLegs = order:getOrderLegs()
        local OrderOperationLeg = op:getOrderOperationLegs()
        --local orderlegNum = #order:getOrderLegs()
        local orderlegNum = #op:getOrderOperationLegs()
        print("orderlegNum : " .. orderlegNum)
        --for _,orderLeg in pairs(orderLegs) do 
        for _,orderLeg in pairs(OrderOperationLeg) do
          --if ( orderlegNum >= 1 ) then
            table.insert (orderItem, {'border', order:getOrderId()})
            table.insert (orderItem, {'bdate', order:getEntryTime():getDateInUtcOffset(common.offsetTimeZoneSecs):toString("%Y%m%d")})
            local cdate = common.GetSettlementDateFromOrder(order);
            if( cdate ~= nil) then
              table.insert (orderItem, {'cdate', cdate:toString("%Y%m%d")})
            end
            table.insert (orderItem, {'order_no', order:getExchangeOrderid()})
            table.insert (orderItem, {'entry_time', order:getEntryTime():getClockTimeInUtcOffset(common.offsetTimeZoneSecs):toString("%T")})

            local orderleg = orderLeg
            if(orderlegNum == 1) then
              table.insert (orderItem, {'series', orderleg:getContract():getContractCode()})
            else
              table.insert (orderItem, {'series', order:getInstrument():getShortName()})
            end

            local LongShort = common.BuySellToLSMapping[order:getBuySell()] or ''

            table.insert (orderItem, {'long_short', LongShort})
            table.insert (orderItem, {'pos', orderleg:getOpenClose()})
              --local totalQty = orderleg:getTotalQty()
            local totalQty = orderleg:getOpenQty()
            local execQty = orderleg:getExecQty()
            table.insert (orderItem, {'vol', totalQty})
            table.insert (orderItem, {'price', orderleg:getPriceLimit()})

            local status = orderMatchStatusToMLMapping[totalQty == execQty]
            table.insert (orderItem, {'status', M.getStatus(order:getOrderId())})

            table.insert (orderItem, {'match_vol', execQty})

            if (orderleg:getExecQty() ~= 0 ) then
              --table.insert (orderItem, {'match_price', orderleg:getAvgExecPrice()})
              table.insert (orderItem, {'match_price', orderleg:getPrice()})
            end
          --end

            if (order:hasStopContract()) then
              table.insert (orderItem, {'stop_series',order:getStopContract():getContractCode()})
            end

            table.insert (orderItem, {'stop_condition', order:getOrderLimitType()})

            local validTime = order:getValidTime()
            if ( not validTime:isUnused() ) then
              table.insert (orderItem, {'until', validTime:toString("%Y%m%d")})

              if (validTime:getDate():compare( tim.Date.current( tim.TimeZone.current() ) ) ==0 ) then
                table.insert (orderItem, {'validity', "Day"})
              else
                table.insert (orderItem, {'validity', "N/A"})
              end

              if (validTime:getClockTime("MIDNIGHT_24"):compare( tim.ClockTime.max() ) < 0 ) then
                table.insert (orderItem, {'gtn', 'N'})
              else
                table.insert (orderItem, {'gtn', 'Y'})
              end
            else
              table.insert (orderItem, {'until', 'GTC'})
              table.insert (orderItem, {'validity', 'GTC'})
              table.insert (orderItem, {'gtn', 'GTC'})
            end


            local cancelOp = easygetter.GetCancelOrderOperation( order )
            if ( cancelOp ~= nil ) then
              local cancel_date_local =  cancelOp:getEntryTime():getDateInUtcOffset(common.offsetTimeZoneSecs)
              local cancel_time_local =  cancelOp:getEntryTime():getClockTimeInUtcOffset(common.offsetTimeZoneSecs)
              table.insert (orderItem, {'cancel_time', cancel_date_local:toString("%Y%m%d-") .. cancel_time_local:toString("%T")})
            end

            table.insert (orderList, orderItem)

          end
        end
      end
    --end
  end
  common.InsertRecords(db_file, log_file, table_name_order, orderList, debug_mode)
end

local inst = os.getenv( "OCSINST" )
local mand = mandator.Mandator( inst )
mandator.changeTo( mand, process )
