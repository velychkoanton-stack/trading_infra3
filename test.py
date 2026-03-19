# this file is used for some running tests in connection.
# it not part of trading infra.

from Execution_layer.Executors.models import ExecutorBotConfig
from Execution_layer.Executors.symbol_mapper import ccxt_symbol_to_asset, ccxt_symbol_to_pybit_symbol
from Execution_layer.Executors.shared_state import SharedExecutorState
from Execution_layer.Support_layer.support_bridge import SupportBridge

print(ccxt_symbol_to_asset("BTC/USDT:USDT"))
print(ccxt_symbol_to_pybit_symbol("ETH/USDT:USDT"))

state = SharedExecutorState()
print(state.get_all_open_pairs())

from Common.db.db_transaction import run_in_transaction

print("transaction helper imported OK")

from Execution_layer.Executors.repositories import ExecutorRepositories

print("repositories imported OK")

from Execution_layer.Executors.order_manager import OrderManager

print("order_manager imported OK")

from Execution_layer.Executors.control_worker import ControlWorker

print("control_worker imported OK")

from Execution_layer.Executors.executor_base import ExecutorBase

print("executor_base imported OK")

from Execution_layer.Executors.executor_worker import ExecutorWorker

print("executor_worker imported OK")

from Execution_layer.Executors.executor_runtime import ExecutorRuntime

print("executor_runtime imported OK")