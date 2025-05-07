import os
import sys
import asyncio
import random
import json
from web3 import Web3
from eth_account import Account
from colorama import init, Fore, Style
import aiohttp
from aiohttp_socks import ProxyConnector

# Initialize colorama
init(autoreset=True)

# Border width
BORDER_WIDTH = 80

# Constants
NETWORK_URL = "https://sepolia.optimism.io"
CHAIN_ID = 11155420
EXPLORER_URL = "https://sepolia-optimistic.etherscan.io/tx/0x"
IP_CHECK_URL = "https://api.ipify.org?format=json"
POOL_ADDR = Web3.to_checksum_address("0xe81c469181Ca7A57cB4Df8656E2fc41f8c92405C")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
}
CONFIG = {
    "PAUSE_BETWEEN_ATTEMPTS": [10, 30],
    "MAX_CONCURRENCY": 5,
    "MAX_RETRIES": 3,
    "MINIMUM_BALANCE": 0.001,
    "PENDING_TX_TIMEOUT": 300,
    "TRANSACTION_TIMEOUT": 180,
    "DEFAULT_GAS_LIMIT": 500000,
    "GAS_MULTIPLIER": 3.0,
}

# Token definitions
TOKENS = {
    "WETH": {"address": Web3.to_checksum_address("0x915d965C881fe4a39f410515d9f38B0B2e719a64"), "decimals": 18},
    "CRV": {"address": Web3.to_checksum_address("0x20994ADb975D6196AD8026CAE296d4285c8AC20f"), "decimals": 18},
    "SUSHI": {"address": Web3.to_checksum_address("0xa1D656B741bA80C665216A28Eb7361Bf2578F1D8"), "decimals": 18},
    "UNI": {"address": Web3.to_checksum_address("0x657b37F5B4D007F8CDA5C8b22304da70F1A55241"), "decimals": 18},
    "USDC": {"address": Web3.to_checksum_address("0x0Ad30413bF3E83e1aD6120516CD07D677f015f5c"), "decimals": 6},
    "wBTC": {"address": Web3.to_checksum_address("0xC14b762bD6b4C7f40bB06E5613d0C2A1cB0f7E9c"), "decimals": 8},
}

# Contract ABIs
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "success", "type": "bool"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"}
        ],
        "name": "allowance",
        "outputs": [{"name": "remaining", "type": "uint256"}],
        "type": "function"
    }
]

UNIFIED_ABI = [
    {
        "inputs": [
            {"name": "", "type": "address"},
            {"name": "", "type": "uint256"}
        ],
        "name": "provideLiquidity",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "", "type": "address"},
            {"name": "", "type": "uint256"}
        ],
        "name": "removeLiquiditySingleToken",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

# Bilingual vocabulary
LANG = {
    'vi': {
        'title': 'CUNG CẤP/RÚT THANH KHOẢN - OPTIMISM SEPOLIA',
        'info': 'Thông tin',
        'found': 'Tìm thấy',
        'wallets': 'ví',
        'found_proxies': 'Tìm thấy {count} proxy trong proxies.txt',
        'processing_wallet': 'XỬ LÝ VÍ',
        'checking_balance': 'Kiểm tra số dư...',
        'insufficient_balance': 'Số dư không đủ',
        'insufficient_token_balance': 'Số dư không đủ cho {token}: Có {balance:.6f}, Cần {required:.6f}',
        'preparing_tx': 'Chuẩn bị giao dịch...',
        'sending_tx': 'Đang gửi giao dịch...',
        'success': 'Thành công: {action} {amount:.6f} {token}',
        'failure': 'Thất bại: {action}',
        'address': 'Địa chỉ ví',
        'amount': 'Số lượng',
        'gas': 'Gas',
        'block': 'Khối',
        'balance': 'Số dư',
        'pausing': 'Tạm nghỉ',
        'seconds': 'giây',
        'completed': 'HOÀN THÀNH: {successful}/{total} GIAO DỊCH THÀNH CÔNG',
        'error': 'Lỗi',
        'connect_success': 'Thành công: Đã kết nối mạng Optimism Sepolia',
        'connect_error': 'Không thể kết nối RPC',
        'web3_error': 'Kết nối Web3 thất bại',
        'pvkey_not_found': 'File pvkey.txt không tồn tại',
        'pvkey_empty': 'Không tìm thấy private key hợp lệ',
        'pvkey_error': 'Đọc pvkey.txt thất bại',
        'invalid_key': 'không hợp lệ, bỏ qua',
        'warning_line': 'Cảnh báo: Dòng',
        'gas_estimation_failed': 'Không thể ước lượng gas',
        'default_gas_used': 'Sử dụng gas mặc định: {gas}',
        'tx_rejected': 'Giao dịch bị từ chối bởi mạng',
        'select_action': 'Chọn hành động [1-2]',
        'action_provide_remove': '1. Cung cấp và rút thanh khoản',
        'action_all_tokens': '2. Cung cấp và rút thanh khoản cho tất cả token',
        'invalid_choice': 'Lựa chọn không hợp lệ, vui lòng chọn 1 hoặc 2',
        'select_token': 'Chọn token [1-{max}]',
        'invalid_token': 'Token không hợp lệ, vui lòng chọn từ 1-{max}',
        'amount_prompt': 'Nhập số lượng {token} để cung cấp',
        'invalid_amount': 'Số lượng không hợp lệ, vui lòng nhập số lớn hơn 0',
        'using_proxy': 'Sử dụng Proxy - [{proxy}] với IP công khai - [{public_ip}]',
        'no_proxy': 'Không có proxy',
        'unknown': 'Không xác định',
        'no_proxies': 'Không tìm thấy proxy trong proxies.txt',
        'invalid_proxy': 'Proxy không hợp lệ hoặc không hoạt động: {proxy}',
        'proxy_error': 'Lỗi kết nối proxy: {error}',
        'ip_check_failed': 'Không thể kiểm tra IP công khai: {error}',
        'pending_tx_warning': 'Cảnh báo: Ví có giao dịch đang chờ xử lý, chờ {timeout}s...',
        'pending_tx_timeout': 'Hết thời gian chờ giao dịch đang chờ xử lý',
        'retrying_tx': 'Thử lại giao dịch (lần {attempt}/{max_retries}) với gas price cao hơn',
        'retrieving_shares': 'Đang lấy số lượng cổ phần...',
        'shares_not_found': 'Không tìm thấy cổ phần cho {token}',
        'shares_retrieved': 'Đã lấy được {shares} cổ phần',
        'removing_shares': 'Rút {percent}% cổ phần ({shares} shares)',
        'loading_shares': 'Đang tải thông tin cổ phần từ shares.json...',
        'saving_shares': 'Đã lưu thông tin cổ phần vào shares.json',
        'shares_file_error': 'Lỗi khi đọc/ghi shares.json: {error}',
        'log_parsing_failed': 'Thất bại khi phân tích log giao dịch: {error}',
    },
    'en': {
        'title': 'PROVIDE/REMOVE LIQUIDITY - OPTIMISM SEPOLIA',
        'info': 'Info',
        'found': 'Found',
        'wallets': 'wallets',
        'found_proxies': 'Found {count} proxies in proxies.txt',
        'processing_wallet': 'PROCESSING WALLET',
        'checking_balance': 'Checking balance...',
        'insufficient_balance': 'Insufficient balance',
        'insufficient_token_balance': 'Insufficient balance for {token}: Available {balance:.6f}, Required {required:.6f}',
        'preparing_tx': 'Preparing transaction...',
        'sending_tx': 'Sending transaction...',
        'success': 'Success: {action} {amount:.6f} {token}',
        'failure': 'Failed: {action}',
        'address': 'Wallet address',
        'amount': 'Amount',
        'gas': 'Gas',
        'block': 'Block',
        'balance': 'Balance',
        'pausing': 'Pausing',
        'seconds': 'seconds',
        'completed': 'COMPLETED: {successful}/{total} TRANSACTIONS SUCCESSFUL',
        'error': 'Error',
        'connect_success': 'Success: Connected to Optimism Sepolia',
        'connect_error': 'Failed to connect to RPC',
        'web3_error': 'Web3 connection failed',
        'pvkey_not_found': 'pvkey.txt file not found',
        'pvkey_empty': 'No valid private keys found',
        'pvkey_error': 'Failed to read pvkey.txt',
        'invalid_key': 'is invalid, skipped',
        'warning_line': 'Warning: Line',
        'gas_estimation_failed': 'Failed to estimate gas',
        'default_gas_used': 'Using default gas: {gas}',
        'tx_rejected': 'Transaction rejected by network',
        'select_action': 'Select action [1-2]',
        'action_provide_remove': '1. Provide and remove liquidity',
        'action_all_tokens': '2. Provide and remove liquidity for all tokens',
        'invalid_choice': 'Invalid choice, please select 1 or 2',
        'select_token': 'Select token [1-{max}]',
        'invalid_token': 'Invalid token, please select from 1-{max}',
        'amount_prompt': 'Enter amount of {token} to provide',
        'invalid_amount': 'Invalid amount, please enter a number greater than 0',
        'using_proxy': 'Using Proxy - [{proxy}] with Public IP - [{public_ip}]',
        'no_proxy': 'None',
        'unknown': 'Unknown',
        'no_proxies': 'No proxies found in proxies.txt',
        'invalid_proxy': 'Invalid or unresponsive proxy: {proxy}',
        'proxy_error': 'Proxy connection error: {error}',
        'ip_check_failed': 'Failed to check public IP: {error}',
        'pending_tx_warning': 'Warning: Wallet has pending transactions, waiting {timeout}s...',
        'pending_tx_timeout': 'Timeout waiting for pending transactions',
        'retrying_tx': 'Retrying transaction (attempt {attempt}/{max_retries}) with higher gas price',
        'retrieving_shares': 'Retrieving shares amount...',
        'shares_not_found': 'Shares not found for {token}',
        'shares_retrieved': 'Retrieved {shares} shares',
        'removing_shares': 'Removing {percent}% shares ({shares} shares)',
        'loading_shares': 'Loading shares data from shares.json...',
        'saving_shares': 'Saved shares data to shares.json',
        'shares_file_error': 'Error reading/writing shares.json: {error}',
        'log_parsing_failed': 'Failed to parse transaction log: {error}',
    }
}

# Display functions
def print_border(text: str, color=Fore.CYAN, width=BORDER_WIDTH):
    text = text.strip()
    if len(text) > width - 4:
        text = text[:width - 7] + "..."
    padded_text = f" {text} ".center(width - 2)
    print(f"{color}┌{'─' * (width - 2)}┐{Style.RESET_ALL}")
    print(f"{color}│{padded_text}│{Style.RESET_ALL}")
    print(f"{color}└{'─' * (width - 2)}┘{Style.RESET_ALL}")

def print_separator(color=Fore.MAGENTA):
    print(f"{color}{'═' * BORDER_WIDTH}{Style.RESET_ALL}")

# Utility functions
def is_valid_private_key(key: str) -> bool:
    key = key.strip()
    if not key.startswith('0x'):
        key = '0x' + key
    try:
        bytes.fromhex(key.replace('0x', ''))
        return len(key) == 66
    except ValueError:
        return False

def load_private_keys(file_path: str = "pvkey.txt", language: str = 'en') -> list:
    try:
        if not os.path.exists(file_path):
            print(f"{Fore.RED}  ✖ {LANG[language]['pvkey_not_found']}{Style.RESET_ALL}")
            with open(file_path, 'w') as f:
                f.write("# Add private keys here, one per line\n# Example: 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef\n")
            sys.exit(1)
        
        valid_keys = []
        with open(file_path, 'r') as f:
            for i, line in enumerate(f, 1):
                key = line.strip()
                if key and not key.startswith('#'):
                    if is_valid_private_key(key):
                        if not key.startswith('0x'):
                            key = '0x' + key
                        valid_keys.append((i, key))
                    else:
                        print(f"{Fore.YELLOW}  ⚠ {LANG[language]['warning_line']} {i} {LANG[language]['invalid_key']}: {key[:10]}...{Style.RESET_ALL}")
        
        if not valid_keys:
            print(f"{Fore.RED}  ✖ {LANG[language]['pvkey_empty']}{Style.RESET_ALL}")
            sys.exit(1)
        
        return valid_keys
    except Exception as e:
        print(f"{Fore.RED}  ✖ {LANG[language]['pvkey_error']}: {str(e)}{Style.RESET_ALL}")
        sys.exit(1)

def load_proxies(file_path: str = "proxies.txt", language: str = 'en') -> list:
    try:
        if not os.path.exists(file_path):
            print(f"{Fore.YELLOW}  ⚠ {LANG[language]['no_proxies']}{Style.RESET_ALL}")
            with open(file_path, 'w') as f:
                f.write("# Add proxies here, one per line\n# Example: socks5://user:pass@host:port or http://host:port\n")
            return []
        
        proxies = []
        with open(file_path, 'r') as f:
            for line in f:
                proxy = line.strip()
                if proxy and not proxy.startswith('#'):
                    proxies.append(proxy)
        
        if not proxies:
            print(f"{Fore.YELLOW}  ⚠ {LANG[language]['no_proxies']}{Style.RESET_ALL}")
            return []
        
        print(f"{Fore.YELLOW}  ℹ {LANG[language]['found_proxies'].format(count=len(proxies))}{Style.RESET_ALL}")
        return proxies
    except Exception as e:
        print(f"{Fore.RED}  ✖ {LANG[language]['error']}: {str(e)}{Style.RESET_ALL}")
        return []

async def get_proxy_ip(proxy: str = None, language: str = 'en') -> str:
    try:
        if proxy:
            if proxy.startswith(('socks5://', 'socks4://', 'http://', 'https://')):
                connector = ProxyConnector.from_url(proxy)
            else:
                parts = proxy.split(':')
                if len(parts) == 4:
                    proxy_url = f"socks5://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                    connector = ProxyConnector.from_url(proxy_url)
                elif len(parts) == 3 and '@' in proxy:
                    connector = ProxyConnector.from_url(f"socks5://{proxy}")
                else:
                    print(f"{Fore.YELLOW}  ⚠ {LANG[language]['invalid_proxy'].format(proxy=proxy)}{Style.RESET_ALL}")
                    return LANG[language]['unknown']
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(IP_CHECK_URL, headers=HEADERS) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('ip', LANG[language]['unknown'])
                    print(f"{Fore.YELLOW}  ⚠ {LANG[language]['ip_check_failed'].format(error=f'HTTP {response.status}')}{Style.RESET_ALL}")
                    return LANG[language]['unknown']
        else:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(IP_CHECK_URL, headers=HEADERS) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('ip', LANG[language]['unknown'])
                    print(f"{Fore.YELLOW}  ⚠ {LANG[language]['ip_check_failed'].format(error=f'HTTP {response.status}')}{Style.RESET_ALL}")
                    return LANG[language]['unknown']
    except Exception as e:
        print(f"{Fore.YELLOW}  ⚠ {LANG[language]['ip_check_failed'].format(error=str(e))}{Style.RESET_ALL}")
        return LANG[language]['unknown']

def connect_web3(language: str = 'en'):
    try:
        w3 = Web3(Web3.HTTPProvider(NETWORK_URL))
        if not w3.is_connected():
            print(f"{Fore.RED}  ✖ {LANG[language]['connect_error']}{Style.RESET_ALL}")
            sys.exit(1)
        print(f"{Fore.GREEN}  ✔ {LANG[language]['connect_success']} │ Chain ID: {w3.eth.chain_id}{Style.RESET_ALL}")
        return w3
    except Exception as e:
        print(f"{Fore.RED}  ✖ {LANG[language]['web3_error']}: {str(e)}{Style.RESET_ALL}")
        sys.exit(1)

def check_balance(w3: Web3, address: str, token_address: str, decimals: int, language: str = 'en') -> float:
    if token_address == "native":
        try:
            balance = w3.eth.get_balance(address)
            return float(w3.from_wei(balance, 'ether'))
        except Exception as e:
            print(f"{Fore.YELLOW}  ⚠ {LANG[language]['error']}: {str(e)}{Style.RESET_ALL}")
            return -1
    else:
        token_abi = ERC20_ABI
        checksum_address = Web3.to_checksum_address(token_address)
        contract = w3.eth.contract(address=checksum_address, abi=token_abi)
        try:
            balance = contract.functions.balanceOf(address).call()
            return balance / (10 ** decimals)
        except Exception as e:
            print(f"{Fore.YELLOW}  ⚠ {LANG[language]['error']}: {str(e)}{Style.RESET_ALL}")
            return -1

def display_token_balances(w3: Web3, address: str, language: str = 'en'):
    print_border(f"{LANG[language]['balance']}", Fore.CYAN)
    eth_balance = check_balance(w3, address, "native", 18, language)
    print(f"{Fore.YELLOW}  - ETH: {eth_balance:.6f}{Style.RESET_ALL}")
    for symbol, token_data in TOKENS.items():
        balance = check_balance(w3, address, token_data["address"], token_data["decimals"], language)
        print(f"{Fore.YELLOW}  - {symbol}: {balance:.6f}{Style.RESET_ALL}")

async def wait_for_pending_transactions(w3: Web3, address: str, language: str = 'en') -> bool:
    start_time = asyncio.get_event_loop().time()
    timeout = CONFIG['PENDING_TX_TIMEOUT']
    while asyncio.get_event_loop().time() - start_time < timeout:
        pending_nonce = w3.eth.get_transaction_count(address, 'pending')
        confirmed_nonce = w3.eth.get_transaction_count(address, 'latest')
        if pending_nonce <= confirmed_nonce:
            return True
        print(f"{Fore.YELLOW}  ⚠ {LANG[language]['pending_tx_warning'].format(timeout=timeout)}{Style.RESET_ALL}")
        await asyncio.sleep(5)
    print(f"{Fore.RED}  ✖ {LANG[language]['pending_tx_timeout']}{Style.RESET_ALL}")
    return False

def load_shares(address: str, language: str = 'en') -> dict:
    try:
        if os.path.exists('shares.json'):
            with open('shares.json', 'r') as f:
                shares_data = json.load(f)
            print(f"{Fore.YELLOW}  ℹ {LANG[language]['loading_shares']}{Style.RESET_ALL}")
            return shares_data.get(address, {})
        return {}
    except Exception as e:
        print(f"{Fore.RED}  ✖ {LANG[language]['shares_file_error'].format(error=str(e))}{Style.RESET_ALL}")
        return {}

def save_shares(address: str, shares_dict: dict, language: str = 'en'):
    try:
        shares_data = {}
        if os.path.exists('shares.json'):
            with open('shares.json', 'r') as f:
                shares_data = json.load(f)
        shares_data[address] = shares_dict
        with open('shares.json', 'w') as f:
            json.dump(shares_data, f, indent=2)
        print(f"{Fore.GREEN}  ✔ {LANG[language]['saving_shares']}{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}  ✖ {LANG[language]['shares_file_error'].format(error=str(e))}{Style.RESET_ALL}")

def extract_shares_from_receipt(receipt, token: str, language: str = 'en'):
    try:
        print(f"{Fore.CYAN}  > {LANG[language]['retrieving_shares']}{Style.RESET_ALL}")
        for log in receipt.logs:
            try:
                value = int.from_bytes(log.data, byteorder='big')
                if value > 0:
                    print(f"{Fore.YELLOW}  ℹ Found shares from log.data: {value}{Style.RESET_ALL}")
                    return value
            except Exception as e:
                print(f"{Fore.YELLOW}  ⚠ {LANG[language]['log_parsing_failed'].format(error=str(e))}{Style.RESET_ALL}")
        
        print(f"{Fore.YELLOW}  ⚠ {LANG[language]['shares_not_found'].format(token=token)}{Style.RESET_ALL}")
        return None
    except Exception as e:
        print(f"{Fore.RED}  ✖ {LANG[language]['error']} extracting shares: {str(e)}{Style.RESET_ALL}")
        return None

async def approve_token(w3: Web3, private_key: str, token_address: str, spender: str, amount: int, language: str = 'en', nonce: int = None):
    account = Account.from_key(private_key)
    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    if nonce is None:
        nonce = w3.eth.get_transaction_count(account.address, 'pending')
    
    for attempt in range(1, CONFIG['MAX_RETRIES'] + 1):
        gas_price = int(w3.eth.gas_price * (CONFIG['GAS_MULTIPLIER'] + 0.3 * attempt))
        try:
            allowance = token_contract.functions.allowance(account.address, Web3.to_checksum_address(spender)).call()
            if allowance >= amount:
                return True, nonce
            tx = token_contract.functions.approve(Web3.to_checksum_address(spender), 2**256 - 1).build_transaction({
                'nonce': nonce,
                'from': account.address,
                'chainId': CHAIN_ID,
                'gasPrice': gas_price
            })
            try:
                tx['gas'] = w3.eth.estimate_gas(tx)
                print(f"{Fore.YELLOW}    Gas estimated: {tx['gas']}{Style.RESET_ALL}")
            except Exception as e:
                tx['gas'] = 100000
                print(f"{Fore.YELLOW}    {LANG[language]['gas_estimation_failed']}: {str(e)}. {LANG[language]['default_gas_used'].format(gas=100000)}{Style.RESET_ALL}")
            
            print(f"{Fore.YELLOW}    Sending approve tx with nonce {nonce}, gasPrice {gas_price}{Style.RESET_ALL}")
            signed_tx = w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            receipt = await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: w3.eth.wait_for_transaction_receipt(tx_hash, timeout=CONFIG['TRANSACTION_TIMEOUT'])
            )
            if receipt.status == 1:
                print(f"{Fore.GREEN}    Approve tx successful: {tx_hash.hex()}{Style.RESET_ALL}")
                return True, nonce + 1
            else:
                print(f"{Fore.RED}    Approve tx failed: {tx_hash.hex()}{Style.RESET_ALL}")
                return False, nonce
        except Exception as e:
            if 'replacement transaction underpriced' in str(e) and attempt < CONFIG['MAX_RETRIES']:
                print(f"{Fore.YELLOW}  ⚠ {LANG[language]['retrying_tx'].format(attempt=attempt+1, max_retries=CONFIG['MAX_RETRIES'])}{Style.RESET_ALL}")
                nonce = w3.eth.get_transaction_count(account.address, 'pending')
                continue
            print(f"{Fore.RED}  ✖ Approve failed: {str(e)}{Style.RESET_ALL}")
            return False, nonce
    return False, nonce

async def process_liquidity(w3: Web3, private_key: str, wallet_index: int, action: str, token: str, amount_in: int, proxy: str = None, language: str = 'en', shares_dict: dict = None):
    account = Account.from_key(private_key)
    sender_address = account.address
    pool = w3.eth.contract(address=POOL_ADDR, abi=UNIFIED_ABI)
    
    action_display = 'Cung cấp' if action == 'provide' else 'Rút'
    
    print_border(f"{action_display} Thanh khoản cho {token}", Fore.YELLOW)
    
    public_ip = await get_proxy_ip(proxy, language)
    proxy_display = proxy if proxy else LANG[language]['no_proxy']
    print(f"{Fore.CYAN}  🔄 {LANG[language]['using_proxy'].format(proxy=proxy_display, public_ip=public_ip)}{Style.RESET_ALL}")

    print(f"{Fore.CYAN}  > {LANG[language]['checking_balance']}{Style.RESET_ALL}")
    eth_balance = float(w3.from_wei(w3.eth.get_balance(sender_address), 'ether'))
    if eth_balance < CONFIG['MINIMUM_BALANCE']:
        print(f"{Fore.RED}  ✖ {LANG[language]['insufficient_balance']}: {eth_balance:.4f} ETH < {CONFIG['MINIMUM_BALANCE']} ETH{Style.RESET_ALL}")
        return False, None
    
    token_balance = check_balance(w3, sender_address, TOKENS[token]["address"], TOKENS[token]["decimals"], language)
    if action == 'provide' and amount_in > token_balance * (10 ** TOKENS[token]["decimals"]):
        print(f"{Fore.RED}  ✖ {LANG[language]['insufficient_token_balance'].format(token=token, balance=token_balance, required=amount_in / (10 ** TOKENS[token]['decimals']))}{Style.RESET_ALL}")
        return False, None
    
    if not await wait_for_pending_transactions(w3, sender_address, language):
        return False, None

    print(f"{Fore.CYAN}  > {LANG[language]['preparing_tx']}{Style.RESET_ALL}")
    nonce = w3.eth.get_transaction_count(sender_address, 'pending')
    
    if action == 'provide':
        success, nonce = await approve_token(w3, private_key, TOKENS[token]["address"], POOL_ADDR, amount_in, language, nonce)
        if not success:
            return False, None
        tx_params = pool.functions.provideLiquidity(
            TOKENS[token]["address"], amount_in
        ).build_transaction({
            'nonce': nonce,
            'from': sender_address,
            'chainId': CHAIN_ID,
            'gasPrice': int(w3.eth.gas_price * CONFIG['GAS_MULTIPLIER'])
        })
    else:
        shares = amount_in
        tx_params = pool.functions.removeLiquiditySingleToken(
            TOKENS[token]["address"], shares
        ).build_transaction({
            'nonce': nonce,
            'from': sender_address,
            'chainId': CHAIN_ID,
            'gasPrice': int(w3.eth.gas_price * CONFIG['GAS_MULTIPLIER'])
        })
    
    for attempt in range(1, CONFIG['MAX_RETRIES'] + 1):
        gas_price = int(w3.eth.gas_price * (CONFIG['GAS_MULTIPLIER'] + 0.3 * attempt))
        tx_params['gasPrice'] = gas_price
        try:
            try:
                tx_params['gas'] = w3.eth.estimate_gas(tx_params)
                print(f"{Fore.YELLOW}    Gas estimated: {tx_params['gas']}{Style.RESET_ALL}")
            except Exception as e:
                tx_params['gas'] = CONFIG['DEFAULT_GAS_LIMIT']
                print(f"{Fore.YELLOW}    {LANG[language]['gas_estimation_failed']}: {str(e)}. {LANG[language]['default_gas_used'].format(gas=CONFIG['DEFAULT_GAS_LIMIT'])}{Style.RESET_ALL}")
            
            print(f"{Fore.CYAN}  > {LANG[language]['sending_tx']}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}    Sending tx with nonce {nonce}, gasPrice {gas_price}{Style.RESET_ALL}")
            signed_tx = w3.eth.account.sign_transaction(tx_params, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_link = f"{EXPLORER_URL}{tx_hash.hex()}"
            
            receipt = await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: w3.eth.wait_for_transaction_receipt(tx_hash, timeout=CONFIG['TRANSACTION_TIMEOUT'])
            )
            
            if receipt.status == 1:
                amount_display = amount_in / (10 ** TOKENS[token]["decimals"]) if action == 'provide' else amount_in
                print(f"{Fore.GREEN}  ✔ {LANG[language]['success'].format(action=action_display, amount=amount_display, token=token)} │ Tx: {tx_link}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}    {LANG[language]['address']:<12}: {sender_address}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}    {LANG[language]['block']:<12}: {receipt['blockNumber']}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}    {LANG[language]['gas']:<12}: {receipt['gasUsed']}{Style.RESET_ALL}")
                eth_balance_after = float(w3.from_wei(w3.eth.get_balance(sender_address), 'ether'))
                token_balance_after = check_balance(w3, sender_address, TOKENS[token]["address"], TOKENS[token]["decimals"], language)
                print(f"{Fore.YELLOW}    {LANG[language]['balance']:<12}: {eth_balance_after:.6f} ETH | {token}: {token_balance_after:.6f}{Style.RESET_ALL}")
                
                shares = extract_shares_from_receipt(receipt, token, language) if action == 'provide' else None
                if shares:
                    print(f"{Fore.GREEN}    {LANG[language]['shares_retrieved'].format(shares=shares)}{Style.RESET_ALL}")
                
                return True, shares
            else:
                print(f"{Fore.RED}  ✖ {LANG[language]['failure'].format(action=action_display)} │ Tx: {tx_link}{Style.RESET_ALL}")
                print(f"{Fore.RED}    {LANG[language]['tx_rejected']}{Style.RESET_ALL}")
                return False, None
        except Exception as e:
            if 'replacement transaction underpriced' in str(e) and attempt < CONFIG['MAX_RETRIES']:
                print(f"{Fore.YELLOW}  ⚠ {LANG[language]['retrying_tx'].format(attempt=attempt+1, max_retries=CONFIG['MAX_RETRIES'])}{Style.RESET_ALL}")
                nonce = w3.eth.get_transaction_count(sender_address, 'pending')
                tx_params['nonce'] = nonce
                continue
            print(f"{Fore.RED}  ✖ {LANG[language]['failure'].format(action=action_display)}: {str(e)}{Style.RESET_ALL}")
            return False, None
    return False, None

async def process_wallet(index: int, profile_num: int, private_key: str, proxy: str, w3: Web3, language: str):
    total_wallets = CONFIG.get('TOTAL_WALLETS', 1)
    print_border(
        f"{LANG[language]['processing_wallet']} {profile_num} ({index + 1}/{total_wallets})",
        Fore.MAGENTA
    )
    account = Account.from_key(private_key)
    print(f"{Fore.YELLOW}  {LANG[language]['address']}: {account.address}{Style.RESET_ALL}")
    display_token_balances(w3, account.address, language)
    print_separator()

    print(f"{Fore.CYAN}{LANG[language]['select_action']}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  {LANG[language]['action_provide_remove']}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}  {LANG[language]['action_all_tokens']}{Style.RESET_ALL}")
    while True:
        try:
            action_choice = int(input(f"{Fore.GREEN}  > {Style.RESET_ALL}"))
            if action_choice in [1, 2]:
                break
            print(f"{Fore.RED}  ✖ {LANG[language]['invalid_choice']}{Style.RESET_ALL}")
        except ValueError:
            print(f"{Fore.RED}  ✖ {LANG[language]['invalid_choice']}{Style.RESET_ALL}")

    shares_dict = load_shares(account.address, language)
    successful_txs = 0

    if action_choice == 2:  # Provide and remove liquidity for all tokens
        for token in TOKENS.keys():
            default_amounts = {
                "WETH": 0.02,
                "CRV": 0.11,
                "SUSHI": 0.07,
                "UNI": 0.11,
                "USDC": 0.07,
                "wBTC": 0.001
            }
            amount_input = default_amounts.get(token, 0.01)
            amount_in = int(amount_input * 10 ** TOKENS[token]["decimals"])
            
            try:
                # Provide liquidity
                success, shares = await process_liquidity(w3, private_key, profile_num, 'provide', token, amount_in, proxy, language, shares_dict)
                if success and shares:
                    successful_txs += 1
                    shares_dict[token] = shares
                    
                    # Randomly remove 10-50% of shares
                    remove_percent = random.randint(10, 50)
                    shares_to_remove = int(shares * remove_percent / 100)
                    print(f"{Fore.CYAN}  > {LANG[language]['removing_shares'].format(percent=remove_percent, shares=shares_to_remove)}{Style.RESET_ALL}")
                    remove_success, _ = await process_liquidity(w3, private_key, profile_num, 'remove', token, shares_to_remove, proxy, language, shares_dict)
                    if remove_success:
                        successful_txs += 1
                        shares_dict[token] = shares - shares_to_remove
                    save_shares(account.address, shares_dict, language)
                elif not success:
                    print(f"{Fore.RED}  ✖ Thất bại khi cung cấp thanh khoản cho {token}{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}  ✖ Lỗi xử lý {token}: {str(e)}{Style.RESET_ALL}")
            await asyncio.sleep(random.uniform(5, 10))
    else:  # Provide and remove liquidity for a single token
        token_list = list(TOKENS.keys())
        print(f"{Fore.CYAN}{LANG[language]['select_token'].format(max=len(token_list))}{Style.RESET_ALL}")
        for i, token in enumerate(token_list, 1):
            print(f"{Fore.YELLOW}  {i}. {token}{Style.RESET_ALL}")
        while True:
            try:
                token_choice = int(input(f"{Fore.GREEN}  > {Style.RESET_ALL}"))
                if 1 <= token_choice <= len(token_list):
                    token = token_list[token_choice - 1]
                    break
                print(f"{Fore.RED}  ✖ {LANG[language]['invalid_token'].format(max=len(token_list))}{Style.RESET_ALL}")
            except ValueError:
                print(f"{Fore.RED}  ✖ {LANG[language]['invalid_token'].format(max=len(token_list))}{Style.RESET_ALL}")

        token_balance = check_balance(w3, account.address, TOKENS[token]["address"], TOKENS[token]["decimals"], language)
        max_amount = token_balance

        print()
        while True:
            print(f"{Fore.CYAN}{LANG[language]['amount_prompt'].format(token=token)} {Fore.YELLOW}(Max: {max_amount:.6f} {token}){Style.RESET_ALL}")
            try:
                amount_input = float(input(f"{Fore.GREEN}  > {Style.RESET_ALL}"))
                if amount_input > 0 and amount_input <= max_amount:
                    amount_in = int(amount_input * 10 ** TOKENS[token]["decimals"])
                    break
                print(f"{Fore.RED}  ✖ {LANG[language]['invalid_amount']} hoặc vượt quá số dư{Style.RESET_ALL}")
            except ValueError:
                print(f"{Fore.RED}  ✖ {LANG[language]['invalid_amount']}{Style.RESET_ALL}")

        print()
        # Provide liquidity
        success, shares = await process_liquidity(w3, private_key, profile_num, 'provide', token, amount_in, proxy, language, shares_dict)
        if success and shares:
            successful_txs += 1
            shares_dict[token] = shares
            
            # Randomly remove 10-50% of shares
            remove_percent = random.randint(10, 50)
            shares_to_remove = int(shares * remove_percent / 100)
            print(f"{Fore.CYAN}  > {LANG[language]['removing_shares'].format(percent=remove_percent, shares=shares_to_remove)}{Style.RESET_ALL}")
            remove_success, _ = await process_liquidity(w3, private_key, profile_num, 'remove', token, shares_to_remove, proxy, language, shares_dict)
            if remove_success:
                successful_txs += 1
                shares_dict[token] = shares - shares_to_remove
            save_shares(account.address, shares_dict, language)

    print_separator(Fore.GREEN if successful_txs > 0 else Fore.RED)
    return successful_txs

async def run_liquidity(language: str = 'vi'):
    print()
    print_border(LANG[language]['title'], Fore.CYAN)
    print()

    private_keys = load_private_keys('pvkey.txt', language)
    proxies = load_proxies('proxies.txt', language)
    print(f"{Fore.YELLOW}  ℹ {LANG[language]['info']}: {LANG[language]['found']} {len(private_keys)} {LANG[language]['wallets']}{Style.RESET_ALL}")
    print()

    if not private_keys:
        return

    w3 = connect_web3(language)
    print()

    successful_txs = 0
    total_txs = len(private_keys)
    failed_attempts = 0
    CONFIG['TOTAL_WALLETS'] = len(private_keys)
    CONFIG['MAX_CONCURRENCY'] = min(CONFIG['MAX_CONCURRENCY'], len(private_keys))

    random.shuffle(private_keys)
    semaphore = asyncio.Semaphore(CONFIG['MAX_CONCURRENCY'])

    async def limited_task(index, profile_num, private_key, proxy):
        nonlocal successful_txs, failed_attempts
        async with semaphore:
            result = await process_wallet(index, profile_num, private_key, proxy, w3, language)
            successful_txs += result
            if result == 0:
                failed_attempts += 1
                if failed_attempts >= CONFIG['MAX_RETRIES']:
                    print(f"{Fore.RED}  ✖ Dừng ví {profile_num}: Quá nhiều giao dịch thất bại liên tiếp{Style.RESET_ALL}")
                    return
            else:
                failed_attempts = 0
            if index < len(private_keys) - 1:
                delay = random.uniform(CONFIG['PAUSE_BETWEEN_ATTEMPTS'][0], CONFIG['PAUSE_BETWEEN_ATTEMPTS'][1])
                print(f"{Fore.YELLOW}  ℹ {LANG[language]['pausing']} {delay:.2f} {LANG[language]['seconds']}{Style.RESET_ALL}")
                await asyncio.sleep(delay)

    tasks = []
    for i, (profile_num, private_key) in enumerate(private_keys):
        proxy = proxies[i % len(proxies)] if proxies else None
        tasks.append(limited_task(i, profile_num, private_key, proxy))

    await asyncio.gather(*tasks, return_exceptions=True)

    print()
    print_border(
        f"{LANG[language]['completed'].format(successful=successful_txs, total=total_txs)}",
        Fore.GREEN
    )
    print()

if __name__ == "__main__":
    asyncio.run(run_liquidity('vi'))
