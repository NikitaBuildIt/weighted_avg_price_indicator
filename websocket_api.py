import websockets
import json
import traceback
from datetime import datetime, timedelta
import aiohttp
import asyncio
import ssl
import re
from loguru import logger
from supported_pairs import data as supported_pairs


updated_data_dict = {}
avg_prices = {}
precision = {'BTCUSDT': 2, 'ETHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 4, 'ETHBTC': 6, 'XRPBTC': 8, 'DOTUSDT': 3, 'XLMUSDT': 5, 'LTCUSDT': 2, 'DOGEUSDT': 5, 'CHZUSDT': 4, 'AXSUSDT': 4, 'MANAUSDT': 4, 'DYDXUSDT': 3, 'MKRUSDT': 1, 'COMPUSDT': 2, 'AAVEUSDT': 4, 'YFIUSDT': 2, 'LINKUSDT': 4, 'SUSHIUSDT': 3, 'UNIUSDT': 4, 'KSMUSDT': 4, 'ICPUSDT': 4, 'ADAUSDT': 4, 'ETCUSDT': 2, 'KLAYUSDT': 5, 'XTZUSDT': 4, 'BCHUSDT': 1, 'SRMUSDT': 4, 'QNTUSDT': 1, 'USDCUSDT': 4, 'GRTUSDT': 5, 'SOLUSDT': 2, 'FILUSDT': 3, 'OMGUSDT': 3, 'TRIBEUSDT': 4, 'BATUSDT': 4, 'ZRXUSDT': 4, 'CRVUSDT': 4, 'AGLDUSDT': 4, 'ANKRUSDT': 5, 'PERPUSDT': 4, 'MATICUSDT': 4, 'WAVESUSDT': 4, 'LUNCUSDT': 8, 'SPELLUSDT': 8, 'SHIBUSDT': 10, 'FTMUSDT': 5, 'ATOMUSDT': 4, 'ALGOUSDT': 5, 'ENJUSDT': 5, 'CBXUSDT': 5, 'SANDUSDT': 5, 'AVAXUSDT': 4, 'WOOUSDT': 5, 'FTTUSDT': 4, 'GODSUSDT': 5, 'IMXUSDT': 5, 'ENSUSDT': 3, 'GMUSDT': 10, 'CWARUSDT': 5, 'CAKEUSDT': 4, 'STETHUSDT': 2, 'GALFTUSDT': 5, 'LFWUSDT': 6, 'SLPUSDT': 6, 'C98USDT': 4, 'PSPUSDT': 6, 'GENEUSDT': 4, 'AVAUSDT': 4, 'ONEUSDT': 5, 'PTUUSDT': 4, 'SHILLUSDT': 5, 'XYMUSDT': 5, 'BOBAUSDT': 5, 'JASMYUSDT': 6, 'GALAUSDT': 5, 'RNDRUSDT': 4, 'TRVLUSDT': 6, 'WEMIXUSDT': 4, 'XEMUSDT': 5, 'BICOUSDT': 4, 'CELUSDT': 4, 'UMAUSDT': 4, 'HOTUSDT': 6, 'NEXOUSDT': 4, 'BNTUSDT': 4, 'SNXUSDT': 4, 'RENUSDT': 5, '1INCHUSDT': 4, 'TELUSDT': 6, 'SISUSDT': 5, 'LRCUSDT': 4, 'LDOUSDT': 4, 'REALUSDT': 5, 'KRLUSDT': 4, 'DEVTUSDT': 7, 'ETHUSDC': 2, 'BTCUSDC': 2, '1SOLUSDT': 6, 'PLTUSDT': 6, 'IZIUSDT': 6, 'QTUMUSDT': 4, 'DCRUSDT': 4, 'ZENUSDT': 4, 'THETAUSDT': 4, 'MXUSDT': 4, 'DGBUSDT': 6, 'RVNUSDT': 5, 'EGLDUSDT': 2, 'RUNEUSDT': 4, 'XLMBTC': 10, 'XLMUSDC': 5, 'SOLUSDC': 2, 'XRPUSDC': 4, 'ALGOBTC': 8, 'SOLBTC': 7, 'RAINUSDT': 7, 'XECUSDT': 8, 'ICXUSDT': 5, 'XDCUSDT': 5, 'HNTUSDT': 4, 'BTGUSDT': 4, 'ZILUSDT': 5, 'HBARUSDT': 5, 'FLOWUSDT': 4, 'SOSUSDT': 11, 'KASTAUSDT': 6, 'STXUSDT': 5, 'SIDUSUSDT': 7, 'VPADUSDT': 5, 'LOOKSUSDT': 4, 'MBSUSDT': 6, 'DAIUSDT': 4, 'ACAUSDT': 4, 'MVUSDT': 5, 'MIXUSDT': 6, 'LTCUSDC': 2, 'MANABTC': 8, 'MATICBTC': 8, 'LTCBTC': 6, 'DOTBTC': 8, 'SANDBTC': 8, 'MANAUSDC': 4, 'MATICUSDC': 4, 'SANDUSDC': 5, 'DOTUSDC': 3, 'LUNCUSDC': 8, 'RSS3USDT': 5, 'TAPUSDT': 6, 'ERTHAUSDT': 6, 'GMXUSDT': 4, 'TUSDT': 5, 'ACHUSDT': 6, 'JSTUSDT': 5, 'SUNUSDT': 6, 'BTTUSDT': 10, 'TRXUSDT': 5, 'NFTUSDT': 10, 'POKTUSDT': 5, 'SCRTUSDT': 4, 'PSTAKEUSDT': 5, 'SONUSDT': 7, 'HEROUSDT': 6, 'DOMEUSDT': 6, 'USTCUSDT': 5, 'BNBUSDT': 2, 'NEARUSDT': 4, 'PAXGUSDT': 4, 'SDUSDT': 4, 'APEUSDT': 4, 'BTC3SUSDT': 4, 'BTC3LUSDT': 4, 'FIDAUSDT': 4, 'MINAUSDT': 4, 'SCUSDT': 6, 'RACAUSDT': 8, 'CAPSUSDT': 6, 'STGUSDT': 4, 'GLMRUSDT': 4, 'MOVRUSDT': 4, 'ZAMUSDT': 6, 'ETHDAI': 2, 'BTCDAI': 2, 'WBTCUSDT': 2, 'XAVAUSDT': 4, 'MELOSUSDT': 6, 'GMTUSDT': 4, 'GSTUSDT': 5, 'CELOUSDT': 4, 'SFUNDUSDT': 5, 'ELTUSDT': 7, 'LGXUSDT': 6, 'APEXUSDT': 4, 'CTCUSDT': 6, 'COTUSDT': 6, 'KMONUSDT': 6, 'PLYUSDT': 7, 'XWGUSDT': 7, 'FITFIUSDT': 6, 'STRMUSDT': 6, 'GALUSDT': 4, 'ETH3SUSDT': 5, 'ETH3LUSDT': 4, 'KOKUSDT': 6, 'FAMEUSDT': 6, 'USDDUSDT': 4, 'OPUSDT': 4, 'LUNAUSDT': 4, 'DFIUSDT': 4, 'MOVEZUSDT': 7, 'THNUSDT': 6, 'VINUUSDT': 12, 'BELUSDT': 4, 'FORTUSDT': 4, 'WLKNUSDT': 6, 'KONUSDT': 7, 'OBXUSDT': 7, 'SEORUSDT': 6, 'MNZUSDT': 7, 'DOGEUSDC': 5, 'EOSUSDC': 4, 'CUSDUSDT': 4, 'CMPUSDT': 5, 'KUNCIUSDT': 6, 'GSTSUSDT': 6, 'XETAUSDT': 6, 'AZYUSDT': 5, 'MMCUSDT': 6, 'FLOKIUSDT': 9, 'BABYDOGEUSDT': 12, 'STATUSDT': 4, 'DICEUSDT': 5, 'WAXPUSDT': 5, 'ARUSDT': 4, 'KDAUSDT': 4, 'ROSEUSDT': 5, 'DEFYUSDT': 8, 'PSGUSDT': 4, 'BARUSDT': 4, 'JUVUSDT': 4, 'ACMUSDT': 4, 'INTERUSDT': 4, 'AFCUSDT': 4, 'CITYUSDT': 4, 'SOLOUSDT': 5, 'WBTCBTC': 4, 'AVAXUSDC': 4, 'ADAUSDC': 4, 'OPUSDC': 4, 'APEXUSDC': 4, 'TRXUSDC': 5, 'ICPUSDC': 4, 'LINKUSDC': 4, 'GMTUSDC': 4, 'CHZUSDC': 4, 'SHIBUSDC': 10, 'LDOUSDC': 4, 'APEUSDC': 4, 'FILUSDC': 3, 'CHRPUSDT': 6, 'WWYUSDT': 6, 'LINGUSDT': 8, 'SWEATUSDT': 6, 'DLCUSDT': 5, 'OKGUSDT': 6, 'ETHWUSDT': 4, 'INJUSDT': 3, 'MPLXUSDT': 5, 'COUSDT': 6, 'AGLAUSDT': 5, 'RONDUSDT': 6, 'QMALLUSDT': 6, 'PUMLXUSDT': 5, 'GCAKEUSDT': 10, 'APTUSDT': 4, 'APTUSDC': 4, 'USDTEUR': 4, 'MCRTUSDT': 6, 'MASKUSDT': 4, 'ECOXUSDT': 4, 'HFTUSDC': 4, 'HFTUSDT': 4, 'KCALUSDT': 4, 'PEOPLEUSDT': 5, 'TWTUSDT': 4, 'ORTUSDT': 6, 'HOOKUSDT': 4, 'PRIMALUSDT': 7, 'MCTUSDT': 5, 'OASUSDT': 6, 'MAGICUSDT': 4, 'MEEUSDT': 6, 'TONUSDT': 4, 'BONKUSDT': 10, 'FLRUSDT': 5, 'TIMEUSDT': 4, '3PUSDT': 12, 'RPLUSDT': 4, 'SSVUSDT': 4, 'FXSUSDT': 4, 'COREUSDT': 4, 'RDNTUSDT': 4, 'BLURUSDT': 5, 'LISUSDT': 6, 'MDAOUSDT': 5, 'ACSUSDT': 7, 'HVHUSDT': 5, 'GNSUSDT': 4, 'DPXUSDT': 3, 'PIPUSDT': 5, 'PRIMEUSDT': 3, 'EVERUSDT': 5, 'VRAUSDT': 6, 'FBUSDT': 3, 'DZOOUSDT': 6, 'IDUSDT': 5, 'ARBUSDC': 3, 'ARBUSDT': 3, 'XCADUSDT': 4, 'MBXUSDT': 3, 'AXLUSDT': 4, 'CGPTUSDT': 6, 'AGIUSDT': 5, 'SUIUSDT': 4, 'SUIUSDC': 4, 'TAMAUSDT': 5, 'MVLUSDT': 6, 'PEPEUSDT': 9, 'LADYSUSDT': 11, 'LMWRUSDT': 4, 'BOBUSDT': 8, 'TOMIUSDT': 4, 'KARATEUSDT': 7, 'SUIAUSDT': 4, 'TURBOSUSDT': 6, 'FMBUSDT': 5, 'TENETUSDT': 5, 'VELOUSDT': 6, 'ELDAUSDT': 6, 'CANDYUSDT': 5, 'FONUSDT': 4, 'OMNUSDT': 6, 'VELAUSDT': 4, 'USDTBRZ': 3, 'BTCBRZ': 1, 'PENDLEUSDT': 4, 'EGOUSDT': 5, 'NYMUSDT': 4, 'MNTUSDT': 4, 'MNTUSDC': 4, 'MNTBTC': 8, 'GSWIFTUSDT': 5, 'SALDUSDT': 6, 'ARKMUSDT': 5, 'NEONUSDT': 5, 'WLDUSDC': 4, 'WLDUSDT': 4, 'PLANETUSDT': 8, 'DSRUNUSDT': 5, 'SPARTAUSDT': 4, 'TAVAUSDT': 5, 'SEILORUSDT': 6, 'SEIUSDT': 4, 'CYBERUSDT': 4, 'ORDIUSDT': 4, 'KAVAUSDT': 4, 'VVUSDT': 6, 'SAILUSDT': 5, 'PYUSDUSDT': 4, 'SOLEUR': 2, 'USDCEUR': 4, 'ADAEUR': 4, 'DOGEEUR': 5, 'LTCEUR': 2, 'XRPEUR': 4, 'ETHEUR': 2, 'BTCEUR': 2, 'VEXTUSDT': 5, 'CTTUSDT': 6, 'NEXTUSDT': 5, 'KASUSDT': 5, 'NESSUSDT': 5, 'CATUSDT': 6, 'FETUSDT': 4, 'LEVERUSDT': 6, 'VEGAUSDT': 4, 'ZTXUSDT': 5, 'JEFFUSDT': 5, 'PPTUSDT': 4, 'TUSDUSDT': 4, 'BEAMUSDT': 6, 'POLUSDT': 4, 'TIAUSDT': 4, 'TOKENUSDT': 5, 'MEMEUSDT': 6, 'SHRAPUSDT': 5, 'RPKUSDT': 5, 'FLIPUSDT': 4, 'CRDSUSDT': 5, 'VRTXUSDT': 4, 'ROOTUSDT': 6, 'PYTHUSDT': 5, 'MLKUSDT': 4, 'TVKUSDT': 5, 'KUBUSDT': 4, '5IREUSDT': 5, 'KCSUSDT': 4, 'VANRYUSDT': 5, 'INSPUSDT': 5, 'JTOUSDT': 5, 'METHUSDT': 2, 'METHETH': 4, 'CBKUSDT': 4, 'ZIGUSDT': 5, 'VPRUSDT': 5, 'TRCUSDT': 5, 'SEIUSDC': 4, 'FMCUSDT': 6, 'MYRIAUSDT': 6, 'AKIUSDT': 5, 'MBOXUSDT': 4, 'IRLUSDT': 5, 'ARTYUSDT': 4, 'GRAPEUSDT': 5, 'COQUSDT': 9, 'AIOZUSDT': 4, 'GGUSDT': 4, 'VICUSDT': 4, 'COMUSDT': 4, 'RATSUSDT': 7, 'SATSUSDT': 10, 'ZKFUSDT': 6, 'PORT3USDT': 5, 'XAIUSDT': 5, 'ONDOUSDT': 5, 'HONUSDT': 6, 'FARUSDT': 5, 'SQRUSDT': 5, 'DUELUSDT': 6, 'APPUSDT': 6, 'SAROSUSDT': 6, 'USDYUSDT': 4, 'MANTAUSDT': 4, 'MYROUSDT': 4, 'GTAIUSDT': 4, 'OMNICATUSDT': 7, 'DMAILUSDT': 4, 'AFGUSDT': 5, 'DYMUSDT': 4, 'ZETAUSDT': 4, 'JUPUSDT': 4, 'MXMUSDT': 5, 'DEFIUSDT': 4, 'FIREUSDT': 4, 'MINUUSDT': 6, 'MAVIAUSDT': 4, 'PURSEUSDT': 7, 'LENDSUSDT': 5, 'ROUTEUSDT': 4, 'BCUTUSDT': 5, 'ALTUSDT': 5, 'HTXUSDT': 9, 'CSPRUSDT': 5, 'STRKUSDC': 3, 'STRKUSDT': 3, 'CPOOLUSDT': 5, 'QORPOUSDT': 5, 'BBLUSDT': 5, 'DECHATUSDT': 4, 'SQTUSDT': 5, 'PORTALUSDT': 4, 'AEGUSDT': 5, 'SCAUSDT': 4, 'AEVOUSDT': 4, 'NIBIUSDT': 5, 'STARUSDT': 5, 'NGLUSDT': 5, 'ZENDUSDT': 4, 'BOMEUSDT': 6, 'VENOMUSDT': 4, 'ZKJUSDT': 4, 'ETHFIUSDT': 4, 'WEETHETH': 4, 'AURYUSDT': 4, 'FLTUSDT': 4, 'NAKAUSDT': 4, 'GMRXUSDT': 6, 'APRSUSDT': 4, 'WENUSDT': 7, 'BRAWLUSDT': 7, 'DEGENUSDT': 5, 'MFERUSDT': 5, 'BONUSUSDT': 4, 'ENAUSDT': 4, 'USDEUSDT': 4, 'VELARUSDT': 5, 'XARUSDT': 5, 'WUSDT': 4, 'EVERYUSDT': 5, 'MOJOUSDT': 5, 'G3USDT': 5, 'ESEUSDT': 5, 'TNSRUSDT': 4, 'BLOCKUSDT': 4, 'MASAUSDT': 5, 'FOXYUSDT': 6, 'SHARKUSDT': 4, 'PRCLUSDT': 4, 'MSTARUSDT': 5, 'OMNIUSDT': 3, 'BRETTUSDT': 5, 'MEWUSDT': 6, 'MERLUSDT': 4, 'PBUXUSDT': 5, 'EXVGUSDT': 5, 'LLUSDT': 5, 'SAFEUSDT': 4, 'WIFUSDT': 4, 'LAIUSDT': 5, 'GUMMYUSDT': 6, 'SVLUSDT': 6, 'KMNOUSDT': 5, 'ZENTUSDT': 5, 'TAIUSDT': 5, 'MODEUSDT': 5, 'SPECUSDT': 4, 'GALAXISUSDT': 5, 'ZEROUSDT': 7, 'LFTUSDT': 5, 'BUBBLEUSDT': 6, 'PONKEUSDT': 5, 'BBUSDT': 4, 'CTAUSDT': 5, 'NOTUSDT': 6, 'NLKUSDT': 5, 'DRIFTUSDT': 4, 'SQDUSDT': 5, 'NUTSUSDT': 6, 'NYANUSDT': 5, 'HLGUSDT': 6, 'USDEUSDC': 4, 'SOLUSDE': 2, 'ETHUSDE': 2, 'BTCUSDE': 2, 'MONUSDT': 5, 'PARAMUSDT': 5, 'ELIXUSDT': 5, 'INTXUSDT': 4, 'APTRUSDT': 5, 'RUBYUSDT': 5, 'MOGUSDT': 9, 'TAIKOUSDT': 4, 'ULTIUSDT': 5, 'AURORAUSDT': 4, 'AARKUSDT': 5, 'IOUSDT': 4, 'ATHUSDT': 6, 'COOKIEUSDT': 5, 'PIRATEUSDT': 5, 'XZKUSDT': 4, 'ZKUSDC': 4, 'ZKUSDT': 4, 'POPCATUSDT': 4, 'ZROUSDC': 4, 'ZROUSDT': 4, 'NRNUSDT': 5, 'MCGUSDT': 5, 'ZEXUSDT': 5, 'BLASTUSDT': 6, 'TSTUSDT': 5, 'BTCBRL': 1, 'ETHBRL': 2, 'SOLBRL': 1, 'USDTBRL': 3, 'USDCBRL': 3, 'WELLUSDT': 7, 'PTCUSDT': 4, 'DOP1USDT': 6, 'NEAREUR': 3, 'SHIBEUR': 8, 'WIFEUR': 4, 'PEPEEUR': 8, 'AVAXEUR': 4, 'ONDOEUR': 4, 'STETHEUR': 2, 'WLDEUR': 4, 'ENAEUR': 4, 'TONEUR': 4, 'FTMEUR': 4, 'LINKEUR': 3, 'MOCAUSDT': 5, 'PIXFIUSDT': 6, 'UXLINKUSDT': 4, 'A8USDT': 4, 'CLOUDUSDT': 4, 'ZKLUSDT': 5, 'AVAILUSDT': 5}



async def get_rest_api_data(pair, days):
    # Выполняем все асинхронные запросы параллельно
    results = await asyncio.gather(
        get_binance_data(pair=pair, days=days),
        get_bybit_data(pair=pair, days=days),
        get_okx_data(pair=pair, days=days)
    )

    # Присваиваем результаты соответствующим биржам
    aggregate_dict = {
        'binance': results[0],
        'bybit': results[1],
        'okx': results[2]
    }
    return aggregate_dict

####### GET API ФУНКЦИИ #######
async def subscribe_to_bybit(args, stop_event):
    uri = "wss://stream.bybit.com/v5/public/spot"

    async with websockets.connect(uri) as websocket:
        subscribe_message = json.dumps({
            "op": "subscribe",
            "args": args
        })
        await websocket.send(subscribe_message)

        while not stop_event.is_set():
            try:
                response = await websocket.recv()
                data = json.loads(response)
            except:
                return await subscribe_to_bybit(args)
            if stop_event.is_set(): break
            try:
                filtered_data = {'exchange': 'bybit', 'pair': data['topic'].replace('kline.D.', ''), 'volume': data['data'][0]['turnover'], 'price': data['data'][0]['close']}
                pair = filtered_data['pair']
                exchange = filtered_data['exchange']
                updated_data_dict.setdefault(exchange, {})
                updated_data_dict[exchange][pair] = {
                    'volume': filtered_data['volume'],
                    'price': filtered_data['price']
                }
            except Exception as e:
                continue

async def subscribe_to_binance(args, stop_event):
    uri = "wss://stream.binance.com:9443/ws"
    async with websockets.connect(uri) as websocket:
        subscribe_message = json.dumps({
            "method": "SUBSCRIBE",
            "params": args,
            "id": 1
        })
        await websocket.send(subscribe_message)

        while not stop_event.is_set():
            try:
                response = await websocket.recv()
                data = json.loads(response)
            except:
                return await subscribe_to_binance(args)


            try:
                filtered_data = {'exchange': 'binance', 'pair': data['s'], 'volume': data['k']['q'], 'price': data['k']['c']}
                pair = filtered_data['pair']
                exchange = filtered_data['exchange']
                updated_data_dict.setdefault(exchange, {})
                updated_data_dict[exchange][pair] = {
                    'volume': filtered_data['volume'],
                    'price': filtered_data['price']
                }
            except Exception as e:
                continue

async def subscribe_to_okx(args, stop_event):
    uri = "wss://ws.okx.com:8443/ws/v5/business"
    async with websockets.connect(uri) as websocket:
        subscribe_message = json.dumps({
            "op": "subscribe",
            "args": args
        })
        await websocket.send(subscribe_message)

        while not stop_event.is_set():
            try:
                response = await websocket.recv()
                data = json.loads(response)
            except:
                return await subscribe_to_okx(args)


            try:
                filtered_data = {'exchange': 'okx', 'pair': data['arg']['instId'].replace('-', ''), 'volume': data['data'][0][7], 'price': data['data'][0][4]}
                pair = filtered_data['pair']
                exchange = filtered_data['exchange']
                updated_data_dict.setdefault(exchange, {})
                updated_data_dict[exchange][pair] = {
                    'volume': filtered_data['volume'],
                    'price': filtered_data['price']
                }
            except Exception as e:
                continue

async def get_binance_data(pair, days):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async def get_data(session, pair):
        volumes_list = []
        prices_list = []
        try:
            url = f'https://api.binance.com/api/v3/klines?symbol={pair}&interval=1d&limit=1000&startTime={subtract_days_from_now(days)}'
            try:
                response = await asyncio.wait_for(session.get(url, ssl=ssl_context), timeout=15)
                resp = await response.json()
                for i in resp:
                    volumes_list.append(float(i[7]))
                    prices_list.append(float(i[4]))
            except:
                return {'volumes': [0], 'prices': [0]}

            return {'volumes': volumes_list, 'prices': prices_list}
        except asyncio.TimeoutError:
            logger.info(f"Таймаут запроса для {pair}")
            return None
        except Exception as e:
            logger.info(f"Ошибка при получении данных для {pair}: {traceback.format_exc()}")
            return None

    async def data():
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                tasks = []

                task = asyncio.create_task(get_data(session, pair))
                tasks.append(task)

                results = await asyncio.gather(*tasks)
                return results[0]
        except Exception as e:
            logger.info(f"Ошибка при создании сессии: {traceback.format_exc()}")
            return None

    return await data()

async def get_bybit_data(pair, days):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    async def get_data(session, pair):
        volumes_list = []
        prices_list = []
        try:
            url = f'https://api.bybit.com/v5/market/kline?symbol={pair}&interval=D&limit=1000&start={subtract_days_from_now(days)}&category=spot'
            try:
                response = await asyncio.wait_for(session.get(url, ssl=ssl_context), timeout=15)

                resp = await response.json()
                for i in resp['result']['list']:
                    volumes_list.append(float(i[6]))
                    prices_list.append(float(i[4]))
            except:
                return {'volumes': [0], 'prices': [0]}

            return {'volumes': list(reversed(volumes_list)), 'prices': list(reversed(prices_list))}
        except asyncio.TimeoutError:
            logger.info(f"Таймаут запроса для {pair}")
            return None
        except Exception as e:
            logger.info(f"Ошибка при получении данных для {pair}: {traceback.format_exc()}")
            return None

    async def data():
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                tasks = []

                task = asyncio.create_task(get_data(session, pair))
                tasks.append(task)

                results = await asyncio.gather(*tasks)
                return results[0]
        except Exception as e:
            logger.info(f"Ошибка при создании сессии: {traceback.format_exc()}")
            return None

    return await data()

async def get_okx_data(pair, days):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    match = re.search(r'(.*)(USDT|BTC|ETH)$', pair)
    if match:
        pair = f'{match.group(1)}-{match.group(2)}'
    else:
        pair = pair
    async def get_data(session, pair):
        volumes_list = []
        prices_list = []
        try:
            url = f'https://www.okx.com/api/v5/market/candles?instId={pair}&bar=1D'
            try:
                response = await asyncio.wait_for(session.get(url, ssl=ssl_context), timeout=15)

                resp = await response.json()
                for i in resp['data'][:days]:
                    volumes_list.append(float(i[7]))
                    prices_list.append(float(i[4]))
            except:
                return {'volumes': [0], 'prices': [0]}

            return {'volumes': list(reversed(volumes_list)), 'prices': list(reversed(prices_list))}
        except asyncio.TimeoutError:
            logger.info(f"Таймаут запроса для {pair}")
            return None
        except Exception as e:
            logger.info(f"Ошибка при получении данных для {pair}: {traceback.format_exc()}")
            return None

    async def data():
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                tasks = []

                task = asyncio.create_task(get_data(session, pair))
                tasks.append(task)

                results = await asyncio.gather(*tasks)
                return results[0]
        except Exception as e:
            logger.info(f"Ошибка при создании сессии: {traceback.format_exc()}")
            return None

    return await data()

####### GET API ФУНКЦИИ #######

def subtract_days_from_now(n_days):
    now = datetime.now()
    new_time = now - timedelta(days=n_days)
    timestamp = new_time.timestamp()
    timestamp = round(timestamp)*1000

    return timestamp

async def run_subscriptions(pairs_list, stop_event):
    bybit_args, binance_args, okx_args = bybit_args_generator(pairs_list), binance_args_generator(pairs_list), okx_args_generator(pairs_list)
    await asyncio.gather(
        *(coroutine for coroutine in [
            subscribe_to_bybit(bybit_args, stop_event) if bybit_args else None,
            subscribe_to_binance(binance_args, stop_event) if binance_args else None,
            subscribe_to_okx(okx_args, stop_event) if okx_args else None
        ] if coroutine is not None)
    )

def bybit_args_generator(pairs_list):
    args = []
    for pair in pairs_list:
        if pair in supported_pairs['bybit']: args.append(f'kline.D.{pair}')

    return args

def binance_args_generator(pairs_list):
    args = []
    for pair in pairs_list:
        if pair in supported_pairs['binance']: args.append(f'{pair.lower()}@kline_1d')

    return args

def okx_args_generator(pairs_list):
    args = []
    for pair in pairs_list:
        match = re.search(r'(.*)(USDT|BTC|ETH)$', pair)
        if match:
            pair = f'{match.group(1)}-{match.group(2)}'
        else:
            pair = pair
        if pair in supported_pairs['okx']: args.append({"channel": "candle1D", "instId": pair})

    return args
def get_ws_data(pair, volumes, prices, data_dict, ethalon_len, n):
    keys = ['binance', 'bybit', 'okx']
    volumes = [volumes[i] for i in range(len(volumes)) if (i + 1) % n != 0]
    prices = [prices[i] for i in range(len(prices)) if (i + 1) % n != 0]

    for key in keys:
        try:
            volumes.append(float(data_dict[key][pair]['volume']))
            prices.append(float(data_dict[key][pair]['price']))
        except: continue

    return volumes, prices

def unpack_range(data):
    all_volumes = []  # Список для всех объемов
    all_prices = []  # Список для всех цен

    for exchange, values in data.items():
        volumes = values.get('volumes', [])
        prices = values.get('prices', [])

        # Объединяем объемы и цены в один список
        all_volumes.extend(volumes)  # Добавляем объемы
        all_prices.extend(prices)  # Добавляем цены

    return [all_volumes, all_prices]

def calculate_weighted_average_price(data, pair, updated_data, ethalon_len, days):
    if ethalon_len:
        volumes, prices = get_ws_data(pair, data[0], data[1], updated_data, ethalon_len, days)

        total_volume = sum(volumes)
        weighted_sum = sum(volume * price for volume, price in zip(volumes, prices))

        if total_volume == 0:
            return None

        weighted_average_price = weighted_sum / total_volume
    else:
        volumes, prices = data[0], data[1]
        total_volume = sum(volumes)
        weighted_sum = sum(volume * price for volume, price in zip(volumes, prices))

        if total_volume == 0:
            return None

        weighted_average_price = weighted_sum / total_volume

    return weighted_average_price

async def updater_data(pairs_list, final_aggregate_dict, stop_event, queue, days):
    while not stop_event.is_set():
        for pair in pairs_list:
            unpack_range_data = unpack_range(final_aggregate_dict[pair])
            ethalon_len = len(unpack_range_data[0])

            if updated_data_dict:
                try:
                    avg_price = calculate_weighted_average_price(unpack_range_data, pair, updated_data_dict, ethalon_len, days)
                    avg_prices.setdefault(pair, avg_price)
                    try:
                        round_number = precision[pair]
                        weighted_average_price = round(avg_price, round_number)
                        weighted_average_price = f"{weighted_average_price:.{round_number}f}"
                        avg_prices[pair] = weighted_average_price
                    except:
                        avg_prices[pair] = avg_price
                except:
                    continue
        queue.put(avg_prices)

        await asyncio.sleep(1)  # Периодическое обновление данных

async def start_streams(pairs_list, stop_event, queue, days):
    final_aggregate_dict = {}
    for pair in pairs_list:
        final_aggregate_dict[pair] = await get_rest_api_data(pair, days)

    await asyncio.gather(
        run_subscriptions(pairs_list, stop_event),
        updater_data(pairs_list, final_aggregate_dict, stop_event, queue, days)
    )

async def get_avg_price(pair, days):
    final_aggregate_dict = {}
    final_aggregate_dict[pair] = await get_rest_api_data(pair, days)
    unpack_range_data = unpack_range(final_aggregate_dict[pair])
    ethalon_len = None
    avg_price = calculate_weighted_average_price(unpack_range_data, pair, updated_data_dict, ethalon_len, None)
    round_number = precision[pair]

    avg_price = f"{avg_price:.{round_number}f}"
    return avg_price
