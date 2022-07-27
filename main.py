import os.path
from datetime import datetime
import datetime as dt
import json
import bisect
import pymysql
import logging
from constants import *


def connect_db():
    try:
        connection = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['db_name'],
        )
        logging.info(SUCCESS_CONNECTED)
    except Exception as ex:
        logging.error(FAILED_CONNECTION + ex)
        exit(-1)
    return connection


def insert_date(insert_query):
    connection = connect_db()
    try:
        connection.query(insert_query)
        connection.commit()
        logging.info(SUCCESS_CONNECTED)
    except ConnectionError:
        logging.error(FAILED_CONNECTION)
        exit(-1)
    finally:
        connection.close()


def generate_sql_query(ID, Create_Date, Change_Date, State, Direction, Instrument, Initial_Volume, Fill_Volume,
                       Initial_Price, Fill_Price, Note, Tag):
    try:
        sql_query = "INSERT INTO `history_order`(`id`, `creation date`, `change date`, `state`, `instrument`, `direction`, `initial volume`, `fill volume`, `initial price`, `fill price`, `note`, `tag`) VALUES"
        for index in range(config['history_rows']):
            sql_query += f"({ID[index]}," \
                         f"'{Create_Date[index]}'," \
                         f"'{Change_Date[index]}'," \
                         f"'{State[index]}'," \
                         f"'{Direction[index]}'," \
                         f"'{Instrument[index]}'," \
                         f"{Initial_Volume[index]}," \
                         f"{Fill_Volume[index]}," \
                         f"{Initial_Price[index]}," \
                         f"{Fill_Price[index]}," \
                         f"'{Note[index]}'," \
                         f"'{Tag[index]}'),"
        return sql_query[:-1] + ";"
    except Exception:
        logging.error(ERROR_CREATE_SQL_QUERY)
        exit(-1)


def history_order_id(SettingsID, rows):
    idList = []
    try:
        for index in range(rows):
            if len(idList) == 0:
                idList.append((SettingsID["step"] * 1 + SettingsID["alfa"]) % SettingsID["modul"])
            else:
                idList.append((SettingsID["step"] * idList[index - 1] + SettingsID["alfa"]) % SettingsID["modul"])
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_id")
        return idList
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_id")
        exit(-1)


def history_order_create_date():
    SettingsCreateDate = difference_date()
    Create_Date = []
    ChangeDate = history_order_id(SettingsCreateDate, config['history_rows'])
    try:
        for index in range(config['history_rows']):
            result = datetime.fromtimestamp(ChangeDate[index] + start_date.timestamp())
            Create_Date.append(result)
        Create_Date_Timezone = timezone(Create_Date)
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_create_date")
        return rows_without_repeats(Create_Date_Timezone)
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_create_date")
        exit(-1)


def difference_date():
    SettingsCreateDate = config['SettingsCreateDate']
    ConfigStartDate = config['StartDate']
    ConfigEndDate = config['EndDate']
    global start_date
    start_date = dt.datetime(ConfigStartDate["year"], ConfigStartDate["month"], ConfigStartDate["day"],
                             ConfigStartDate["hour"],
                             ConfigStartDate["minute"], ConfigStartDate["second"], ConfigStartDate["microsecond"])
    end_date = dt.datetime(ConfigEndDate["year"], ConfigEndDate["month"], ConfigEndDate["day"],
                           ConfigEndDate["hour"], ConfigEndDate["minute"],
                           ConfigEndDate["second"],
                           ConfigEndDate["microsecond"])
    SettingsCreateDate["modul"] = end_date.timestamp() - start_date.timestamp()
    logging.info(DIFFERENCE_DATE_INFO)
    return SettingsCreateDate


def history_order_change_date():
    tempIndex = 0
    Change_Date_Temp = []
    Change_Date_Result = []
    SettingsChangeDate = config['SettingsChangeDate']
    Time = history_order_id(SettingsChangeDate, config['history_rows'])
    Create_Date = history_order_create_date()
    CDate = []
    try:
        for index in range(config['history_rows']):
            CDate.append(Create_Date[index].timestamp())
        for index in range(config['len_without_repeats']):
            Change_Date_Temp.append(CDate[index] + Time[tempIndex])
            tempIndex = tempIndex + 1
        for index in range(config['history_rows'] - config['len_without_repeats']):
            Change_Date_Temp.append(Change_Date_Temp[index] + Time[tempIndex])
            tempIndex = tempIndex + 1
        for index in range(config['history_rows']):
            result = datetime.fromtimestamp(Change_Date_Temp[index])
            Change_Date_Result.append(result)
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_change_date")
        return timezone(Change_Date_Result)
    except ValueError:
        logging.error(ERROR_GENERATE_DATA + " history_order_change_date")


def boundaries(num, breakpoints, result):
    i = bisect.bisect(breakpoints, num - 1)
    return result[i]


def history_order_instrument():
    Pair = config['INSTRUMENT']
    breakpoints = config['BreakpointsInstrument']
    SettingsInstrument = config['SettingsInstrument']
    IdInstrument = history_order_id(SettingsInstrument, config['len_without_repeats'])
    try:
        for index, item in enumerate(IdInstrument):
            IdInstrument[index] = boundaries(item, breakpoints, Pair)
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_instrument")
        return rows_without_repeats(IdInstrument)
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_instrument")
        exit(-1)


def history_order_note():
    Note = config['Records']
    SettingsNote = config['SettingsNote']
    breakpoints = config['BreakpointsNote']
    IdNote = history_order_id(SettingsNote, config['len_without_repeats'])
    try:
        for index, item in enumerate(IdNote):
            IdNote[index] = boundaries(item, breakpoints, Note)
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_note")
        return rows_without_repeats(IdNote)
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_note")
        exit(-1)


def history_order_direction():
    SettingsDirection = config['SettingsDirection']
    IdDirection = history_order_id(SettingsDirection, config['history_rows'])
    try:
        for index, item in enumerate(IdDirection):
            if item > config["history_rows"] / CONST_DIRECTION_HALF:
                IdDirection[index] = config['SellBuy'][CONST_DIRECTION_SELL]
            else:
                IdDirection[index] = config['SellBuy'][CONST_DIRECTION_BUY]
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_direction")
        return rows_without_repeats(IdDirection)
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_direction")
        exit(-1)


def history_order_initial_volume():
    SettingsIV = config['SettingsIV']
    IV = history_order_id(SettingsIV, config['len_without_repeats'])
    try:
        for index in range(config['len_without_repeats']):
            IV[index] = round(IV[index], CONST_INITIAL_VOLUME_NUMBER_ROUNDING)
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_initial_volume")
        return rows_without_repeats(IV)
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_initial_volume")


def history_order_initial_price():
    InitialPriceResult = []
    OrderInitialPriceDict = config['InitialPrice']
    SetInstrument = history_order_instrument()
    try:
        for index in range(config['history_rows']):
            if SetInstrument[index] in OrderInitialPriceDict:
                InitialPriceResult.append(OrderInitialPriceDict[SetInstrument[index]])
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_initial_price")
        return InitialPriceResult
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_initial_price")
        exit(-1)


def history_order_fill_price():
    Fill_Price_Result = []
    IP = history_order_initial_price()
    Direct = history_order_direction()
    try:
        for index in range(config['history_rows']):
            if Direct[index] == config['SellBuy'][CONST_FILL_PRICE_ZERO]:
                Fill_Price_Result.append(IP[index] + (IP[index] * CONST_FILL_PRICE_CHANGE))
            else:
                Fill_Price_Result.append(IP[index] - (IP[index] * CONST_FILL_PRICE_CHANGE))
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_fill_price")
        return Fill_Price_Result
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_fill_price")
        exit(-1)


def history_order_state():
    State_Result_Order = []
    BreakpointsNumber = config['BreakpointsNumber']
    BreakpointsState = config['BreakpointsState']
    try:
        for index in range(config['history_rows'] + 1):
            State_Result_Order.append(boundaries(index, BreakpointsNumber, BreakpointsState))
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_state")
        return State_Result_Order
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_state")
        exit(-1)


def history_order_fill_volume():
    Fill_Volume_Result = []
    IDState = history_order_state()
    State = config['State']
    Initial_Volume = history_order_initial_volume()
    try:
        for index in range(config['history_rows']):
            if IDState[index] == State['PARTIAL_FILL']:
                Temp_Fill_Volume = Initial_Volume[index] + Initial_Volume[index] * CONST_FILL_VOLUME_CHANGE
                Fill_Volume_Result.append(Temp_Fill_Volume)
            elif IDState[index] == State['FILL'] or IDState[index] == State['DONE']:
                Fill_Volume_Result.append(Initial_Volume[index])
            else:
                Fill_Volume_Result.append(CONST_FILL_VOLUME_ZERO)
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_fill_volume")
        return Fill_Volume_Result
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_fill_volume")
        exit(-1)


def history_order_tag():
    Tag_Result = []
    TagDict = config['Tag']
    SetState = history_order_state()
    try:
        for index in range(config['history_rows']):
            if SetState[index] in TagDict:
                Tag_Result.append(TagDict[SetState[index]])
        logging.info(SUCCESSFULLY_GENERATE_DATA + " history_order_tag")
        return Tag_Result
    except Exception:
        logging.error(ERROR_GENERATE_DATA + " history_order_tag")
        exit(-1)


def additional_table():
    SettingsID = config['SettingsID']
    tempID = history_order_id(SettingsID, config['history_rows'])
    ID_without_repeats = []
    try:
        for index in tempID:
            if index not in ID_without_repeats:
                ID_without_repeats.append(index)
        logging.info(ADDITIONAL_TABLE_SUCCESS)
        return ID_without_repeats
    except Exception:
        logging.error(ADDITIONAL_TABLE_ERROR)
        exit(-1)


def setup():
    global config
    config = init_config()
    try:
        logging.basicConfig(filename=config["LOG_PATH"], level=config["LOG_LEVEL"], format=config["LOG_FORMAT"])
        logging.info(LOGING_SETUP)
    except KeyError or FileNotFoundError:
        exit(-1)


def init_config():
    main_path = os.path.split(__file__)[0]
    try:
        file = open(main_path + '/config.json', 'r')
        config_data = json.load(file)
        for key in config_data.keys():
            if not config_data[key]:
                raise ValueError("Config Error in " + key)
                exit(-1)
        return config_data
    except NameError:
        exit(-1)
    finally:
        file.close()


def rows_without_repeats(List):
    List_Result = []
    SettingsID = config['SettingsID']
    SetID = history_order_id(SettingsID, config['history_rows'])
    Order_Dict = dict()
    try:
        for index in range(len(additional_table())):
            Order_Dict[additional_table()[index]] = List[index]
        for index in range(config['history_rows']):
            if SetID[index] in Order_Dict:
                List_Result.append(Order_Dict[SetID[index]])
        logging.info(SUCCESS_ROWS_WITHOUT_REPEAT)
        return List_Result
    except Exception:
        logging.error(ERROR_ROWS_WITHOUT_REPEAT)
        exit(-1)


def workflow():
    SettingsID = config['SettingsID']
    ID = history_order_id(SettingsID, config['history_rows'])
    Create_Date = history_order_create_date()
    Change_Date = history_order_change_date()
    State = history_order_state()
    Direction = history_order_direction()
    Instrument = history_order_instrument()
    Initial_Volume = history_order_initial_volume()
    Fill_Volume = history_order_fill_volume()
    Initial_Price = history_order_initial_price()
    Fill_Price = history_order_fill_price()
    Note = history_order_note()
    Tag = history_order_tag()
    logging.info(SUCCESSFULLY_GENERATE_DATA)
    insert_date(
        generate_sql_query(ID, Create_Date, Change_Date, State, Direction, Instrument, Initial_Volume, Fill_Volume,
                           Initial_Price, Fill_Price, Note, Tag))


def timezone(ListDate):
    Result_Date = []
    try:
        for index in range(len(ListDate)):
            if ListDate[index].hour >= config["StartLimit"] or ListDate[index].hour < config["EndLimit"]:
                temp = ListDate[index].timestamp()
                temp += config["TimestampLimit"]
                result = datetime.fromtimestamp(temp)
                Result_Date.append(result)
            else:
                Result_Date.append(ListDate[index])
        logging.info(TIME_LIMIT)
        return Result_Date
    except Exception:
        logging.error(ERROR_TIME_LIMIT)
        exit(-1)


if __name__ == '__main__':
    init_config()
    setup()
    workflow()
