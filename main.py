import os.path
from datetime import datetime
import datetime as dt
import json
import bisect
from getpass import getpass
from mysql.connector import connect


def idOrders(SettingsID, rows):
    idList = []
    for index in range(rows):
        if len(idList) == 0:
            idList.append((SettingsID[1] * 1 + SettingsID[0]) % SettingsID[2])
        else:
            idList.append((SettingsID[1] * idList[index - 1] + SettingsID[0]) % SettingsID[2])
    return idList


def History_Order_Create_Date():
    Create_Date = []
    SettingsCreateDate = Init()["SettingsCreateDate"]
    ConfigStartDate = Init()["StartDate"]
    ConfigEndDate = Init()["EndDate"]
    start_date = dt.datetime(ConfigStartDate[0], ConfigStartDate[1], ConfigStartDate[2], ConfigStartDate[3],
                             ConfigStartDate[4], ConfigStartDate[5], ConfigStartDate[6])
    end_date = dt.datetime(ConfigEndDate[0], ConfigEndDate[1], ConfigEndDate[2], ConfigEndDate[3], ConfigEndDate[4],
                           ConfigEndDate[5],
                           ConfigEndDate[6])
    time_start = start_date.timestamp()
    time_end = end_date.timestamp()
    SettingsCreateDate[2] = time_end - time_start
    ChangeDate = idOrders(SettingsCreateDate, Init()["history_rows"])
    for index in range(Init()["history_rows"]):
        result = datetime.fromtimestamp(ChangeDate[index] + time_start)
        Create_Date.append(result)
    return Rows_Without_repeats(Create_Date)


def History_Order_Change_Date():
    tempIndex = 0
    Change_Date_Temp = []
    Change_Date_Result = []
    SettingsChangeDate = Init()["SettingsChangeDate"]
    Time = idOrders(SettingsChangeDate, Init()["history_rows"])
    Create_Date = History_Order_Create_Date()
    CDate = []
    for index in range(Init()["history_rows"]):
        CDate.append(Create_Date[index].timestamp())
    for index in range(Init()["len_without_repeats"]):
        Change_Date_Temp.append(CDate[index] + Time[tempIndex])
        tempIndex = tempIndex + 1
    for index in range(Init()["history_rows"] - 625):
        Change_Date_Temp.append(Change_Date_Temp[index] + Time[tempIndex])
        tempIndex = tempIndex + 1
    for index in range(Init()["history_rows"]):
        result = datetime.fromtimestamp(Change_Date_Temp[index])
        Change_Date_Result.append(result)
    return Change_Date_Result


def boundaries(num, breakpoints, result):
    i = bisect.bisect(breakpoints, num - 1)
    return result[i]


def Instrument():
    Pair = Init()["INSTRUMENT"]
    breakpoints = Init()["BreakpointsInstrument"]
    SettingsInstrument = Init()["SettingsInstrument"]
    IdInstrument = idOrders(SettingsInstrument, Init()["len_without_repeats"])
    for index, item in enumerate(IdInstrument):
        IdInstrument[index] = boundaries(item, breakpoints, Pair)
    return Rows_Without_repeats(IdInstrument)


def History_Order_Note():
    Note = Init()["Records"]
    SettingsNote = Init()["SettingsNote"]
    breakpoints = Init()["BreakpointsNote"]
    IdNote = idOrders(SettingsNote, Init()["len_without_repeats"])
    for index, item in enumerate(IdNote):
        IdNote[index] = boundaries(item, breakpoints, Note)
    return Rows_Without_repeats(IdNote)


def Direction():
    SettingsDirection = Init()["SettingsDirection"]
    IdDirection = idOrders(SettingsDirection, Init()["history_rows"])
    for index, item in enumerate(IdDirection):
        if item > Init()["history_rows"] / 2:
            IdDirection[index] = Init()["SellBuy"][0]
        else:
            IdDirection[index] = Init()["SellBuy"][1]

    return Rows_Without_repeats(IdDirection)


def History_Order_Initial_Volume():
    SettingsIV = Init()["SettingsIV"]
    IV = idOrders(SettingsIV, Init()["len_without_repeats"])
    for index in range(Init()["len_without_repeats"]):
        IV[index] = round(IV[index], 1)
    return Rows_Without_repeats(IV)


def Initial_Price():
    InitialPriceResult = []
    OrderInitialPriceDict = Init()["InitialPrice"]
    SetInstrument = Instrument()
    for index in range(Init()["history_rows"]):
        if SetInstrument[index] in OrderInitialPriceDict:
            InitialPriceResult.append(OrderInitialPriceDict[SetInstrument[index]])
    return InitialPriceResult


def Fill_Price():
    Fill_Price_Result = []
    IP = Initial_Price()
    Direct = Direction()
    for index in range(Init()["history_rows"]):
        if Direct[index] == Init()["SellBuy"][0]:
            Fill_Price_Result.append(IP[index] + (IP[index] * 5) / 100)
        else:
            Fill_Price_Result.append(IP[index] - (IP[index] * 5) / 100)
    return Fill_Price_Result


def Order_State():
    State_Result_Order = []
    BreakpointsNumber = Init()["BreakpointsNumber"]
    BreakpointsState = Init()["BreakpointsState"]
    for index in range(Init()["history_rows"] + 1):
        State_Result_Order.append(boundaries(index, BreakpointsNumber, BreakpointsState))
    return State_Result_Order


def History_Order_Fill_Volume():
    Fill_Volume_Result = []
    IDState = Order_State()
    State = Init()["State"]
    Initial_Volume = History_Order_Initial_Volume()
    for index in range(Init()["history_rows"]):
        if IDState[index] == State[2]:
            Fill_Volume_Result.append(Initial_Volume[index] * 0.01)
        elif IDState[index] == State[3] or IDState[index] == State[5]:
            Fill_Volume_Result.append(Initial_Volume[index])
        else:
            Fill_Volume_Result.append(0)
    for index in range(2500):
        print(Fill_Volume_Result[index])


def Order_Tag():
    Tag_Result = []
    TagDict = Init()["Tag"]
    SetState = Order_State()
    for index in range(Init()["history_rows"]):
        if SetState[index] in TagDict:
            Tag_Result.append(TagDict[SetState[index]])
    return Tag_Result


def Additional_table():
    SettingsID = Init()["SettingsID"]
    tempID = idOrders(SettingsID, Init()["history_rows"])
    ID_without_repeats = []
    for index in tempID:
        if index not in ID_without_repeats:
            ID_without_repeats.append(index)
    return ID_without_repeats


def Init():
    main_path = os.path.split(__file__)[0]
    try:
        file = open(main_path + '/config.json', 'r')
        config_data = json.load(file)
    except NameError:
        exit(-1)
    finally:
        file.close()
    return config_data


def Rows_Without_repeats(List):
    List_Result = []
    SettingsID = Init()["SettingsID"]
    SetID = idOrders(SettingsID, Init()["history_rows"])
    Order_Dict = dict()
    for index in range(len(Additional_table())):
        Order_Dict[Additional_table()[index]] = List[index]
    for index in range(Init()["history_rows"]):
        if SetID[index] in Order_Dict:
            List_Result.append(Order_Dict[SetID[index]])
    # for index in range(2500):
    #   print(List_Result[index])
    return List_Result


def Workflow():
    SettingsID = Init()["SettingsID"]
    # History_Order_Change_Date()
    # History_Order_Fill_Volume()
    # Direction()
    # idOrders(SettingsID,2500)
    # History_Order_Initial_Volume()
    # History_Order_Note()
    # Instrument()
    # History_Order_Create_Date()
    # Initial_Price()
    # Fill_Price()
    # Order_State()
    # Order_Tag()


if __name__ == '__main__':
    Workflow()
