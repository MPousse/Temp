import os
import random
import re
import pandas as pd
from io import BytesIO
from lxml import etree
from enum import Enum

CONFIG_FILE_FOLDER = './InputFiles'

NAMESPACES = {'t':"http://www.ima.it/hmi/info-model/tags",
                'da':"http://www.ima.it/hmi/info-model/Automation",
                '':"http://www.ima.it/hmi/info-model"}

class PlcType(Enum):
    BECKHOFF = 'Beckhoff'
    SIEMENS = 'Siemens'
    OPCUA = 'OpcUa'
    ROCKWELL = 'Rockwell'

PLCTAG_DATAFRAME : pd.DataFrame = pd.read_csv('./ConfigFIles/PlcTags.csv', sep=';', header=0,index_col=0)

class PlcConfig:
    def __init__(self, plcType : PlcType, address : str) -> None:
        self.plcType = plcType
        self.address = address

    def get_tag(self, key : str, dfInformationModelRow : pd.Series, wphNumber : int = None, nestNumber : int = None) -> tuple[str,str]:
        
        tagData  = PLCTAG_DATAFRAME.loc[key]
        stringFormater : str = tagData[f'{self.plcType.value} Format']
        return tagData[f'{self.plcType.value} Type'] , stringFormater.format(
            Machine_Number = dfInformationModelRow['Machine'],
            Station_Number = dfInformationModelRow['Station'],
            Station_Name = dfInformationModelRow['StationName'],
            Actuator_Number = dfInformationModelRow['Actuator'],
            Actuator_Name = dfInformationModelRow['ActuatorName'],
            Wph_Number = wphNumber,
            Nest_Number = nestNumber
            )


class OpcuaConfig(PlcConfig):
    
    def __init__(self,  address: str,defaultNamespaceUri, port : int = 4940) -> None:
        super().__init__(PlcType.OPCUA, address)
        self.defaultNamespaceUri = defaultNamespaceUri
        self.port = port

class BeckhoffConfig(PlcConfig):
    
    def __init__(self, address: str, remoteAmsNetId: str = None, localAmsNetId: str = None) -> None:
        super().__init__(PlcType.BECKHOFF, address)
        self.remoteAmsNetId = remoteAmsNetId
        self.localAmsNetId = localAmsNetId

class RockwellConfig(PlcConfig):
    """172.16.228.92/0:1
    """
    def __init__(self, address: str) -> None:
        super().__init__(PlcType.ROCKWELL, address)

PLC_CONFIG : dict[str, BeckhoffConfig | OpcuaConfig]= {
    # 'PLC1' : BeckhoffConfig(rootAddress="MAIN_PRG", address="172.16.224.79", 
    #                 remoteAmsNetId="172.16.224.79.1.1", localAmsNetId="172.27.192.1.1.1"),
    # 'PLC1' : OpcuaConfig(rootAddress="DefaultMachine", address="127.0.0.1", defaultNamespaceUri="http://0.0.0.0"),
    # 'PLC1' : OpcuaConfig(address="172.16.224.68", defaultNamespaceUri="http://0.0.0.0"),
    # 
    'PLC1' : OpcuaConfig(address="172.16.224.80", defaultNamespaceUri="urn:BeckhoffAutomation:Ua:PLC1"),
    'PLC2' : OpcuaConfig(address="172.16.224.80", defaultNamespaceUri="urn:BeckhoffAutomation:Ua:PLC2"),
}

class Primitive:

    def __init__(self, name:str, datatype : str, plcTag : str = None) -> None:
        self.Name = name
        self.DataType = datatype
        self.PlcTag = plcTag

Actuator_CONFIG = {
    'Act_Bin' : [
                    Primitive(name="Sts_Inp", datatype="Boolean", plcTag="Sts_Inp"),
                    Primitive(name="Cmd_Out", datatype="Boolean", plcTag="Cmd_Out"),
                    Primitive(name="Cycle_Time", datatype="Int32", plcTag="Rep_dT"),
                    Primitive(name="Sts_NoAlm", datatype="Boolean", plcTag="Sts_NoAlm"),                    
                    Primitive(name="Sts_Auth", datatype="Boolean"),
                    Primitive(name="Sts_State", datatype="Boolean"),
                    Primitive(name="Sts_Mode", datatype="Boolean"),
                    Primitive(name="Sts_inter", datatype="Boolean", plcTag="Cfg_NoInterlock"),                    
                ],
    'Act_Bix' : [],
    
}


def getPath(element : etree.Element)->str:
    if"AutomationDevice" in element.tag:
        return element.attrib["name"]
    else:
        parent = element.getparent()
        return f"{parent.attrib['name']}"

def findRange(df: pd.DataFrame,column: str, keyWord: any):
    try:
        iStart = df[df[column] == keyWord].index.values[0]
    except:        
        return None,None
    try:
        iEnd = df[df[column] == f'{keyWord} End'].index.values[0]
    except:        
        return None,None
    return iStart,iEnd

def parametersToDataFrame(dfParameters : pd.DataFrame, tab : str, plcName, buf : BytesIO) -> pd.DataFrame:

    df_Raw = pd.read_excel(buf, sheet_name=tab, header=None)
    df_Camsparam = None
    df_Params = None

    # start, end = findRange(df=df_Raw,column=0,keyWord="Cams")

    # if not start == None:
    #     df_Camsparam =  df_Raw[start: end]
    #     df_Camsparam.columns = df_Camsparam.iloc[0]
    #     df_Camsparam=df_Camsparam[1:]
    #     df_Camsparam.dropna(how='all', inplace=True)

    start, end = findRange(df=df_Raw,column=0,keyWord="Pars")
    
    if not start == None:
        df_Params =  df_Raw[start: end]
        df_Params.columns = df_Params.iloc[0]
        df_Params=df_Params[1:]
        df_Params.dropna(how='all', inplace=True)

    if df_Params is not None and len(df_Params) > 0:
    
        df_Raw = df_Params

        dfParameters : pd.DataFrame = pd.DataFrame(columns=['Plc', 'Machine', 'Station', 'Minimum', 'Value', 'Maximum', 'ConfigAnte'])

        machineRe = re.search(r"^_([0-9]{2})$", tab )
    
        if machineRe is not None:
            df_Raw['Machine'] = machineRe.group(1)

        stationRe = re.search(r"^_([0-9]{2})_([0-9]{2})$", tab )

        if stationRe is not None:
            df_Raw['Machine'] = machineRe.group(1)
            df_Raw['Station'] = stationRe.group(2)    

        df_Raw['AutomationDevice'] = plcName        

        df_Raw.rename(columns={'Alarm name':'AlarmName', 'Alm' : 'AlarmInput', 'Ack' : 'AlarmAcknowledge', 
                        'en-US' : 'AlarmMessage', 'Class' : 'AlarmClass' },inplace=True)
        
        
        
        return df_Raw[dfParameters.columns]

def alarmsToDataFrame(dfAlarms : pd.DataFrame,tab : str, plcName, buf : BytesIO) -> pd.DataFrame:

    if not tab == '_Alarms':
        return
    
    df_Raw = pd.read_excel(buf, sheet_name=tab)

    df_Raw.rename(columns={'Alarm name':'AlarmName', 'Alm' : 'AlarmInput', 'Ack' : 'AlarmAcknowledge', 
                    'en-US' : 'AlarmMessage' },inplace=True)
    
    df_Raw['AutomationDevice'] = plcName
    
    return df_Raw[dfAlarms.columns]

def informationModelToDataFrame(dfInformationModel : pd.DataFrame, tab : str, plcName, buf : BytesIO) -> pd.DataFrame:
    # Handle InformationModel
    machineRe = re.search(r"^_([0-9]{2})_([0-9]{2})$", tab )
    
    if machineRe is None:
        #print (f"No parsing (no machine in sheet name) {f}, tab {tab}")
        return
    
    machineStr = machineRe.group(1)
    stationNumberStr = machineRe.group(2)
    df_Raw = pd.read_excel(buf, sheet_name=tab, header=None)
    
    stationNameStr = df_Raw[df_Raw[0]=='NameL1'][1].values[0]

    start, end = findRange(df=df_Raw,column=0,keyWord="Actuator")

    df_actuator =  df_Raw[start: end]
    df_actuator.columns = df_actuator.iloc[0]
    df_actuator=df_actuator[1:]
    #'Plc', 'Machine', 'Station', 'StationName', 'Actuator','ActuatorType', 'ActuatorName'
    df_actuator['AutomationDevice'] = plcName
    df_actuator['Machine'] = machineStr
    df_actuator['Station'] = stationNumberStr
    df_actuator['StationName'] = stationNameStr     
    df_actuator['Actuator'] = df_actuator['Actuator'].str.replace('_','')

    df_actuator.rename(columns={'DataType':'ActuatorType', 'Name' : 'ActuatorName' },inplace=True)
    df_actuator = df_actuator[dfInformationModel.columns]

    return df_actuator

def excelConfigFilesToDataFrames(folderpath : str) -> (pd.DataFrame,pd.DataFrame,pd.DataFrame):

    dfInformationModel : pd.DataFrame = pd.DataFrame(columns=['AutomationDevice', 'Machine', 'Station', 'StationName', 'Actuator','ActuatorType', 'ActuatorName'])
    dfAlarms : pd.DataFrame = pd.DataFrame(columns=['AutomationDevice', 'AlarmName', 'AlarmInput', 'AlarmAcknowledge', 'AlarmMessage'])
    dfParameters : pd.DataFrame = pd.DataFrame(columns=['AutomationDevice', 'Machine', 'Station', 'Actuator', 'ParameterName', 'Minimum', 'Value', 'Maximum'])
    
    # iterate over fil e in folder
    for file in os.listdir(folderpath):

        # only handle .xlsm file and not currently opened one
        if not file.endswith(".xlsm") or file.startswith("~$"):
            continue
        
        with open(os.path.join(folderpath,file), "rb") as fh:
            buf = BytesIO(fh.read())
            tabs = pd.ExcelFile(buf).sheet_names

            plcName = file.split('_')[1].replace(".xlsm","")
            
            for tab in tabs:
                dfInformationModel = pd.concat((dfInformationModel, informationModelToDataFrame(dfInformationModel=dfInformationModel, tab=tab, plcName=plcName, buf=buf)), ignore_index=True)
                dfAlarms = pd.concat((dfAlarms, alarmsToDataFrame(dfAlarms=dfAlarms, tab=tab,plcName=plcName, buf=buf)), ignore_index=True)
                #dfParameters = pd.concat((dfParameters, parametersToDataFrame(dfParameters=dfParameters, tab=tab,plcName=plcName, buf=buf)))
            

    return dfInformationModel, dfParameters, dfAlarms


def makePrimitive(name : str, dataType : str, plcTag: str = None, canSet : str = None):
    """
        <Primitive name="Data" plcTag="//Application.ModBus_Array.MDD_a_bArrB0000[1]" canSet="{path:{GeneralData}/PackML_ProductionMode_Enable/Data}" isVisible="true" dataType="Boolean" behaviour="Switch" />
    """
    primitive :etree.Element = etree.Element("Primitive")
    primitive.attrib["name"] = name
    primitive.attrib["dataType"] = dataType
    if plcTag:
        primitive.attrib["plcTag"] = plcTag
    if canSet:
        primitive.attrib["canSet"] = canSet        

    return primitive

def makeGenericOutbound(name : str, dataType : str, plcTag: str = None, canSet : str = None) -> etree.Element :
    """<da:GenericOutbound name="PushButtonCMD_Automatic_Mode" hmiId="2100" historian="true" tags="Type/GeneralData,Page/HomePage">
        <Primitive name="Data" plcTag="//Application.ModBus_Array.MDD_a_bArrB0000[1]" canSet="{path:{GeneralData}/PackML_ProductionMode_Enable/Data}" isVisible="true" dataType="Boolean" behaviour="Switch" />
    </da:GenericOutbound>"""
    
    daGenericOutbound :etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}GenericOutbound")
    daGenericOutbound.attrib["name"] = name
    daGenericOutbound.attrib['scopeId'] = name    
    daGenericOutbound.attrib['hmiId'] = f'{random.randint(10000,99999)}'

    daGenericOutbound.append(makePrimitive(name="Data",dataType=dataType,plcTag=plcTag, canSet = canSet))
    return daGenericOutbound

def makeGenericOutboundFromPrimitive(primitive : Primitive, GenericOutboundPLcTag : str = None, tagType : str = None) -> etree.Element :
    if primitive.PlcTag:
        return makeGenericOutbound(name=primitive.Name, dataType= tagType if tagType else primitive.DataType, 
                plcTag=f"{GenericOutboundPLcTag}.{primitive.PlcTag}" if GenericOutboundPLcTag else primitive.PlcTag)
    else:
        return makeGenericOutbound(name=primitive.Name, dataType=primitive.DataType)
    

def makeActuator(daStationElement : etree.Element, dfinformationModel : pd.DataFrame):
    
    #ignore actuator type alias
    if dfinformationModel['ActuatorType'].values[0] not in Actuator_CONFIG:
        print(f"Error : actuator type unsuported : {dfinformationModel['ActuatorType'].values[0]} ")
        return

    daActuator : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}{dfinformationModel['ActuatorType'].values[0]}")
    daStationElement.append(daActuator)
    daActuator.attrib['name'] =f"ACT{dfinformationModel['Actuator'].values[0]}" 
    daActuator.attrib['scopeId'] = f"_{dfinformationModel['Machine'].values[0]}_{dfinformationModel['Station'].values[0]}_{dfinformationModel['Actuator'].values[0]}_{dfinformationModel['ActuatorName'].values[0]}" 
    daActuator.attrib['hmiId'] = f'{random.randint(10000,99999)}'

    primitiveList : list[Primitive] = Actuator_CONFIG[dfinformationModel['ActuatorType'].values[0]]
    
    plconfig = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]]

    for primitive in primitiveList:
        _, tagAddress = plconfig.get_tag(key='actuator_node',dfInformationModelRow=dfinformationModel.iloc[0])
        if primitive.Name == 'Cmd_Out':
            daActuator.append(makeGenericOutbound(name=primitive.Name,dataType=primitive.DataType,
                plcTag=f"//{tagAddress}.{primitive.PlcTag}",
                canSet="{path:{Rights}/ManualActionEnable}"))
        else:
            daActuator.append(makeGenericOutbound(name=primitive.Name,dataType=primitive.DataType,
                    plcTag=f"//{tagAddress}.{primitive.PlcTag}"))

def makeStation(daMachineElement : etree.Element, dfinformationModel : pd.DataFrame, dfParameters: pd.DataFrame):
    daStation : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}Station")
    daMachineElement.append(daStation)
    daStation.attrib['name'] = f"ST{dfinformationModel['Station'].values[0]}"
    daStation.attrib['scopeId'] = f"ST{dfinformationModel['Station'].values[0]}"
    daStation.attrib['hmiId'] = f'{random.randint(10000,99999)}'
    
    plconfig = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]]
    tagType, tagAddress = plconfig.get_tag(key='Station_PackMl_State',dfInformationModelRow=dfinformationModel.iloc[0])
    daStation.append(makeGenericOutbound(name='PackMlState',dataType=tagType,
                    plcTag=f"//{tagAddress}"))
    
        
    for i in ['Sts_Idle', 'Sts_NoAlm']:
        tagType, tagAddress = plconfig.get_tag(key=f'station_node',dfInformationModelRow=dfinformationModel.iloc[0])
        daStation.append(makeGenericOutbound(name=i, dataType='Boolean', 
                                plcTag=f"//{tagAddress}.{i}"))
        
    daStation.append(makeParameters(parametersName=f"Parameters",
                                dfParameters=dfParameters,
                                dfStation=dfinformationModel))
    
    daStation.append(makeshiftRegister(shiftRegisterName=f"ShiftRegister{daStation.attrib['name']}",
                                dfinformationModel=dfinformationModel,
                                wphCount=2,
                                nestCount=4,
                                dfStation=dfinformationModel))

    for groupName, groupeDataFrame in dfinformationModel.groupby(["Actuator",'ActuatorName']):
        makeActuator(daStationElement=daStation, dfinformationModel=groupeDataFrame)

def makeParameters(parametersName : str, dfParameters : pd.DataFrame,  dfStation: pd.DataFrame = None) -> etree.Element:
    """
        <Folder name="Parameters">
            <da:GenericOutbound name="WaitingTime" scopeId="M01" hmiId="34294" tags="Type/Parameter">
                <Primitive name="Data" dataType="Int32" canSet="true" isVisible="true" plcTag="//WaitingTime" min="//WaitingTimeMin" max="//WaitingTimeMax"/>
            </da:GenericOutbound>
        </Folder>		
    """
    plconfig : PlcConfig = PLC_CONFIG[dfStation['AutomationDevice'].values[0]]
    daParameters : etree.Element = etree.Element(f"Folder")
    daParameters.attrib['name'] = parametersName
    if dfParameters is not None:
        for primitive in dfParameters['ParameterName']:
            """<da:GenericOutbound  name="WaitingTime" scopeId="M01" hmiId="34294" tags="Type/Parameter"""
            tagType, tagAddress = plconfig.get_tag(key=f'primitive.Name',dfInformationModelRow=dfParameters.iloc[0])
            daParam : makeGenericOutbound(name=primitive.Name, dataType= tagType if tagType else primitive.DataType, 
                    plcTag=tagAddress)
            daParam.attrib['tags'] = f"Type/Parameter"
            daParameters.append(daParam)
    return daParameters

    
def makeshiftRegister(shiftRegisterName : str, dfinformationModel : pd.DataFrame, wphCount : int,  nestCount : int, dfStation: pd.DataFrame = None) -> etree.Element:
    """
        <da:ShiftRegister name="Loop_ShiftRegister_001" scopeId="Loop_ShiftRegister_001" hmiId="965247" tags="Type/ShiftRegister">
            <Primitive name="StationId" dataType="Int64" plcTag="////M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Sts_MoverID" hmiId="9651447"/>
            <da:Wph name="1" scopeId="Wph1" hmiId="965147">
                <Primitive name="WphId" dataType="Int32" plcTag="//M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Sts_MoverID" hmiId="965247"/>
                <da:Nest name="1" hmiId="965447">
                    <Primitive name="Sts_Enable" dataType="Boolean" plcTag="//M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Nest[1].Sts_Enable" hmiId="965248" />
                    <Primitive name="Sts_Bad" dataType="Boolean" plcTag="//M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Nest[1].Sts_Bad" hmiId="9652447"/>
                    <Primitive name="Sts_Full" dataType="Boolean" plcTag="//M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Nest[1].Sts_Full" hmiId="9655247"/>
                    <Primitive name="Sts_Good" dataType="Boolean" plcTag="//M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Nest[1].Sts_Good" hmiId="9658547"/>
                    <Primitive name="RejectCode" dataType="Int32" plcTag="//M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Nest[1].Sts_SpecFailCode" hmiId="96524657"/>
                    <Primitive name="StationReject" dataType="Int32" plcTag="//M{dfinformationModel['Machine'].values[0]}.GVLWPHsLoop01[0].Nest[1].Sts_StationFailID" hmiId="96455247"/>
                </da:Nest>
            </da:Wph>
        </da:ShiftRegister>
    """
    plconfig : PlcConfig = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]]

    daShiftRegister : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}ShiftRegister")

    daShiftRegister.attrib['name'] = shiftRegisterName
    daShiftRegister.attrib['scopeId'] = shiftRegisterName
    daShiftRegister.attrib['hmiId'] = f'{random.randint(10000,99999)}'
    daShiftRegister.attrib['tags'] = "Type/ShiftRegister"
    
    if dfStation is not None:
        tagType, tagAddress = plconfig.get_tag(key='shift_register_Station_StationID',dfInformationModelRow=dfinformationModel.iloc[0])
        daShiftRegister.append(makePrimitive(name="StationId", dataType=tagType, 
                plcTag=f"//{tagAddress}"))

    for i in range(1,wphCount+1):
        """<da:Wph name="1" scopeId="Wph1" hmiId="965147">"""
        daWph : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}Wph")
        daShiftRegister.append(daWph)
        daWph.attrib['name'] = f"WPH_{i}"
        daWph.attrib['scopeId'] = f"WPH_{i}"
        daWph.attrib['hmiId'] = f'{random.randint(10000,99999)}'

        if dfStation is not None:            
            tagType, tagAddress = plconfig.get_tag(key='shift_register_Station_WPHID',dfInformationModelRow=dfinformationModel.iloc[0], wphNumber=i)
        else:            
            tagType, tagAddress = plconfig.get_tag(key='shift_register_Loop_WPHID',dfInformationModelRow=dfinformationModel.iloc[0], wphNumber=i)
        
        daWph.append(makePrimitive(name="WphId", dataType=tagType,
                                plcTag=f"//{tagAddress}"))

        for j in range(1,nestCount+1):
            """<da:Nest name="4" hmiId="965447">"""
            daNest : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}Nest")
            daWph.append(daNest)
            daNest.attrib['name'] = f"{j}"
            daNest.attrib['hmiId'] = f'{random.randint(10000,99999)}'

            if dfStation is not None:  
                prefix = 'shift_register_Station_Nest_'                    
            else:
                prefix = 'shift_register_Loop_Nest_'

            for nestTag in ['Sts_Bad','Sts_Full','Sts_Good','Sts_Enable','RejectCode', 'StationReject']:
                tagType, tagAddress = plconfig.get_tag(key=f'{prefix}{nestTag}',dfInformationModelRow=dfinformationModel.iloc[0], wphNumber=i, nestNumber=j)
                daNest.append(makePrimitive(name=nestTag, dataType=tagType, 
                        plcTag=f"//{tagAddress}"))

    return daShiftRegister

def makeMachine(daAutomationDeviceElement : etree.Element, dfinformationModel : pd.DataFrame, dfParameters : pd.DataFrame):
    """
    <da:Machine name="Machine01" hmiId="29245" tags="Type/MachineState">        
        <da:GenericOutbound name="PackMlState" hmiId="59608">
                    <Primitive name="Data" dataType="Int32"/>
                </da:GenericOutbound>
                <da:GenericOutbound name="PackMlMode" hmiId="59608">
                    <Primitive name="Data" dataType="Int32"/>
                </da:GenericOutbound>
    </da:Machine>
    """
    daMachine : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}Machine")
    daAutomationDeviceElement.append(daMachine)

    #daMachine.attrib['name'] = f"{getPath(daMachine)}.M{dfinformationModel['Machine'].values[0]}"
    daMachine.attrib['name'] = f"M{dfinformationModel['Machine'].values[0]}"
    daMachine.attrib['scopeId'] = f"M{dfinformationModel['Machine'].values[0]}"    
    daMachine.attrib['hmiId'] = f'{random.randint(10000,99999)}'
    plconfig = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]]
    
    tagType, tagAddress = plconfig.get_tag(key=f'Machine_PackMl_State',dfInformationModelRow=dfinformationModel.iloc[0])

    daMachine.append(makeGenericOutbound(name='PackMlState', dataType=tagType, 
                            plcTag=f"//{tagAddress}"))
    tagType, tagAddress = plconfig.get_tag(key=f'Machine_PackMl_Mode',dfInformationModelRow=dfinformationModel.iloc[0])
    daMachine.append(makeGenericOutbound(name='PackMlMode', dataType=tagType, 
                            plcTag=f"//{tagAddress}"))

    for groupName, groupeDataFrame in dfinformationModel.groupby('Station'):
        makeStation(daMachineElement=daMachine, dfinformationModel=groupeDataFrame, dfParameters=dfParameters)

    #makeShiftRegisterLoop
créé:    daMachine.append(makeshiftRegister(shiftRegisterName='Loop01', dfinformationModel=dfinformationModel,wphCount=160,nestCount=4))
git@gitlab:Development/tr88/utilities/parsers.git

def makeTwinCatComProtocol(daAutomationDeviceElement: etree.Element, dfinformationModel : pd.DataFrame):
    """
    <da:TwinCatCommProtocol name="TwinCATCommProtocol" simulationEnable="false" disableVitalityCheck="true" >
				<TwinCat name="PlcComm" port="851" ipAddress="172.16.224.115" remoteAmsNetId="172.16.224.115.1.1" localAmsNetId="172.16.224.115.1.2" />
	</da:TwinCatCommProtocol>
    """

    daTwinCatCommProtocol : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}TwinCatCommProtocol")
    daTwinCatCommProtocol.attrib['name'] = f"TwinCatProtocol{dfinformationModel['AutomationDevice'].values[0]}"
    daTwinCatCommProtocol.attrib['simulationEnable'] = "false"
    daTwinCatCommProtocol.attrib['disableVitalityCheck'] = "true"
    daTwinCatCommProtocol.attrib['hmiId'] = f'{random.randint(10000,99999)}'

    twincat : etree.Element = etree.Element("TwinCat")
    twincat.attrib['name'] = f"PlcComm{dfinformationModel['AutomationDevice'].values[0]}"
    twincat.attrib['port'] = "851"
    twincat.attrib['ipAddress'] = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].address
    twincat.attrib['remoteAmsNetId'] = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].remoteAmsNetId
    twincat.attrib['localAmsNetId'] = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].localAmsNetId

    daTwinCatCommProtocol.append(twincat)
    daAutomationDeviceElement.append(daTwinCatCommProtocol)


def makeOpcUaComProtocol(daAutomationDeviceElement: etree.Element, dfinformationModel : pd.DataFrame):
# <da:OpcUaCommProtocol name="opcUaCommProtocol"  ipAddress="192.168.100.100" port="4840"
# simulationEnable="false" disableVitalityCheck="false" plcVitality="Application.ModBus_Array.MDD_a_bArrW4000[5209]" 
# hmiVitality="Application.ModBus_Array.MDD_a_bArrW4000[51]" defaultNamespaceUri="OpcUaServer" logging="true" />
    daOpcUAProtocol : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}OpcUaCommProtocol")
    daOpcUAProtocol.attrib['name'] = f"OpcUaProtocol{dfinformationModel['AutomationDevice'].values[0]}"
    daOpcUAProtocol.attrib['port'] = str(PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].port)    
    daOpcUAProtocol.attrib['ipAddress'] = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].address
    daOpcUAProtocol.attrib['defaultNamespaceUri'] = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].defaultNamespaceUri
    daOpcUAProtocol.attrib['logging'] = "true"
    daOpcUAProtocol.attrib['simulationEnable'] = "false"
    daOpcUAProtocol.attrib['disableVitalityCheck'] = "true"
    daOpcUAProtocol.attrib['hmiId'] = f'{random.randint(10000,99999)}'

    daAutomationDeviceElement.append(daOpcUAProtocol)

def makeEthernetIpComProtocol(daAutomationDeviceElement: etree.Element, dfinformationModel : pd.DataFrame):
# #<da:EthernetIPCommProtocol name="EthernetIPCommProtocol" simulationEnable="false" disableVitalityCheck="true" plcVitality="0:50:0" hmiVitality="0:51:0"> 
#     <EthernetIP name="Machine" ipAddress="192.168.1.1"/> 
# </da:EthernetIPCommProtocol>
    daEthernetIpComProtocol : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}EthernetIPCommProtocol")
    daEthernetIpComProtocol.attrib['name'] = f"EthernetIPCommProtocol{dfinformationModel['AutomationDevice'].values[0]}"
    daEthernetIpComProtocol.attrib['simulationEnable'] = "false"
    daEthernetIpComProtocol.attrib['disableVitalityCheck'] = "true"
    daEthernetIpComProtocol.attrib['hmiId'] = f'{random.randint(10000,99999)}'

    ethernetIp : etree.Element = etree.Element("EthernetIP")
    ethernetIp.attrib['name'] = f"PlcComm{dfinformationModel['AutomationDevice'].values[0]}"
    ethernetIp.attrib['ipAddress'] = PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].address
    daEthernetIpComProtocol.append(ethernetIp)

    daAutomationDeviceElement.append(daEthernetIpComProtocol)

def makeComProtocol(daAutomationDeviceElement: etree.Element, dfinformationModel : pd.DataFrame):
    if dfinformationModel['AutomationDevice'].values[0] in PLC_CONFIG:
        
        match PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].plcType :
            case PlcType.BECKHOFF:
                makeTwinCatComProtocol(daAutomationDeviceElement=daAutomationDeviceElement, dfinformationModel=dfinformationModel)

            case PlcType.OPCUA:
                makeOpcUaComProtocol(daAutomationDeviceElement=daAutomationDeviceElement, dfinformationModel=dfinformationModel)

            case PlcType.ROCKWELL:
                makeEthernetIpComProtocol(daAutomationDeviceElement=daAutomationDeviceElement, dfinformationModel=dfinformationModel)

            case _:
                print(f"Error PlcType {PLC_CONFIG[dfinformationModel['AutomationDevice'].values[0]].plcType} have no driver specified")

    else:
        print(f"Error the PLC {dfinformationModel['AutomationDevice'].values[0]} is not present in the CONFIG_PLC structure")

def makeAlarmsTextFiles(dfAlarms : pd.DataFrame):

    #openFile
    alarmListXml = etree.parse("./BaseFiles/Alarms.xml")
    alarmListEtree : etree.Element = alarmListXml.getroot()

    alarmTranslationXml = etree.parse("./BaseFiles/en-US_Ima.Hmi.Module.Automation.Alarm.xml")
    alarmTranslationEtree : etree.Element = alarmTranslationXml.getroot().find('.//Translation')
    
    for index, alarm in dfAlarms.iterrows():
        #<da:Alarm name="_11_00_Alms.L2.0" scopeId="1" hmiId="1" displayName="Ima.Hmi.Module.Automation&gt;Alarm_5" severity="Alarm" />
        daAlarm : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}Alarm")
        daAlarm.attrib['name'] = alarm['AlarmInput']
        daAlarm.attrib['scopeId'] = str(index +1)
        daAlarm.attrib['hmiId'] = str(index +1)
        daAlarm.attrib['displayName'] = f"Ima.Hmi.Module.Automation>{alarm['AlarmName']}"
        """ Info = 1;
        public const ushort Warning = 401;
        public const ushort Anomaly = 601;
        public const ushort Alarm = 801;
        """
        daAlarm.attrib['severity'] = "Alarm"
        alarmListEtree.append(daAlarm)

        daItem : etree.Element = etree.Element(f"Item")
        daItem.attrib['textId'] = f"{alarm['AlarmName']}"
        daItem.text = alarm['AlarmMessage']
        alarmTranslationEtree.append(daItem)

    etree.indent(alarmListXml, '    ')
    etree.indent(alarmTranslationXml, '    ')

    alarmListXml.write("./OutputFiles/serverConfiguration/02_Application/Data/Services/Alarms.xml", encoding="utf-8", xml_declaration=True)
    alarmTranslationXml.write("./OutputFiles/serverConfiguration/02_Application/Translations/en-US_Ima.Hmi.Module.Automation.Alarm.xml", encoding="utf-8", xml_declaration=True)


def makeAlarms(daAutomationDeviceElement: etree.Element, dfAlarms : pd.DataFrame, dfInformationModel : pd.DataFrame):

    """ <Folder name="Alarms">
            <Primitive name="_01_00_Alms.L1" dataType="Int32" plcTag="_01_00_Alms.L1" />
            <Primitive name="_01_00_Alms.L2" dataType="Int32" plcTag="_01_00_Alms.L2" />        
        </Folder>
    """

    folder : etree.Element = etree.Element("Folder")
    folder.attrib['name'] = "Alarms"

    datatype = "None"
    #depending on the com protocols the datatype of the alarms can change
    match PLC_CONFIG[dfInformationModel['AutomationDevice'].values[0]].plcType :
            case PlcType.BECKHOFF:
                datatype = "Int32"

            # case PlcType.SIEMENS:
            #     pass

            case PlcType.OPCUA:
                datatype = "Int32"

            case PlcType.ROCKWELL:
                datatype = "Int32"

            case _:
                print(f"Error PlcType {PLC_CONFIG[dfInformationModel['AutomationDevice'].values[0]].plcType} have no driver specified")
    
    for alarmAddress  in dfAlarms["AlarmInput"].apply(lambda x : x[0:x.rindex(".")]).unique():
        try:
            alarmSplited : list[str] = alarmAddress.split('_')[1:]
            alarmAddr : str = ""
            if "Alms" in alarmSplited[1]  :
                alarmAddr = f"MAIN_PRG._{infoModelRow['Machine']}_Main.{alarmSplited[-1]}"        
            else:
                infoModelRow = dfInformationModel.loc[(dfInformationModel['Machine'] == alarmSplited[0]) & (dfInformationModel['Station'] == alarmSplited[1])].head(1).to_dict(orient="records")[0]
                alarmAddr = f"MAIN_PRG._{infoModelRow['Machine']}_{infoModelRow['Station']}_{infoModelRow['StationName']}.{alarmSplited[-1]}"        
            folder.append(makePrimitive(name=alarmAddress, dataType=datatype, plcTag=alarmAddr))
        except Exception as e:
            print(f"Error during Alarm creation : {e}")

    daAutomationDeviceElement.append(folder)

def makeAutomationDevice(daApplicationElement : etree.Element, dfinformationModel : pd.DataFrame, dfAlarms : pd.DataFrame, dfParameters : pd.DataFrame):
    daAutomationDevice : etree.Element = etree.Element(f"{{{NAMESPACES['da']}}}AutomationDevice")
    daAutomationDevice.attrib['name'] = dfinformationModel['AutomationDevice'].values[0]    
    daAutomationDevice.attrib['shortcut'] = dfinformationModel['AutomationDevice'].values[0]        
    daAutomationDevice.attrib['hmiId'] = f'{random.randint(10000,99999)}'

    if dfinformationModel['AutomationDevice'].values[0] in PLC_CONFIG:
        daAutomationDevice.attrib['rootAddress'] = dfinformationModel['AutomationDevice'].values[0]

    makeComProtocol(daAutomationDeviceElement=daAutomationDevice, dfinformationModel=dfinformationModel)    

    for groupName, groupeDataFrame in dfinformationModel.groupby('Machine'):
        makeMachine(daAutomationDeviceElement=daAutomationDevice, dfinformationModel=groupeDataFrame, dfParameters=dfParameters)

    makeAlarms(daAutomationDeviceElement=daAutomationDevice, dfAlarms=dfAlarms, dfInformationModel=dfinformationModel)

    daApplicationElement.append(daAutomationDevice)

def addIncludeProjectTags(maininformationmodelElement : etree.Element):

    tagsContainer : etree.Element = maininformationmodelElement.find('.//TagsContainer',NAMESPACES)
    
    includeElement : etree.Element = etree.Element(f"{{{NAMESPACES['']}}}Include")
    #daAutomationDevice.attrib['name'] = dfinformationModel['AutomationDevice'].values[0]
    includeElement.attrib['file'] = "Services/ProjectTags.xml"
    includeElement.attrib['ignoreIfMissing'] = "true"

    tagsContainer.append(includeElement)

    #maininformationmodelElement.append(tagsContainer)

def generateMainInformationModelFromDataFrames(dfinformationModel : pd.DataFrame, dfParameters : pd.DataFrame, dfAlarms : pd.DataFrame) -> etree.Element:
    
    etree.register_namespace('da',NAMESPACES['da'])
    etree.register_namespace('xlink',"http://www.w3.org/1999/xlink")

    maininformationmodelFile = etree.parse("./BaseFiles/MainInformationModelBase.xml")

    maininformationmodel : etree.Element = maininformationmodelFile.getroot()
    
    daApplicationElement : etree.Element = maininformationmodel.find('.//da:Application', NAMESPACES)

    for group_name, group_dataframe in  dfinformationModel.groupby('AutomationDevice'):

        subDfAlarms = dfAlarms[dfAlarms['AutomationDevice'] == group_name]
        #make automation device for this PLC
        makeAutomationDevice(daApplicationElement=daApplicationElement,
                            dfinformationModel=group_dataframe,
                            dfAlarms=subDfAlarms,
                            dfParameters=dfParameters)

    makeAlarmsTextFiles(dfAlarms=dfAlarms)

    # add the include of the ProjectTagsFile in tagsManager
    addIncludeProjectTags(maininformationmodelElement=maininformationmodel)

    etree.indent(maininformationmodel, '    ')
    maininformationmodelFile.write("./OutputFiles/serverConfiguration/02_Application/Data/MainInformationModel.xml", encoding="utf-8", xml_declaration=True)

    return maininformationmodel


def recursiveDiscoverCreateTags(element : etree.Element, tagsDataFrame : pd.DataFrame):

    for children in element.getchildren():
        index = 0
        try:
            index = children.tag.rindex('}')+1
        except:
            pass
        tagsDataFrame.loc[len(tagsDataFrame)] ={'name': children.tag[index:], 'displayName' : f"Ima.Hmi.Module.Automation>Type_{children.tag[index:]}"}
        
        recursiveDiscoverCreateTags(element=children,tagsDataFrame=tagsDataFrame)

def makeProjectHmiTypeTags(maininformationmodel : etree.Element):
    projectTagsFile = etree.parse("./BaseFiles/ProjectTags.xml")

    projectTagsModel : etree.Element = projectTagsFile.getroot()

    tagsFolder : etree.Element = etree.Element(f"{{{NAMESPACES['t']}}}TagsFolder")
    tagsFolder.attrib['name'] = "Type"
    tagsFolder.attrib['displayName'] = "http://www.ima.it/hmi0/ui/translations/common&gt;TagsType"

    projectTagsModel.append(tagsFolder)

    daApplicationElement : etree.Element = maininformationmodel.find('.//da:Application', NAMESPACES)

    tagsDataFrame :pd.DataFrame = pd.DataFrame(columns=['name', 'displayName'])

    recursiveDiscoverCreateTags(element=daApplicationElement, tagsDataFrame=tagsDataFrame)

    for groupName, dataframe in tagsDataFrame.groupby(['name', 'displayName']):

        tag : etree.Element = etree.Element(f"{{{NAMESPACES['']}}}Tag")
        tag.attrib['name'] = groupName[0]
        tag.attrib['displayName'] = groupName[1]

        tagsFolder.append(tag)

    etree.indent(projectTagsModel, '    ')
    projectTagsFile.write("./OutputFiles/serverConfiguration/02_Application/Data/Services/ProjectTags.xml", encoding="utf-8", xml_declaration=True)

if __name__ == '__main__':

    print("Start ConfigFileConverter...")
    dfinformationModel, dfParameters, dfAlarms = excelConfigFilesToDataFrames(CONFIG_FILE_FOLDER)

    maininformationmodel : etree.Element = generateMainInformationModelFromDataFrames(dfinformationModel=dfinformationModel, dfParameters=dfParameters, dfAlarms=dfAlarms)

    makeProjectHmiTypeTags(maininformationmodel=maininformationmodel)


