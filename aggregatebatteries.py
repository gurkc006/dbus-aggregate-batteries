#!/usr/bin/env python3

"""
Service to aggregate multiple serial batteries https://github.com/Louisvdw/dbus-serialbattery
to one virtual battery.

Python location on Venus:
/usr/bin/python3.8
/usr/lib/python3.8/site-packages/

References:
https://dbus.freedesktop.org/doc/dbus-python/tutorial.html
https://github.com/victronenergy/venus/wiki/dbus
https://github.com/victronenergy/velib_python
"""

VERSION = '1.0'

from gi.repository import GLib
import logging
import sys
import os
import dbus
import re
from settings import *
from functions import *
from datetime import datetime as dt         # for UTC time stamps for logging
import time as tt                           # for charge measurement
from dbusmon import DbusMon
from threading import Thread

sys.path.append('/opt/victronenergy/dbus-systemcalc-py/ext/velib_python')
from vedbus import VeDbusService

#class DbusVariable(object):
#
#    def __init__(self, service):
#        self._path: str = ''
#        self._dbusservice: VeDbusService = service._dbusservice
#        self._add_params: list = {}

class DbusAggBatService(object):
    
    def __init__(self, servicename='com.victronenergy.battery.aggregate'):
        self._fn = Functions()
        self._batteries_dict = {}               # marvo2011
        self._multi = None
        self._grid = None
        self._mppts_list = []
        self._smartShunt = None
        self._searchTrials = 0
        self._readTrials = 0
        self._MaxChargeVoltage_old = 0
        self._MaxChargeCurrent_old = 0
        self._MaxDischargeCurrent_old = 0
        self._fullyDischarged = False           # implementing hysteresis for allowing discharge
        self._dbusservice = VeDbusService(servicename)
        self._dbusConn = dbus.SessionBus()  if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else dbus.SystemBus()
        self._timeOld = tt.time()
        self._DCfeedActive = False              # written when dynamic CVL limit activated
        self._balancing = 0                     # 0: inactive; 1: goal reached, waiting for discharging under nominal voltage; 2: nominal voltage reached  
        self._lastBalancing = 0                 # Day in year
        self._dynamicCVL = False                # set if the CVL needs to be reduced due to peaking            
        self._logTimer = 0                      # measure logging period in seconds
        self._EssActive = 0
        self._SmoothFilter = 251
        self._MaxChargeCurrentSm = 0
        
        # read initial charge from text file
        try:
            self._charge_file = open('/data/dbus-aggregate-batteries/charge', 'r')      # read
            self._ownCharge = float(self._charge_file.readline().strip())
            self._charge_file.close()
            self._ownCharge_old = self._ownCharge
            logging.info('%s: Initial Ah read from file: %.0fAh' % ((dt.now()).strftime('%c'), self._ownCharge))
        except Exception:
            logging.error('%s: Charge file read error. Exiting.' % (dt.now()).strftime('%c'))
            sys.exit()

        if OWN_CHARGE_PARAMETERS:               # read the day of the last balancing from text file
            try:
                self._lastBalancing_file = open('/data/dbus-aggregate-batteries/last_balancing', 'r')      # read
                self._lastBalancing = int(self._lastBalancing_file.readline().strip())
                self._lastBalancing_file.close()
                time_unbalanced = int((dt.now()).strftime('%j')) - self._lastBalancing                  # in days
                if time_unbalanced < 0:
                    time_unbalanced += 365                                                              # year change
                logging.info('%s: Last balancing done at the %d. day of the year' % ((dt.now()).strftime('%c'), self._lastBalancing))
                logging.info('Batteries balanced %d days ago.' % time_unbalanced)
            except Exception:
                logging.error('%s: Last balancing file read error. Exiting.' % (dt.now()).strftime('%c'))
                sys.exit()         
 
        # Create the mandatory objects
        self._dbusservice.add_mandatory_paths(processname = __file__, processversion = '0.0', connection = 'Virtual',
			deviceinstance = 0, productid = 0, productname = 'AggregateBatteries', firmwareversion = VERSION, 
            hardwareversion = '0.0', connected = 1)

        # Create DC paths        
        self._dbusservice.add_path('/Dc/0/Voltage', None, writeable=True, gettextcallback=lambda a, x: "{:.2f}V".format(x))
        self._dbusservice.add_path('/Dc/0/Current', None, writeable=True, gettextcallback=lambda a, x: "{:.2f}A".format(x))
        self._dbusservice.add_path('/Dc/0/Power', None, writeable=True, gettextcallback=lambda a, x: "{:.0f}W".format(x))
        
        # Create capacity paths
        self._dbusservice.add_path('/Soc', None, writeable=True)
        self._dbusservice.add_path('/Capacity', None, writeable=True, gettextcallback=lambda a, x: "{:.0f}Ah".format(x))
        self._dbusservice.add_path('/InstalledCapacity', None, gettextcallback=lambda a, x: "{:.0f}Ah".format(x))
        self._dbusservice.add_path('/ConsumedAmphours', None, gettextcallback=lambda a, x: "{:.0f}Ah".format(x))
        
        # Create temperature paths
        self._dbusservice.add_path('/Dc/0/Temperature', None, writeable=True)       
        self._dbusservice.add_path('/System/MinCellTemperature', None, writeable=True)
        self._dbusservice.add_path('/System/MaxCellTemperature', None, writeable=True)     
        
        # Create extras paths
        self._dbusservice.add_path('/System/MinCellVoltage', None, writeable=True, gettextcallback=lambda a, x: "{:.3f}V".format(x))    # marvo2011
        self._dbusservice.add_path('/System/MinVoltageCellId', None, writeable=True)
        self._dbusservice.add_path('/System/MaxCellVoltage', None, writeable=True, gettextcallback=lambda a, x: "{:.3f}V".format(x))    # marvo2011
        self._dbusservice.add_path('/System/MaxVoltageCellId', None, writeable=True)
        self._dbusservice.add_path('/System/NrOfCellsPerBattery', None, writeable=True)
        self._dbusservice.add_path('/System/NrOfModulesOnline', None, writeable=True)
        self._dbusservice.add_path('/System/NrOfModulesOffline', None, writeable=True)
        self._dbusservice.add_path('/System/NrOfModulesBlockingCharge', None, writeable=True)
        self._dbusservice.add_path('/System/NrOfModulesBlockingDischarge', None, writeable=True)
        self._dbusservice.add_path('/Voltages/Sum', None, writeable=True, gettextcallback=lambda a, x: "{:.3f}V".format(x))
        self._dbusservice.add_path('/Voltages/Diff', None, writeable=True, gettextcallback=lambda a, x: "{:.3f}V".format(x))
        self._dbusservice.add_path('/TimeToGo', None, writeable=True)
        
        # Create alarm paths
        self._dbusservice.add_path('/Alarms/LowVoltage', None, writeable=True)
        self._dbusservice.add_path('/Alarms/HighVoltage', None, writeable=True)
        self._dbusservice.add_path('/Alarms/LowCellVoltage', None, writeable=True)
        #self._dbusservice.add_path('/Alarms/HighCellVoltage', None, writeable=True)
        self._dbusservice.add_path('/Alarms/LowSoc', None, writeable=True)
        self._dbusservice.add_path('/Alarms/HighChargeCurrent', None, writeable=True)
        self._dbusservice.add_path('/Alarms/HighDischargeCurrent', None, writeable=True)
        self._dbusservice.add_path('/Alarms/CellImbalance', None, writeable=True)
        self._dbusservice.add_path('/Alarms/InternalFailure', None, writeable=True)
        self._dbusservice.add_path('/Alarms/HighChargeTemperature', None, writeable=True)
        self._dbusservice.add_path('/Alarms/LowChargeTemperature', None, writeable=True)
        self._dbusservice.add_path('/Alarms/HighTemperature', None, writeable=True)
        self._dbusservice.add_path('/Alarms/LowTemperature', None, writeable=True)
        self._dbusservice.add_path('/Alarms/BmsCable', None, writeable=True)
        
        # Create control paths
        self._dbusservice.add_path('/Info/MaxChargeCurrent', None, writeable=True, gettextcallback=lambda a, x: "{:.1f}A".format(x))
        self._dbusservice.add_path('/Info/MaxDischargeCurrent', None, writeable=True, gettextcallback=lambda a, x: "{:.1f}A".format(x))
        self._dbusservice.add_path('/Info/MaxChargeVoltage', None, writeable=True, gettextcallback=lambda a, x: "{:.2f}V".format(x))
        self._dbusservice.add_path('/Io/AllowToCharge', None, writeable=True)
        self._dbusservice.add_path('/Io/AllowToDischarge', None, writeable=True)
        self._dbusservice.add_path('/Io/AllowToBalance', None, writeable=True)

        # Create battery current control paths
        self._dbusservice.add_path('/Ess/Active', 0, writeable=True, onchangecallback=self._onDbusUpdate)
        self._dbusservice.add_path('/Ess/BatteryP', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/BatteryI', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/BatteryCalcI', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/MpptP', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/MpptI', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/AcInP', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/AcInI', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/AcOutP', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/AcOutI', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/InverterP', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/InverterI', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/MaxChargeP', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/MaxChargeI', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/MaxChargeIsm', None, writeable=False, gettextcallback=lambda a, x: "{:.2f} A".format(x))
        self._dbusservice.add_path('/Ess/GridSetpoint', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/GridP', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/AcPowerSetpoint', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} W".format(x))
        self._dbusservice.add_path('/Ess/MaxChrgCellVoltage', None, writeable=False, gettextcallback=lambda a, x: "{:.3f} V".format(x))
        self._dbusservice.add_path('/Ess/SmoothFilter', self._SmoothFilter, writeable=True, onchangecallback=self._onDbusUpdate)
        self._dbusservice.add_path('/Ess/ConsumptionInputL1', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/ConsumptionInputL2', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/ConsumptionInputL3', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/ConsumptionInput', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/PvOnGridL1', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/PvOnGridL2', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/PvOnGridL3', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/PvOnGrid', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/AcLoadL1', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/AcLoadL2', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/AcLoadL3', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/AcLoad', None, writeable=False, gettextcallback=lambda a, x: "{:.1f} W".format(x))
        self._dbusservice.add_path('/Ess/CorrectionI', None, writeable=False, gettextcallback=lambda a, x: "{:.3f} A".format(x))
        self._dbusservice.add_path('/Ess/MinimumSocLimit', None, writeable=False, gettextcallback=lambda a, x: "{:.0f} %".format(x))

        x = Thread(target = self._startMonitor)
        x.start()   

        GLib.timeout_add(10000, self._find_settings)                     # search com.victronenergy.settings

    ##############################################################################################################
    ##############################################################################################################
    ### Starting battery dbus monitor in external thread (otherwise collision with AggregateBatteries service) ###
    ##############################################################################################################
    ##############################################################################################################
    
    def _startMonitor(self):
        logging.info('%s: Starting battery monitor.' % (dt.now()).strftime('%c'))
        self._dbusMon = DbusMon()


    #####################################################################
    #####################################################################
    ### switch Ess test features on/off                               ###
    #####################################################################
    #####################################################################

    def _onDbusUpdate(self, path, value):
        if path == '/Ess/Active':
            logging.info('%s: Ess/Active manually set to %d' % ((dt.now()).strftime('%c'), value))
            if value == 0:
                self._EssActive = value
                self._dbusMon.dbusmon.set_value('com.victronenergy.settings', '/Settings/CGwacs/Hub4Mode', 1)
                logging.info('%s: Hub4Mode set to normal control!' % ((dt.now()).strftime('%c')))
            elif value > 0 and value <=5:
                self._EssActive = value
                self._dbusMon.dbusmon.set_value('com.victronenergy.settings', '/Settings/CGwacs/Hub4Mode', 3)
                logging.info('%s: Hub4Mode set to external control!' % ((dt.now()).strftime('%c')))
            else:
                logging.info('%s: wrong value! Reset to old value!' % ((dt.now()).strftime('%c')))
        elif path == '/Ess/SmoothFilter':
            self._SmoothFilter = value
            logging.info('%s: /Ess/SmoothFilter manually set to %d' % ((dt.now()).strftime('%c'), self._SmoothFilter))
        else:
            pass
        
        return True


    #####################################################################
    #####################################################################
    ### search Settings, to maintain CCL during dynamic CVL reduction ###
    # https://www.victronenergy.com/upload/documents/Cerbo_GX/140558-CCGX__Venus_GX__Cerbo_GX__Cerbo-S_GX_Manual-pdf-en.pdf, P72
    #####################################################################
    #####################################################################
    
    def _find_settings(self):
        logging.info('%s: Searching Settings: Trial Nr. %d' % ((dt.now()).strftime('%c'),(self._searchTrials + 1)))
        try:
            for service in self._dbusConn.list_names():
                if 'com.victronenergy.settings' in service:
                    self._settings = service
                    logging.info('%s: com.victronenergy.settings found.' % (dt.now()).strftime('%c'))
        except Exception:
            pass
            
        if (self._settings != None):
            self._searchTrials = 0
            GLib.timeout_add(1000, self._find_batteries)                # search batteries on DBus if present    ##### !!! was 5000 cg
            return False                                                # all OK, stop calling this function
        elif self._searchTrials < SEARCH_TRIALS:
            self._searchTrials += 1
            return True                                                 # next trial
        else:
            logging.error('%s: com.victronenergy.settings not found. Exiting.' % (dt.now()).strftime('%c'))
            sys.exit()  
    
    #####################################################################
    #####################################################################
    ### search physical batteries and optional SmartShunt on DC loads ###
    #####################################################################
    #####################################################################
    
    def _find_batteries(self):
        self._batteries_dict = {}    # Marvo2011
        batteriesCount = 0
        productName = ''
        logging.info('%s: Searching batteries: Trial Nr. %d' % ((dt.now()).strftime('%c'),(self._searchTrials + 1)))
        
        try:                                                            # if Dbus monitor not running yet, new trial instead of exception
            for service in self._dbusConn.list_names():
                if BATTERY_SERVICE_NAME in service:
                    productName = self._dbusMon.dbusmon.get_value(service, BATTERY_PRODUCT_NAME_PATH)
                    if BATTERY_PRODUCT_NAME in productName:    
                        
                        # Custom name, if exists, Marvo2011
                        try:
                            BatteryName = self._dbusMon.dbusmon.get_value(service, BATTERY_INSTANCE_NAME_PATH)
                        except Exception:
                            BatteryName = 'Battery%d' % (batteriesCount + 1)     
                        # Check if all batteries have custom names 
                        if BatteryName in self._batteries_dict:
                            BatteryName = '%s%d' %(BatteryName, batteriesCount + 1)

                        self._batteries_dict[BatteryName] = service
                        logging.info('%s: %s found, named as: %s.' % ((dt.now()).strftime('%c'),(self._dbusMon.dbusmon.get_value(service, '/ProductName')), BatteryName))
                        
                        batteriesCount += 1
                        
                        # Create voltage paths with battery names
                        if SEND_CELL_VOLTAGES == 1:
                            for cellId in range(1, (NR_OF_CELLS_PER_BATTERY) + 1):
                                self._dbusservice.add_path('/Voltages/%s_Cell%d' % (re.sub('[^A-Za-z0-9_]+', '', BatteryName), cellId), None, writeable=True, gettextcallback=lambda a, x: "{:.3f}V".format(x))

                        # Check if Nr. of cells is equal
                        if self._dbusMon.dbusmon.get_value(service, '/System/NrOfCellsPerBattery') != NR_OF_CELLS_PER_BATTERY:
                            logging.error('%s: Number of cells of batteries is not correct. Exiting.'  % (dt.now()).strftime('%c'))
                            sys.exit()
                                
                        # end of section, Marvo2011        
                        
                    elif SMARTSHUNT_NAME_KEY_WORD in productName:           # if SmartShunt found, can be used for DC load current
                        self._smartShunt = service
                    
        except Exception:
            pass
        logging.info('%s: %d batteries found.' % ((dt.now()).strftime('%c'), batteriesCount))
        
        if batteriesCount == NR_OF_BATTERIES:
            if CURRENT_FROM_VICTRON:
                self._searchTrials = 0
                GLib.timeout_add(1000, self._find_multis)               # if current from Victron stuff search multi/quattro on DBus
            else:
                self._timeOld = tt.time()
                GLib.timeout_add(UPDATE_INTV, self._update)                    # if current from BMS start the _update loop
            return False                                                # all OK, stop calling this function
        elif self._searchTrials < SEARCH_TRIALS:
            self._searchTrials += 1
            return True                                                 # next trial
        else:
            logging.error('%s: Required number of batteries not found. Exiting.' % (dt.now()).strftime('%c'))
            sys.exit()
    
    ##########################################################################
    ##########################################################################
    ### search Multis or Quattros (if selected for DC current measurement) ###
    ##########################################################################
    ##########################################################################
    
    def _find_multis(self):
        logging.info('%s: Searching Multi/Quatro VEbus: Trial Nr. %d' % ((dt.now()).strftime('%c'),(self._searchTrials + 1)))
        try:
            for service in self._dbusConn.list_names():
                if MULTI_KEY_WORD in service:
                    self._multi = service
                    logging.info('%s: %s found.' % ((dt.now()).strftime('%c'),(self._dbusMon.dbusmon.get_value(service, '/ProductName'))))
        except Exception:
            pass
            
        if (self._multi != None):        
            if (NR_OF_MPPTS > 0):
                self._searchTrials = 0
                GLib.timeout_add(1000, self._find_mppts)                # search MPPTs on DBus if present
            else:
                self._timeOld = tt.time()
                GLib.timeout_add(UPDATE_INTV, self._update)                    # if no MPPTs start the _update loop
            return False                                                # all OK, stop calling this function
        elif self._searchTrials < SEARCH_TRIALS:
            self._searchTrials += 1
            return True                                                 # next trial
        else:
            logging.error('%s: Multi/Quattro not found. Exiting.' % (dt.now()).strftime('%c'))
            sys.exit()    
            
    #############################################################
    #############################################################
    ### search MPPTs (if selected for DC current measurement) ###
    #############################################################
    #############################################################
    
    def _find_mppts(self):
        self._mppts_list = []
        mpptsCount = 0
        logging.info('%s: Searching MPPTs: Trial Nr. %d' % ((dt.now()).strftime('%c'),(self._searchTrials + 1)))
        try:
            for service in self._dbusConn.list_names():
                if MPPT_KEY_WORD in service:
                    self._mppts_list.append(service)
                    logging.info('%s: %s found.' % ((dt.now()).strftime('%c'),(self._dbusMon.dbusmon.get_value(service, '/ProductName'))))
                    mpptsCount += 1
        except Exception:
            pass
            
        logging.info('%s: %d MPPT(s) found.' % ((dt.now()).strftime('%c'), mpptsCount))
        if mpptsCount == NR_OF_MPPTS:
            self._timeOld = tt.time()
            GLib.timeout_add(1000, self._find_grid)
            return False                                                    # all OK, stop calling this function
        elif self._searchTrials < SEARCH_TRIALS:
            self._searchTrials += 1
            return True                                                     # next trial
        else:
            logging.error('%s: Required number of MPPTs not found. Exiting.' % (dt.now()).strftime('%c'))
            sys.exit()


    #############################################################
    #############################################################
    ### search grid meter ###
    #############################################################
    #############################################################
    
    def _find_grid(self):
        logging.info('%s: Searching grid meter: Trial Nr. %d' % ((dt.now()).strftime('%c'),(self._searchTrials + 1)))
        try:
            for service in self._dbusConn.list_names():
                # logging.error('%s: service=%s' % ((dt.now()).strftime('%c'),service))
                if GRID_KEY_WORD in service:
                    self._grid = service
                    logging.info('%s: %s found.' % ((dt.now()).strftime('%c'),(self._dbusMon.dbusmon.get_value(service, '/ProductName'))))
        except Exception:
            logging.info('%s: Exception' % (dt.now()).strftime('%c'))
            
        if (self._grid != None):        
            self._timeOld = tt.time()
            GLib.timeout_add(UPDATE_INTV, self._update)                    # if no MPPTs start the _update loop
            return False                                                # all OK, stop calling this function
        elif self._searchTrials < SEARCH_TRIALS:
            self._searchTrials += 1
            return True                                                 # next trial
        else:
            logging.error('%s: Grid meter not found. Exiting.' % (dt.now()).strftime('%c'))
            sys.exit()    



    ##################################################################################
    ##################################################################################     
    #### aggregate values of physical batteries, perform calculations, update Dbus ###
    ################################################################################## 
    ################################################################################## 
    
    def _update(self):  
        
        # DC
        Voltage = 0
        Current = 0
        Power = 0
        
        # Capacity
        Soc = 0
        Capacity = 0
        InstalledCapacity = 0
        ConsumedAmphours = 0
        TimeToGo = 0    
        
        # Temperature
        Temperature = 0
        MaxCellTemp_list = []               # list, maxima of all physical batteries
        MinCellTemp_list = []               # list, minima of all physical batteries
        
        # Extras
        cellVoltages_dict = {}
        MaxCellVoltage_dict = {}            # dictionary {'ID' : MaxCellVoltage, ... } for all physical batteries
        MinCellVoltage_dict = {}            # dictionary {'ID' : MinCellVoltage, ... } for all physical batteries        
        NrOfModulesOnline = 0
        NrOfModulesOffline = 0
        NrOfModulesBlockingCharge = 0
        NrOfModulesBlockingDischarge = 0
        VoltagesSum_dict = {}               # battery voltages from sum of cells, Marvo2011
        chargeVoltageReduced_list = []
        
        # Alarms
        LowVoltage_alarm_list = []          # lists to find maxima
        HighVoltage_alarm_list = []
        LowCellVoltage_alarm_list = []
        LowSoc_alarm_list = []
        HighChargeCurrent_alarm_list = []
        HighDischargeCurrent_alarm_list = []
        CellImbalance_alarm_list = []
        InternalFailure_alarm_list = []
        HighChargeTemperature_alarm_list = []
        LowChargeTemperature_alarm_list = []
        HighTemperature_alarm_list = []
        LowTemperature_alarm_list = []
        BmsCable_alarm_list = []
        
        # Charge/discharge parameters
        MaxChargeCurrent_list = []           # the minimum of MaxChargeCurrent * NR_OF_BATTERIES to be transmitted
        MaxDischargeCurrent_list = []        # the minimum of MaxDischargeCurrent * NR_OF_BATTERIES to be transmitted
        MaxChargeVoltage_list = []           # if some cells are above MAX_CELL_VOLTAGE, store here the sum of differences for each battery
        AllowToCharge_list = []              # minimum of all to be transmitted
        AllowToDischarge_list = []           # minimum of all to be transmitted
        AllowToBalance_list = []             # minimum of all to be transmitted
        ChargeMode_list = []                 # Bulk, Absorption, Float, Keep always max voltage   

        # Ess stuff
        BatteryPower = 0
        BatteryCurrent = 0
        MpptCurrent = 0
        MpptPower = 0
        AcInPower = 0
        AcInCurrent = 0
        AcOutPower = 0
        AcOutCurrent = 0
        InverterPower = 0
        InverterCurrent = 0

        MaxChargePower = 0
        MaxChargeCurrent = 0
        MaxChargeVoltage = 0

        MaxDischargePower = 0
        MaxDischargeCurrent = 0

        GridSetpoint = 0
        GridPower = 0
        GridL1 = 0
        GridL2 = 0
        GridL3 = 0
        AcPowerSetpoint = 0
        BatteryCurrentCalc = 0
        MaxChrgCellVoltage = 0

        ConsumptionInputL1 = 0
        ConsumptionInputL2 = 0
        ConsumptionInputL3 = 0
        ConsumptionInput = 0

        PvOnGridL1 = 0
        PvOnGridL2 = 0
        PvOnGridL3 = 0
        PvOnGrid = 0

        AcLoadL1 = 0
        AcLoadL2 = 0
        AcLoadL3 = 0
        AcLoad = 0

        MinimumSocLimit = 0

        ####################################################
        # Get DBus values from all SerialBattery instances #
        ####################################################
        
        try:
            for i in self._batteries_dict:   # Marvo2011 

                # DC                                               
                step = 'Read V, I, P'       # to detect error
                Voltage += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Dc/0/Voltage')                                                                                              
                Current += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Dc/0/Current')                                                                                                     
                Power += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Dc/0/Power')                                                            
                
                # Capacity                                               
                step = 'Read and calculate capacity, SoC, Time to go'
                InstalledCapacity += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/InstalledCapacity')                                         
                
                if not OWN_SOC:
                    ConsumedAmphours += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/ConsumedAmphours')                                       
                    Capacity += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Capacity')
                    Soc += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Soc') * self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Capacity')
                    ttg = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/TimeToGo')
                    if (ttg != None) and (TimeToGo != None):
                        TimeToGo += ttg * self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Capacity')                                             
                    else:
                        TimeToGo = None    
                
                # Temperature
                step = 'Read temperatures'
                Temperature += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Dc/0/Temperature')
                MaxCellTemp_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MaxCellTemperature'))
                MinCellTemp_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MinCellTemperature'))

                # Cell voltages
                step = 'Read max. and min cell voltages and voltage sum'         # cell ID : its voltage
                MaxCellVoltage_dict['%s_%s' % (i, self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MaxVoltageCellId'))]\
                    = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MaxCellVoltage')
                MinCellVoltage_dict['%s_%s' % (i, self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MinVoltageCellId'))]\
                    = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/MinCellVoltage')                                                        
                VoltagesSum_dict[i] = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Voltages/Sum')                                             
                
                # Battery state                
                step = 'Read battery state'
                NrOfModulesOnline += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/NrOfModulesOnline')
                NrOfModulesOffline += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/NrOfModulesOffline')
                NrOfModulesBlockingCharge += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/NrOfModulesBlockingCharge')
                NrOfModulesBlockingDischarge += self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/System/NrOfModulesBlockingDischarge')            # sum of modules blocking discharge
                
                step = 'Read cell voltages'
                for j in range(NR_OF_CELLS_PER_BATTERY):            # Marvo2011
                    cellVoltages_dict['%s_Cell%d' % (i, j+1)] = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Voltages/Cell%d' % (j+1))                        
                
                # Alarms
                step = 'Read alarms'
                LowVoltage_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/LowVoltage'))
                HighVoltage_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/HighVoltage'))
                LowCellVoltage_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/LowCellVoltage'))
                LowSoc_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/LowSoc'))
                HighChargeCurrent_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/HighChargeCurrent'))
                HighDischargeCurrent_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/HighDischargeCurrent'))
                CellImbalance_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/CellImbalance'))
                InternalFailure_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/InternalFailure_alarm'))
                HighChargeTemperature_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/HighChargeTemperature'))
                LowChargeTemperature_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/LowChargeTemperature'))
                HighTemperature_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/HighTemperature'))
                LowTemperature_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/LowTemperature'))
                BmsCable_alarm_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Alarms/BmsCable'))
                
                if OWN_CHARGE_PARAMETERS:    # calculate reduction of charge voltage as sum of overvoltages of all cells
                    step = 'Calculate CVL reduction'
                    cellOvervoltage = 0
                    for j in range (NR_OF_CELLS_PER_BATTERY):   # Marvo2011
                        cellVoltage = self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Voltages/Cell%d' % (j+1))
                        if (cellVoltage > MAX_CELL_VOLTAGE):
                            cellOvervoltage += (cellVoltage - MAX_CELL_VOLTAGE)   
                    chargeVoltageReduced_list.append(VoltagesSum_dict[i] - cellOvervoltage) 
                
                else:    # Aggregate charge/discharge parameters
                    step = 'Read charge parameters'
                    MaxChargeCurrent_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Info/MaxChargeCurrent'))                        # list of max. charge currents to find minimum
                    MaxDischargeCurrent_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Info/MaxDischargeCurrent'))                  # list of max. discharge currents  to find minimum
                    MaxChargeVoltage_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Info/MaxChargeVoltage'))                        # list of max. charge voltages  to find minimum
                    ChargeMode_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Info/ChargeMode'))                                    # list of charge modes of batteries (Bulk, Absorption, Float, Keep always max voltage)
                    
                step = 'Read Allow to'
                AllowToCharge_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Io/AllowToCharge'))                                    # list of AllowToCharge to find minimum
                AllowToDischarge_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Io/AllowToDischarge'))                              # list of AllowToDischarge to find minimum
                AllowToBalance_list.append(self._dbusMon.dbusmon.get_value(self._batteries_dict[i], '/Io/AllowToBalance'))                                  # list of AllowToBalance to find minimum  
        
            step = 'Find max. and min. cell voltage of all batteries'
            # placed in try-except structure for the case if some values are of None. The _max() and _min() don't work with dictionaries
            MaxVoltageCellId = max(MaxCellVoltage_dict, key = MaxCellVoltage_dict.get)
            MaxCellVoltage = MaxCellVoltage_dict[MaxVoltageCellId]
            MinVoltageCellId = min(MinCellVoltage_dict, key = MinCellVoltage_dict.get)
            MinCellVoltage = MinCellVoltage_dict[MinVoltageCellId]
        
        except Exception as err:
            self._readTrials += 1
            logging.error('%s: Error: %s.' % ((dt.now()).strftime('%c'), err))
            logging.error('Occured during step %s, Battery %s.' % (step, i))
            logging.error('Read trial nr. %d' % self._readTrials)
            if (self._readTrials > READ_TRIALS):
                logging.error('%s: DBus read failed. Exiting.'  % (dt.now()).strftime('%c'))
                sys.exit()
            else:
                return True         # next call allowed
        
        self._readTrials = 0        # must be reset after try-except
        
        #####################################################
        # Process collected values (except of dictionaries) #
        #####################################################
        
        # averaging
        Voltage = Voltage / NR_OF_BATTERIES
        Temperature = Temperature / NR_OF_BATTERIES
        VoltagesSum = sum(VoltagesSum_dict.values()) / NR_OF_BATTERIES                      # Marvo2011               
        
        # find max and min cell temperature (have no ID)
        MaxCellTemp = self._fn._max(MaxCellTemp_list)
        MinCellTemp = self._fn._min(MinCellTemp_list)
        
        # find max in alarms
        LowVoltage_alarm = self._fn._max(LowVoltage_alarm_list)
        HighVoltage_alarm = self._fn._max(HighVoltage_alarm_list)
        LowCellVoltage_alarm = self._fn._max(LowCellVoltage_alarm_list)
        LowSoc_alarm = self._fn._max(LowSoc_alarm_list)
        HighChargeCurrent_alarm = self._fn._max(HighChargeCurrent_alarm_list)
        HighDischargeCurrent_alarm = self._fn._max(HighDischargeCurrent_alarm_list)
        CellImbalance_alarm = self._fn._max(CellImbalance_alarm_list)
        InternalFailure_alarm = self._fn._max(InternalFailure_alarm_list)
        HighChargeTemperature_alarm = self._fn._max(HighChargeTemperature_alarm_list)
        LowChargeTemperature_alarm = self._fn._max(LowChargeTemperature_alarm_list)
        HighTemperature_alarm = self._fn._max(HighTemperature_alarm_list)
        LowTemperature_alarm = self._fn._max(LowTemperature_alarm_list)
        BmsCable_alarm = self._fn._max(BmsCable_alarm_list)
        
        # find max. charge voltage (if needed)
        #if not OWN_CHARGE_PARAMETERS:
        if self._fn._min(MaxChargeVoltage_list):
            MaxChargeVoltage = self._fn._min(MaxChargeVoltage_list)                     # add KEEP_MAX_CVL
        if self._fn._min(MaxChargeCurrent_list):
            MaxChargeCurrent = self._fn._min(MaxChargeCurrent_list) * NR_OF_BATTERIES
        if self._fn._min(MaxDischargeCurrent_list):
            MaxDischargeCurrent = self._fn._min(MaxDischargeCurrent_list) * NR_OF_BATTERIES
        
        AllowToCharge = self._fn._min(AllowToCharge_list)
        AllowToDischarge = self._fn._min(AllowToDischarge_list)
        AllowToBalance = self._fn._min(AllowToBalance_list)
        
        ####################################
        # Measure current by Victron stuff #
        ####################################
        
        if CURRENT_FROM_VICTRON:
            try:
                Current_VE = self._dbusMon.dbusmon.get_value(self._multi, '/Dc/0/Current')                                          # get DC current of multi/quattro (or system of them)
                for i in range(NR_OF_MPPTS):
                    MpptCurrent += self._dbusMon.dbusmon.get_value(self._mppts_list[i], '/Dc/0/Current')                             # add DC current of all MPPTs (if present)          
                Current_VE += MpptCurrent
                MpptPower = MpptCurrent * Voltage
                
                if DC_LOADS:
                    if INVERT_SMARTSHUNT:
                        Current_VE += self._dbusMon.dbusmon.get_value(self._smartShunt, '/Dc/0/Current')                            # SmartShunt is monitored as a battery
                    else:
                        Current_VE -= self._dbusMon.dbusmon.get_value(self._smartShunt, '/Dc/0/Current')
                                                                                                       
                if Current_VE is not None:
                    Current = Current_VE                                                                                            # BMS current overwritten only if no exception raised
                    Power = Voltage * Current_VE                                                                                    # calculate own power (not read from BMS)
                else:
                    logging.error('%s: Victron current is None. Using BMS current and power instead.' % (dt.now()).strftime('%c'))  # the BMS values are not overwritten    
            
            except Exception:
                logging.error('%s: Victron current read error. Using BMS current and power instead.' % (dt.now()).strftime('%c'))   # the BMS values are not overwritten       
        
        ####################################################################################################
        # Calculate own charge/discharge parameters (overwrite the values received from the SerialBattery) #
        ####################################################################################################
        
        if OWN_CHARGE_PARAMETERS:
            CVL_NORMAL = NR_OF_CELLS_PER_BATTERY * CHARGE_VOLTAGE_LIST[int((dt.now()).strftime('%m')) - 1]
            CVL_BALANCING = NR_OF_CELLS_PER_BATTERY * BALANCING_VOLTAGE
            ChargeVoltageBattery = CVL_NORMAL
            
            time_unbalanced = int((dt.now()).strftime('%j')) - self._lastBalancing                  # in days
            if time_unbalanced < 0:
                time_unbalanced += 365                                                              # year change
            
            if (CVL_BALANCING > CVL_NORMAL):                                                        # if the normal charging voltage is lower then 100% SoC
                # manage balancing voltage    
                if (self._balancing == 0) and (time_unbalanced >= BALANCING_REPETITION):
                    self._balancing = 1                                                             # activate increased CVL for balancing
                    logging.info('%s: CVL increase for balancing activated.'  % (dt.now()).strftime('%c'))
       
                if self._balancing == 1:
                    ChargeVoltageBattery = CVL_BALANCING
                    if (Voltage >= 0.999 * CVL_BALANCING):
                        self._ownCharge = InstalledCapacity                                         # reset Coulumb counter to 100%
                        if ((MaxCellVoltage - MinCellVoltage) < CELL_DIFF_MAX):
                            self._balancing = 2;
                            logging.info('%s: Balancing goal reached.'  % (dt.now()).strftime('%c'))    
            
                if self._balancing >= 2:
                    ChargeVoltageBattery = CVL_BALANCING                                            # keep balancing voltage at balancing day until decrease of solar powers and   
                    if Voltage <= CVL_NORMAL:                                                       # the charge above "normal" is consumed
                        self._balancing = 0;
                        self._lastBalancing = int((dt.now()).strftime('%j'))
                        self._lastBalancing_file = open('/data/dbus-aggregate-batteries/last_balancing', 'w')
                        self._lastBalancing_file.write('%s' % self._lastBalancing)
                        self._lastBalancing_file.close()
                        logging.info('%s: CVL increase for balancing de-activated.'  % (dt.now()).strftime('%c'))
            
                if self._balancing == 0:
                    ChargeVoltageBattery = CVL_NORMAL
                    
            elif (time_unbalanced > 0) and (Voltage >= 0.999 * CVL_BALANCING) and ((MaxCellVoltage - MinCellVoltage) < CELL_DIFF_MAX):   # if normal charging voltage is 100% SoC and balancing is finished
                self._ownCharge = InstalledCapacity                                                 # reset Coulumb counter to 100%
                logging.info('%s: Balancing goal reached with full charging set as normal. Updating last_balancing file.'  % (dt.now()).strftime('%c'))
                self._lastBalancing = int((dt.now()).strftime('%j'))
                self._lastBalancing_file = open('/data/dbus-aggregate-batteries/last_balancing', 'w')
                self._lastBalancing_file.write('%s' % self._lastBalancing)
                self._lastBalancing_file.close()
            
            # manage dynamic CVL reduction 
            if MaxCellVoltage >= MAX_CELL_VOLTAGE:                         
                if not self._dynamicCVL:
                    self._dynamicCVL = True
                    logging.info('%s: Dynamic CVL reduction started.'  % (dt.now()).strftime('%c'))
                    if self._DCfeedActive == False:                                                                                         # avoid periodic readout if once set True
                        self._DCfeedActive = self._dbusMon.dbusmon.get_value('com.victronenergy.settings', '/Settings/CGwacs/OvervoltageFeedIn')    # check if DC-feed enabled
                
                self._dbusMon.dbusmon.set_value('com.victronenergy.settings', '/Settings/CGwacs/OvervoltageFeedIn', 0)                      # disable DC-coupled PV feed-in
                MaxChargeVoltage = min((min(chargeVoltageReduced_list)), ChargeVoltageBattery)                                              # avoid exceeding MAX_CELL_VOLTAGE
            
            else:     
                MaxChargeVoltage = ChargeVoltageBattery

                if self._dynamicCVL:
                    self._dynamicCVL = False
                    logging.info('%s: Dynamic CVL reduction finished.'  % (dt.now()).strftime('%c'))
                
                if ((MaxCellVoltage - MinCellVoltage) < CELL_DIFF_MAX) and self._DCfeedActive:                                              # re-enable DC-feed if it was enabled before
                    self._dbusMon.dbusmon.set_value('com.victronenergy.settings', '/Settings/CGwacs/OvervoltageFeedIn', 1)                  # enable DC-coupled PV feed-in
                    logging.info('%s: DC-coupled PV feed-in re-activated.'  % (dt.now()).strftime('%c'))
                    self._DCfeedActive = False                                                                            #reset to prevent permanent logging and activation of  /Settings/CGwacs/OvervoltageFeedIn        
                                                      
            if (MinCellVoltage <= MIN_CELL_VOLTAGE) and ZERO_SOC:
                self._ownCharge = 0                                                                                                         # reset Coulumb counter to 0%                 
            
            # manage charge current
            if NrOfModulesBlockingCharge > 0:
                MaxChargeCurrent = 0
            else:
                MaxChargeCurrent = MAX_CHARGE_CURRENT * self._fn._interpolate(CELL_CHARGE_LIMITING_VOLTAGE, CELL_CHARGE_LIMITED_CURRENT, MaxCellVoltage)

            # manage discharge current
            if MinCellVoltage <= MIN_CELL_VOLTAGE:
                self._fullyDischarged = True           
            elif MinCellVoltage > MIN_CELL_VOLTAGE + MIN_CELL_HYSTERESIS:
                self._fullyDischarged = False       
            
            if (NrOfModulesBlockingDischarge > 0) or (self._fullyDischarged):
                MaxDischargeCurrent = 0
            else:
                MaxDischargeCurrent = MAX_DISCHARGE_CURRENT * self._fn._interpolate(CELL_DISCHARGE_LIMITING_VOLTAGE, CELL_DISCHARGE_LIMITED_CURRENT, MinCellVoltage)      

        ###########################################################
        # my ESS test code here #
        ###########################################################

        AcInPower = self._dbusMon.dbusmon.get_value(self._multi, '/Devices/0/Ac/In/P')
        AcInCurrent = AcInPower / 230 if AcInPower is not None else 0
        
        AcOutPower = self._dbusMon.dbusmon.get_value(self._multi, '/Devices/0/Ac/Out/P')
        AcOutCurrent = AcOutPower / 230 if AcOutPower is not None else 0

        InverterPower = self._dbusMon.dbusmon.get_value(self._multi, '/Devices/0/Ac/Inverter/P')
        InverterCurrent = InverterPower / Voltage if InverterPower is not None else 0
        
        GridSetpoint = self._dbusMon.dbusmon.get_value('com.victronenergy.settings', '/Settings/CGwacs/AcPowerSetPoint')
        MinimumSocLimit = self._dbusMon.dbusmon.get_value('com.victronenergy.settings', '/Settings/CGwacs/BatteryLife/MinimumSocLimit')

        GridPower = self._dbusMon.dbusmon.get_value(self._grid, '/Ac/Power')
        GridL1 = self._dbusMon.dbusmon.get_value(self._grid, '/Ac/L1/Power')
        GridL2 = self._dbusMon.dbusmon.get_value(self._grid, '/Ac/L2/Power')
        GridL3 = self._dbusMon.dbusmon.get_value(self._grid, '/Ac/L3/Power')

        ConsumptionInputL1 = self._dbusMon.dbusmon.get_value('com.victronenergy.system', '/Ac/ConsumptionOnInput/L1/Power')
        ConsumptionInputL2 = self._dbusMon.dbusmon.get_value('com.victronenergy.system', '/Ac/ConsumptionOnInput/L2/Power')
        ConsumptionInputL3 = self._dbusMon.dbusmon.get_value('com.victronenergy.system', '/Ac/ConsumptionOnInput/L3/Power')
        ConsumptionInput = ConsumptionInputL1 + ConsumptionInputL2 + ConsumptionInputL3

        PvOnGridL1 = self._dbusMon.dbusmon.get_value('com.victronenergy.system', '/Ac/PvOnGrid/L1/Power')
        PvOnGridL2 = self._dbusMon.dbusmon.get_value('com.victronenergy.system', '/Ac/PvOnGrid/L2/Power')
        PvOnGridL3 = self._dbusMon.dbusmon.get_value('com.victronenergy.system', '/Ac/PvOnGrid/L3/Power')
        PvOnGrid = PvOnGridL1 + PvOnGridL2 + PvOnGridL3

        AcLoadL1 = GridL1 + PvOnGridL1 - AcInPower
        AcLoadL2 = GridL2 + PvOnGridL2
        AcLoadL3 = GridL3 + PvOnGridL3
        AcLoad = AcLoadL1 + AcLoadL2 + AcLoadL3

        BatteryPower = Power
        BatteryCurrent = Current
        BatteryCurrentCalc = MpptCurrent + InverterCurrent
        MaxChargePower = MaxChargeCurrent * Voltage
        MaxChrgCellVoltage = MaxChargeVoltage / NR_OF_CELLS_PER_BATTERY
        if MaxChargeCurrent > self._MaxChargeCurrentSm:
            self._MaxChargeCurrentSm = ((self._SmoothFilter * self._MaxChargeCurrentSm) + MaxChargeCurrent) / (self._SmoothFilter + 1)
        else:
            self._MaxChargeCurrentSm = MaxChargeCurrent #((self._SmoothFilter * self._MaxChargeCurrentSm) + MaxChargeCurrent) / (self._SmoothFilter + 1)
        MaxChargePowerSmooth = self._MaxChargeCurrentSm * Voltage
        
        CorrectionCurrent = BatteryCurrentCalc - BatteryCurrent
        CorrectionPower = CorrectionCurrent * Voltage

        ###############################################################################
        # ESS magic
        #
        # Calculation of AcPowerSetpoint
        # positive AcPowerSetpoint means MP2 is consuming power from the AC input side
        # negative AcPowerSetpoint means MP2 is sourcing power to the AC input side
        ###############################################################################
        
        # ASP1: compensate AC out power and charge battery with grid and MPPTs using maximum charge power
        ASP1 = AcOutPower - MpptPower + MaxChargePowerSmooth + CorrectionPower
        
        # ASP2: maintain gridsetpoint using PvOnGrid and battery (?)
        ASP2 = GridSetpoint + PvOnGrid - ConsumptionInput
        
        # APS4: maintain gridsepoint and charge battery using PvOnGrid and MPPTs  
        ASP4 = GridSetpoint + PvOnGrid - AcLoad
        
        # ASP5: maintain gridsepoint and charge battery using MPPTs only
        ASP5 = AcOutPower - MpptPower + min(MpptPower,(MaxChargePowerSmooth + CorrectionPower)) 

        # ASP_noDischarge: prohibit battery discharge
        ASP_noDischarge = AcOutPower
        
        if (self._EssActive > 0):
            if (self._EssActive == 1):
                AcPowerSetpoint = ASP1
            elif (self._EssActive == 2):
                AcPowerSetpoint = ASP2
            elif (self._EssActive == 3):
                AcPowerSetpoint = min(ASP1,ASP2)
            elif (self._EssActive == 4):
                #AcPowerSetpoint = min(ASP1,ASP4)
                #if (Soc < MinimumSocLimit):
                ACPowerSetpoint = ASP_noDischarge
            elif (self._EssActive == 5):
                AcPowerSetpoint = min(ASP5,ASP4)
                if (Soc < MinimumSocLimit):
                    ACPowerSetpoint = ASP_noDischarge
            self._dbusMon.dbusmon.set_value(self._multi, '/Hub4/L1/AcPowerSetpoint',AcPowerSetpoint)
        else:
            AcPowerSetpoint = self._dbusMon.dbusmon.get_value(self._multi, '/Hub4/L1/AcPowerSetpoint')

        ###########################################################
        # own Coulomb counter (runs even the BMS values are used) #
        ###########################################################
        
        deltaTime = tt.time() - self._timeOld         
        self._timeOld = tt.time()
        if Current > 0:
            self._ownCharge += Current * (deltaTime / 3600) * BATTERY_EFFICIENCY                # charging (with efficiency)
        else:    
            self._ownCharge += Current * (deltaTime / 3600)                                     # discharging
        self._ownCharge = max(self._ownCharge, 0) 
        self._ownCharge = min(self._ownCharge, InstalledCapacity)
        
        # store the charge into text file if changed significantly (avoid frequent file access)
        if abs(self._ownCharge - self._ownCharge_old) >= (CHARGE_SAVE_PRECISION * InstalledCapacity):
            self._charge_file = open('/data/dbus-aggregate-batteries/charge', 'w')
            self._charge_file.write('%.3f' % self._ownCharge)
            self._charge_file.close()
            self._ownCharge_old = self._ownCharge
   
        # overwrite BMS charge values
        if OWN_SOC:
            Capacity = self._ownCharge
            Soc = 100 * self._ownCharge / InstalledCapacity
            ConsumedAmphours = InstalledCapacity - self._ownCharge
            if (self._dbusMon.dbusmon.get_value('com.victronenergy.system', '/SystemState/LowSoc') == 0) and (Current < 0):
                TimeToGo = -3600 * self._ownCharge / Current
            else: 
                TimeToGo = None
        else:
            Soc = Soc / Capacity                                                            # weighted sum
            if TimeToGo != None:
                TimeToGo = TimeToGo / Capacity                                              # weighted sum
        
        #######################
        # Send values to DBus #
        #######################

        with self._dbusservice as bus:

            # send DC
            bus['/Dc/0/Voltage'] = Voltage #round(Voltage, 2)
            bus['/Dc/0/Current'] = Current #round(Current, 1)
            bus['/Dc/0/Power'] = Power #round(Power, 0)
        
            # send charge
            bus['/Soc'] = Soc
            bus['/TimeToGo'] = TimeToGo
            bus['/Capacity'] = Capacity
            bus['/InstalledCapacity'] = InstalledCapacity
            bus['/ConsumedAmphours'] = ConsumedAmphours
        
            # send temperature
            bus['/Dc/0/Temperature'] = Temperature
            bus['/System/MaxCellTemperature'] = MaxCellTemp
            bus['/System/MinCellTemperature'] = MinCellTemp
        
            # send cell voltages
            bus['/System/MaxCellVoltage'] = MaxCellVoltage
            bus['/System/MaxVoltageCellId'] = MaxVoltageCellId
            bus['/System/MinCellVoltage'] = MinCellVoltage
            bus['/System/MinVoltageCellId'] = MinVoltageCellId
            bus['/Voltages/Sum']= VoltagesSum
            bus['/Voltages/Diff']= round(MaxCellVoltage - MinCellVoltage, 3)    # Marvo2011
            
            if SEND_CELL_VOLTAGES == 1:                                         # Marvo2011
                for cellId,currentCell in enumerate(cellVoltages_dict):  
                    bus['/Voltages/%s' % (re.sub('[^A-Za-z0-9_]+', '', currentCell))] = cellVoltages_dict[currentCell]     
        
            # send battery state
            bus['/System/NrOfCellsPerBattery'] = NR_OF_CELLS_PER_BATTERY
            bus['/System/NrOfModulesOnline'] = NrOfModulesOnline
            bus['/System/NrOfModulesOffline'] = NrOfModulesOffline
            bus['/System/NrOfModulesBlockingCharge'] = NrOfModulesBlockingCharge
            bus['/System/NrOfModulesBlockingDischarge'] = NrOfModulesBlockingDischarge
        
            # send alarms
            bus['/Alarms/LowVoltage'] = LowVoltage_alarm
            bus['/Alarms/HighVoltage'] = HighVoltage_alarm
            bus['/Alarms/LowCellVoltage'] = LowCellVoltage_alarm
            #bus['/Alarms/HighCellVoltage'] = HighCellVoltage_alarm   # not implemended in Venus
            bus['/Alarms/LowSoc'] = LowSoc_alarm
            bus['/Alarms/HighChargeCurrent'] = HighChargeCurrent_alarm
            bus['/Alarms/HighDischargeCurrent'] = HighDischargeCurrent_alarm
            bus['/Alarms/CellImbalance'] = CellImbalance_alarm
            bus['/Alarms/InternalFailure'] = InternalFailure_alarm
            bus['/Alarms/HighChargeTemperature'] = HighChargeTemperature_alarm
            bus['/Alarms/LowChargeTemperature'] = LowChargeTemperature_alarm
            bus['/Alarms/HighTemperature'] = HighTemperature_alarm
            bus['/Alarms/LowTemperature'] = LowTemperature_alarm
            bus['/Alarms/BmsCable'] = BmsCable_alarm
        
            # send charge/discharge control
            
            bus['/Info/MaxChargeCurrent'] = MaxChargeCurrent
            bus['/Info/MaxDischargeCurrent'] = MaxDischargeCurrent
            bus['/Info/MaxChargeVoltage'] = MaxChargeVoltage
            
            '''
            # Not working, Serial Battery disapears regardles BLOCK_ON_DISCONNECT is True or False
            if BmsCable_alarm == 0: 
                bus['/Info/MaxChargeCurrent'] = MaxChargeCurrent
                bus['/Info/MaxDischargeCurrent'] = MaxDischargeCurrent
                bus['/Info/MaxChargeVoltage'] = MaxChargeVoltage
            else:                                                       # if BMS connection lost
                bus['/Info/MaxChargeCurrent'] = 0
                bus['/Info/MaxDischargeCurrent'] = 0
                bus['/Info/MaxChargeVoltage'] = NR_OF_CELLS_PER_BATTERY * min(CHARGE_VOLTAGE_LIST)
                logging.error('%s: BMS connection lost.' % (dt.now()).strftime('%c'))
            '''    
            
            # ess stuff
            bus['/Ess/BatteryP'] = round(BatteryPower,0)
            bus['/Ess/BatteryI'] = round(BatteryCurrent,0)
            bus['/Ess/BatteryCalcI'] = round(BatteryCurrentCalc,2)
            bus['/Ess/MpptP'] = round(MpptPower,0)
            bus['/Ess/MpptI'] = round(MpptCurrent, 2)
            bus['/Ess/AcInP'] = round(AcInPower,0) if AcInPower is not None else 0
            bus['/Ess/AcInI'] = round(AcInCurrent,2) 
            bus['/Ess/AcOutP'] = round(AcOutPower,0) if AcOutPower is not None else 0
            bus['/Ess/AcOutI'] = round(AcOutCurrent,2)
            bus['/Ess/InverterP'] = round(InverterPower,0) if InverterPower is not None else 0
            bus['/Ess/InverterI'] = round(InverterCurrent,2)
            bus['/Ess/MaxChargeP'] = round(MaxChargePower,0)
            bus['/Ess/MaxChargeI'] = round(MaxChargeCurrent,2)
            bus['/Ess/MaxChargeIsm'] = round(self._MaxChargeCurrentSm,2)
            bus['/Ess/GridSetpoint'] = round(GridSetpoint,0) if GridSetpoint is not None else -1
            bus['/Ess/GridP'] = round(GridPower,0)    
            bus['/Ess/AcPowerSetpoint'] = round(AcPowerSetpoint,0) if AcPowerSetpoint is not None else -1  
            bus['/Ess/MaxChrgCellVoltage'] = round(MaxChrgCellVoltage,3)
            bus['/Ess/ConsumptionInputL1'] = round(ConsumptionInputL1,1) if ConsumptionInputL1 is not None else -1  
            bus['/Ess/ConsumptionInputL2'] = round(ConsumptionInputL2,1) if ConsumptionInputL2 is not None else -1 
            bus['/Ess/ConsumptionInputL3'] = round(ConsumptionInputL3,1) if ConsumptionInputL3 is not None else -1 
            bus['/Ess/ConsumptionInput'] = round(ConsumptionInput,1)
            bus['/Ess/PvOnGridL1'] = round(PvOnGridL1,1) if PvOnGridL1 is not None else -1  
            bus['/Ess/PvOnGridL2'] = round(PvOnGridL2,1) if PvOnGridL2 is not None else -1 
            bus['/Ess/PvOnGridL3'] = round(PvOnGridL3,1) if PvOnGridL3 is not None else -1 
            bus['/Ess/PvOnGrid'] = round(PvOnGrid,1)
            bus['/Ess/AcLoadL1'] = round(AcLoadL1,1) if AcLoadL1 is not None else -1  
            bus['/Ess/AcLoadL2'] = round(AcLoadL2,1) if AcLoadL2 is not None else -1 
            bus['/Ess/AcLoadL3'] = round(AcLoadL3,1) if AcLoadL3 is not None else -1 
            bus['/Ess/AcLoad'] = round(AcLoad,1)
            bus['/Ess/CorrectionI'] = round(CorrectionCurrent,3)
            bus['/Ess/MinimumSocLimit'] = MinimumSocLimit

            # this does not control the charger, is only displayed in GUI
            bus['/Io/AllowToCharge'] = AllowToCharge
            bus['/Io/AllowToDischarge'] = AllowToDischarge
            bus['/Io/AllowToBalance'] = AllowToBalance

        ###########################################################
        ################# Periodic logging ########################
        ###########################################################
        
        if LOG_PERIOD > 0:
            if self._logTimer < LOG_PERIOD:
                self._logTimer += 1
            else:
                self._logTimer = 0
                logging.info('%s: Repetitive logging:' % dt.now().strftime('%c'))
                logging.info('  CVL: %.1fV, CCL: %.0fA, DCL: %.0fA'  % (MaxChargeVoltage, MaxChargeCurrent, MaxDischargeCurrent))
                logging.info('  Bat. voltage: %.1fV, Bat. current: %.0fA, SoC: %.1f%%, Balancing state: %d'  % (Voltage, Current, Soc, self._balancing))
                logging.info('  Min. cell voltage: %s: %.3fV, Max. cell voltage: %s: %.3fV, difference: %.3fV'  % (MinVoltageCellId, MinCellVoltage, MaxVoltageCellId, MaxCellVoltage, MaxCellVoltage - MinCellVoltage)) 

        return True
            
#################
#################  
### Main loop ###
#################
#################

def main():

    if LOGGING == 1:    # print to console
        logging.basicConfig(level=logging.INFO)        
    elif LOGGING == 2:  # print to file   
        logging.basicConfig(filename = '/data/dbus-aggregate-batteries/aggregatebatteries.log', level=logging.INFO)
    
    logging.info('%s: Starting AggregateBatteries.' % (dt.now()).strftime('%c'))
    from dbus.mainloop.glib import DBusGMainLoop
    DBusGMainLoop(set_as_default=True)
    aggbat = DbusAggBatService()
    logging.info('%s: Connected to DBus, and switching over to GLib.MainLoop()' % (dt.now()).strftime('%c'))
    mainloop = GLib.MainLoop()
    mainloop.run()

if __name__ == "__main__":
    main()

