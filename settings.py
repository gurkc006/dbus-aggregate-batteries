# Version 1.0

#######################################
########## Hardware settings ##########
#######################################

NR_OF_BATTERIES = 1                                     # Nr. of physical batteries to be aggregated. Smart shunt for battery current is not needed and not supported. 
NR_OF_CELLS_PER_BATTERY = 16
NR_OF_MPPTS = 4                                         # Nr. of MPPTs
DC_LOADS = False                                        # If DC loads with Smart Shunt present, can be used for total current measurement
INVERT_SMARTSHUNT = False                               # False: Current subtracted, True: Current added 

#######################################
############ DBus settings ############
#######################################

BATTERY_SERVICE_NAME = 'com.victronenergy.battery'      # Key world to identify services of physical Serial Batteries (and SmartShunt if available)                
BATTERY_PRODUCT_NAME_PATH = '/ProductName'              # Path of Battery Product Name
BATTERY_PRODUCT_NAME = 'SerialBattery'                  # Key world to identify the batteries (to exclude SmartShunt)
BATTERY_INSTANCE_NAME_PATH = '/CustomName'              # if CustomName doesn't exist, set '/ProductName'
MULTI_KEY_WORD = 'com.victronenergy.vebus'              # Key world to identify service of Multis/Quattros (or cluster of them)
GRID_KEY_WORD = 'com.victronenergy.grid'                # Key world to identify service of grid meter
MPPT_KEY_WORD = 'com.victronenergy.solarcharger'        # Key world to identify services of solar chargers
SMARTSHUNT_NAME_KEY_WORD = 'SmartShunt'                 # Key world to identify services of SmartShunt

SEARCH_TRIALS = 10                                      # Trials to identify of all batteries before exit and restart
READ_TRIALS = 10                                        # Trials to get consistent data of all batteries before exit and restart

#######################################
############## Options ################
#######################################

CURRENT_FROM_VICTRON = True                             # If True, the current measurement by Multis/Quattros and MPPTs is taken instead of BMS.
                                                        # Necessary for JK BMS due to poor precision.
OWN_SOC = True                                          # If True, the self calculated charge indicators are taken instead of BMS
ZERO_SOC = False                                        # Allow zeroing charge counter at MIN_CELL_VOLTAGE. At full battery it is always set to 100%. 
CHARGE_SAVE_PRECISION = 0.0025                          # Trade-off between save precision and file access frequency
UPDATE_INTV = 500

#######################################
##### Charge/Discharge parameters #####
#######################################

# Please note: Victron ESS disables CCL if DC-coupled PV feed-in is active. This program disables the DC-coupled PV feed-in when necessary.

OWN_CHARGE_PARAMETERS = False                           # Calculate own charge/discharge control parameters (True) from following settings
                                                        # or use them from battery driver (False)
BALANCING_VOLTAGE = 3.45                                # This voltage per cell will be set periodically and kept until balancing below CELL_DIFF_MAX and next charge cycle
BALANCING_REPETITION = 1                                # in days                      

CHARGE_VOLTAGE_LIST = [3.45, 3.45, 3.45, 3.45, 3.45, 3.45, 3.45, 3.45, 3.45, 3.45, 3.45, 3.45]      # Set up how full the battery has to be charged in given month
#                       jan,  feb,  mar,  apr,  may,  jun,  jul,  aug,  sep,  oct,  nov,  dec

MAX_CELL_VOLTAGE = 3.5                                  # If reached by 1-st cell, the CVL is dynamically limited. DC-coupled PV feed-in will be disabled to enable the CCL. 
MIN_CELL_VOLTAGE = 2.9                                  # If reached, discharge current set to zero
MIN_CELL_HYSTERESIS = 0.1                               # Allow discharge above MIN_CELL_VOLTAGE + MIN_CELL_HYSTERESIS
CELL_DIFF_MAX = 0.015                                   # If lower: re-enable DC-coupled PV feed in (if was disabled by dynamic CVL reduction and was enabled before); go back from BALANCING_VOLTAGE to CHARGE_VOLTAGE
BATTERY_EFFICIENCY = 1.00                               # Ah fed into batteries are multiplied by efficiency 

MAX_CHARGE_CURRENT = 150                                # Max. charge current at normal conditions
MAX_DISCHARGE_CURRENT = 100                             # Max. discharge current at normal conditions

# settings limiting charge and discharge current if at least one cell gets full or empty
# the lists may have any length, but the same length for voltage and current
# linear interpolation is used for values between

CELL_CHARGE_LIMITING_VOLTAGE = [MIN_CELL_VOLTAGE, MIN_CELL_VOLTAGE + 0.05, BALANCING_VOLTAGE - 0.1, BALANCING_VOLTAGE, MAX_CELL_VOLTAGE] # [min, ... ,max]; low voltage: limiting current from grid
CELL_CHARGE_LIMITED_CURRENT =  [0.1, 1, 1, 0.05, 0]
CELL_DISCHARGE_LIMITING_VOLTAGE = [MIN_CELL_VOLTAGE, MIN_CELL_VOLTAGE + 0.1, MIN_CELL_VOLTAGE + 0.2]    # [min, ... ,max]
CELL_DISCHARGE_LIMITED_CURRENT =  [0, 0.05, 1]

########################################
### if OWN_CHARGE_PARAMETERS = False ###
########################################

# If "False", the transmitted CVL is always the minimum of all batteries
# If "True", the transmitted CVL is the maximum of all batteries until all are in "float",
# than the minimum of all is taken. Attention: By using this function you rely on the functionality 
# and correct settings of the SerialBattery driver. Set "True" only if you know exactly
# what you are doing. If not sure, keep it at "False". 

KEEP_MAX_CVL = False

#######################################
####### Logging and reporting #########
#######################################

SEND_CELL_VOLTAGES = 0                                 # 0: Disable Cell Info in dbus, 1: Format: /Cell/BatteryName_Cell<ID>
LOGGING = 1                                            # 0: no logging, 1: print to console, 2: print to file
LOG_PERIOD = 0                                         # in seconds; if 0, periodic logging is disabled
