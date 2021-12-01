use "#datafile#", clear

compress

label data "#node# Household Assets V#version#"

* Label variables
label var HouseholdId "Unique household identifier"
label var ObservationDate "Observation date"
label var AssetId "Asset kind"
label var AssetStatusId "Asset status"

* Define value labels
label define lblAssetStatus 0 "Refused/Missing", add
label define lblAssetStatus 1 "Present", add
label define lblAssetStatus 2 "Absent", add

label define lblAsset 1 "Telephone", add
label define lblAsset 2 "Cellphone", add
label define lblAsset 3 "PrimusCooker", add
label define lblAsset 4 "HotPlate", add
label define lblAsset 5 "Stove", add
label define lblAsset 6 "GasCooker", add
label define lblAsset 7 "Fridge", add
label define lblAsset 8 "Kettle", add
label define lblAsset 9 "Television", add
label define lblAsset 10 "DVD", add
label define lblAsset 11 "Radio", add
label define lblAsset 12 "SewingMachine", add
label define lblAsset 13 "BlockMaker", add
label define lblAsset 14 "Car", add
label define lblAsset 15 "Motorcycle", add
label define lblAsset 16 "Bicycle", add
label define lblAsset 17 "Microbus", add
label define lblAsset 19 "Bed", add
label define lblAsset 20 "Table", add
label define lblAsset 21 "Sofa", add
label define lblAsset 22 "KitchenSink", add
label define lblAsset 23 "SolarPanel", add
label define lblAsset 24 "Wheelbarrow", add
label define lblAsset 25 "Spade", add
label define lblAsset 26 "BedNets", add
label define lblAsset 27 "Cattle", add
label define lblAsset 28 "OtherLivestock", add
label define lblAsset 100 "Cart", add
label define lblAsset 101 "Chicken", add
label define lblAsset 102 "DSTV", add
label define lblAsset 103 "ElectricIron", add
label define lblAsset 104 "ElectricLights", add
label define lblAsset 105 "Goats", add
label define lblAsset 106 "Heater", add
label define lblAsset 107 "Microwave", add
label define lblAsset 108 "Sheep", add

*Attach value labels
label values AssetId lblAsset
label values AssetStatusId lblAssetStatus

*Save file
saveold "#datafile#", replace version(13)