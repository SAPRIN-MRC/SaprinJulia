use "#datafile#", clear

compress

label data "#node# Household Socio-economic data V#version#"

* Label variables
label var HouseholdId "Unique household identifier"
label var ObservationDate "Observation date"
label var WaterSource "Source of drinking water"
label var Toilet "Toilet type"
label var ConnectedToGrid "Connected to electricity grid"
label var CookingFuel "Type of fuel used for cooking"
label var WallMaterial "Building material for walls"
label var FloorMaterial "Building mmaterial used for floors"
label var Bedrooms "Number of bedrooms"
label var Crime "Victim of crime"
label var FinancialStatus "Self reported financial status"
label var CutMeals "Reduced food intake"
label var CutMealsFrequency "Reduced food intake frequency"
label var NotEat "Skipped meals"
label var NotEatFrequency "Skipped meals frequency"
label var ChildMealSkipCut "Children skipped meals"
label var ChildMealSkipCutFrequency "Children skipped meals frequency"
label var ConsentToCall "Consented to telephone surveillance"

* Define value labels
label define lblWaterSource 0 "Refused/Missing", add
label define lblWaterSource 1 "Piped (stand/house)", add
label define lblWaterSource 2 "Piped (communal)", add
label define lblWaterSource 3 "Borehole", add
label define lblWaterSource 4 "Rainwater", add
label define lblWaterSource 5 "Flowing river", add
label define lblWaterSource 6 "Dam/standing", add
label define lblWaterSource 7 "Protected spring", add
label define lblWaterSource 8 "Tanker", add
label define lblWaterSource 10 "Other purified", add
label define lblWaterSource 11 "Other unourified", add

label define lblToilet 0 "Missing/Refused", add
label define lblToilet 1 "Flush", add
label define lblToilet 2 "VIP", add
label define lblToilet 3 "Other Pit", add
label define lblToilet 4 "Bucket", add
label define lblToilet 5 "Chemical", add

label define lblYesNo 0 "No", add
label define lblYesNo 1 "Yes", add

label define lblFuel 0 "Missing/Refused", add
label define lblFuel 1 "Wood", add
label define lblFuel 2 "Gas", add
label define lblFuel 3 "Coal", add
label define lblFuel 4 "Electricity", add
label define lblFuel 5 "Paraffin", add
label define lblFuel 6 "Other", add

label define lblMaterial 0 "Missing/Refused", add
label define lblMaterial 1 "Brick", add
label define lblMaterial 2 "Cement", add
label define lblMaterial 3 "Other modern material", add
label define lblMaterial 4 "Stabilised mud", add
label define lblMaterial 5 "Traditional mud", add
label define lblMaterial 6 "Wood", add
label define lblMaterial 7 "Other informal structure", add
label define lblMaterial 8 "Tiles", add
label define lblMaterial 9 "Modern carpet", add
label define lblMaterial 10 "Other modern material", add
label define lblMaterial 11 "Dirt", add
label define lblMaterial 12 "Mat", add
label define lblMaterial 13 "Other traditional", add

label define lblCrime 0 "Missing/Refused", add
label define lblCrime 1 "None", add
label define lblCrime 2 "Theft", add
label define lblCrime 4 "Assault", add
label define lblCrime 6 "Theft & Assault", add
label define lblCrime 8 "Murder", add
label define lblCrime 10 "Theft & Murder", add
label define lblCrime 12 "Assault & Murder", add
label define lblCrime 14 "Theft, Assault & Murder", add
label define lblCrime 16 "Other crime", add
label define lblCrime 18 "Theft & Other", add
label define lblCrime 20 "Assault & Other", add
label define lblCrime 22 "Theft, Assault & Other", add
label define lblCrime 24 "Murder & Other", add
label define lblCrime 26 "Theft, Murder & Other", add
label define lblCrime 28 "Assault, Murder & Other", add
label define lblCrime 30 "Theft, Assault, Murder & Other", add

label define lblFinStatus 0 "Missing/Refused", add
label define lblFinStatus 1 "Extremely Poor", add
label define lblFinStatus 2 "Poor", add
label define lblFinStatus 3 "Just getting by", add
label define lblFinStatus 4 "Comfortable", add
label define lblFinStatus 5 "Very Comfortable", add

label define lblFrequency 0	"Missing/Refused/NAD", add
label define lblFrequency 1	"Almost every month", add
label define lblFrequency 2	"Some months not all", add
label define lblFrequency 3	"Only once or twice", add
label define lblFrequency 4	"None", add

*Attach value labels
label values WaterSource lblWaterSource
label values Toilet lblToilet
label values ConnectedToGrid lblYesNo
label values CookingFuel lblFuel
label values WallMaterial lblMaterial
label values FloorMaterial lblMaterial
label values Crime lblCrime
label values FinancialStatus lblFinStatus
label values CutMeals lblYesNo
label values NotEat lblYesNo
label values ChildMealSkipCut lblYesNo
label values ConsentToCall lblYesNo
label values CutMealsFrequency lblFrequency
label values NotEatFrequency lblFrequency
label values ChildMealSkipCutFrequency lblFrequency

*Save file
saveold "#datafile#", replace version(13)