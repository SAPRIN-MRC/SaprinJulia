use "#datafile#", clear
compress
label data "SAPRIN #node# Individual Exposure Episodes V#version#"
label var NodeId "SAPRIN Node identifier"
label var IndividualId "Unique individual identifier"
label var DoB "Date of birth"
label var DoD "Date of death"
label var CalendarYear "The calendar year in which the episode false"
label var Age "The age in completed years of the individual"
label var Sex "Sex of individual"
label var LocationId "Where individual was resident, household residence if non-resident"
label var HouseholdId "Unique household identifier of the household the individual is a member of"
label var HHRelation "Relationship to head of household at start of episode"
label var IsUrbanOrRural "Settlement pattern at location"
label var MotherId "Mother's IndividualId"
label var FatherId "Father's IndividualId"
label var SpouseId "IndividualId of spouse - not available"
label var StartDate "Start date of episode (inclusive)"
label var EndDate "End date of episode (inclusive)"
label var StartType "What triggered this episode start?"
label var EndType "How did this episode end?"
label var Episode "This episode number (first=1, last=Episodes)"
label var Episodes "Total number of episodes for individual"
label var Resident "Whether individual is resident for duration of episode"
label var MotherStatus "Status of mother for duration of episode"
label var FatherStatus "Status of father for duration of episode"
label var ChildrenEverBorn "Number of children ever born to individual"
label define lblSex 1 "Male", add
label define lblSex 2 "Female", add
label define lblSex 3 "Unknown", add
label define lblYesNo -1 "Unknown/Missing", add
label define lblYesNo 0 "No", add
label define lblYesNo 1 "Yes", add
label define lblHHRelation 1 "Head"
label define lblHHRelation 2 "Spouse", add
label define lblHHRelation 3 "Child", add
label define lblHHRelation 4 "Son/Daughter-inlaw", add
label define lblHHRelation 5 "Grandchild", add
label define lblHHRelation 6 "Parent", add
label define lblHHRelation 7 "Parent-inlaw", add
label define lblHHRelation 8 "Grandparent", add
label define lblHHRelation 9 "Sibling", add
label define lblHHRelation 10 "Other relative", add
label define lblHHRelation 11 "Domestic worker/tenant", add
label define lblHHRelation 12 "Unrelated/Other", add
label define lblParentStatus 0 "Unknown"
label define lblParentStatus 1 "Coresident", add
label define lblParentStatus 2 "Alive", add
label define lblParentStatus 3 "Dead", add
label define lblStart 1 "Born"
label define lblStart 2 "Enumeration", add
label define lblStart 3 "Residency start", add
label define lblStart 4 "External residency start", add
label define lblStart 5 "Participation restart", add
label define lblStart 6 "Attribute change", add
label define lblEnd 1 "Died"
label define lblEnd 2 "Residency end", add
label define lblEnd 3 "External residency end", add
label define lblEnd 4 "Refusal", add
label define lblEnd 5 "Lost to follow-up", add
label define lblEnd 6 "Attribute change", add
label define lblEnd 7 "Current", add
label define lblSettlement 1 "Rural"
label define lblSettlement 2 "Urban", add
label define lblSettlement 3 "Peri-urban", add
label define lblIsUrbanOrRural 0 "Unknown"
label define lblIsUrbanOrRural 1 "Rural", add
label define lblIsUrbanOrRural 2 "Urban", add
label define lblIsUrbanOrRural 3 "Peri-urban", add
label values StartType lblStart
label values EndType lblEnd
label values Resident lblYesNo
label values Sex lblSex
label values HHRelation lblHHRelation
label values MotherStatus lblParentStatus
label values FatherStatus lblParentStatus
label values IsUrbanOrRural lblIsUrbanOrRural
saveold "#datafile#", replace version(13)