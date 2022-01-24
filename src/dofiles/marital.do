use "#datafile#", clear

compress

label data "#node# Marital Status data V#version#"


label var IndividualId "Individual ID"
label var ObservationDate "Observation Date"
label var MaritalStatus "Marital Status"


*Define value labels
*Marital Status
cap label drop 

* Current Education 
label define lblMaritalStatus 0  "Missing/Refused",add
label define lblMaritalStatus 1  "Never married",add
label define lblMaritalStatus 2  "Married",add
label define lblMaritalStatus 3  "Polygamous Marriage",add
label define lblMaritalStatus 4  "Divorced/Separated",add
label define lblMaritalStatus 5  "Widowed",add

* Attach value labels

label values MaritalStatus lblMaritalStatus

saveold "#datafile#", replace version(13)


	

	


	
	