*Date 25-04-2022

use "#datafile#", clear

compress

label data "#node# Labour Status data V#version#"


label var IndividualId "Individual ID"
label var ObservationDate "Observation Date"
label var CurrentEmployment "Current Employment"
label var EmploymentSector "Employment Sector"
label var EmploymentType "Employment Type"
label var Employer  "Employer"

*Define value labels
*Employment Status
cap label drop lblEmploymentStatus
cap label drop lblEmploymentSector
cap label drop lblEmploymentType
cap label drop lblEmployers

  
label define lblEmploymentStatus 0	"Unknown employment status",add
label define lblEmploymentStatus 1	"Currently employed", add
label define lblEmploymentStatus 2	"Currently part-time employed", add
label define lblEmploymentStatus 3	"Pensioner", add
label define lblEmploymentStatus 4	"Not looking for work", add
label define lblEmploymentStatus 5	"Other", add	
label define lblEmploymentStatus 10	"Unemployed due to caring for others/household duties", add
label define lblEmploymentStatus 11	"Unemployed due to caring for others/household duties", add
label define lblEmploymentStatus 12	"Unemployed and looking for a job", add	
label define lblEmploymentStatus 13	"Unemployed but in school or undergoing other education/training", add	
label define lblEmploymentStatus 14	"Unemployed and unable to work due to illness/disability", add	 
label define lblEmploymentStatus 15	"Not currently in employment", add	
label define lblEmploymentStatus 202 "Work for themselves", add

*Employment Sector
label define lblEmploymentSector   0	"Unknown employment sector", add
label define lblEmploymentSector 100	"Unknown employment sector", add
label define lblEmploymentSector 101	"Agriculture, Fishing, or Forestry", add
label define lblEmploymentSector 102	"Mining", add	
label define lblEmploymentSector 103	"Manufacturing", add
label define lblEmploymentSector 104	"Electricity and water", add	
label define lblEmploymentSector 105	"Construction", add
label define lblEmploymentSector 106	"Wholesale/retail", add
label define lblEmploymentSector 107	"Restaurant/Hotels/Tourism/Sport", add
label define lblEmploymentSector 108	"Transport and communication", add
label define lblEmploymentSector 109	"Financial services", add
label define lblEmploymentSector 110	"Educational services", add
label define lblEmploymentSector 111	"Health services", add
label define lblEmploymentSector 112	"Legal services", add
label define lblEmploymentSector 113	"Research", add
label define lblEmploymentSector 114	"Domestic services", add
label define lblEmploymentSector 115	"Armed forces and police", add	
label define lblEmploymentSector 116	"Informal sector, e.g. street vendor", add	
label define lblEmploymentSector 301    "National/Central government", add
label define lblEmploymentSector 305    "Private sector employer", add	
label define lblEmploymentSector 306    "Non-profit institution", add


*Employment Type

label define lblEmploymentType 	  0	"Unknown employment type", add
label define lblEmploymentType 	200	"Unknown employment type", add	
label define lblEmploymentType  201	"Works as employee", add
label define lblEmploymentType  202	"Work for themselves", add	
label define lblEmploymentType  203	"Do odd jobs/piece jobs", add	


*Employers

label define lblEmployers   0	"Unknown Employer", add
label define lblEmployers 201	"Works as employee", add
label define lblEmployers 300	"Unknown Employer", add
label define lblEmployers 301	"National/Central government", add	
label define lblEmployers 302	"Provincial government", add
label define lblEmployers 303	"Local / district authority or municipality", add	
label define lblEmployers 304	"Public Corporation", add
label define lblEmployers 305	"Private sector employer", add	
label define lblEmployers 306	"Non-profit institution", add	
label define lblEmployers 307	"Self-employment", add
label define lblEmployers 308	"Another household member", add

* Attach value labels

label values CurrentEmployment lblEmploymentStatus 
label values EmploymentSector  lblEmploymentSector
label values EmploymentType    lblEmploymentType
label values Employer          lblEmployers

saveold "#datafile#", replace version(13)





	

	

	


	
	