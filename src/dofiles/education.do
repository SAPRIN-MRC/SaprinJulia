use "#datafile#", clear

compress

label data "#node# Education Statuses data V#version#"


label var IndividualId "Individual ID"
label var ObservationDate "Observation Date"
label var CurrentEducation "Current Education"
label var HighestSchoolLevel "Highest School Level"
label var HighestNonSchoolLevel "Highest Non-School Level"

*Define value labels
*Employment Status
cap label drop lblCurrentEducation
cap label drop lblHighestSchoolLevel
cap label drop lblHighestNonSchoolLevel


* Current Education 
label define lblCurrentEducation -1	"Undefined",add
label define lblCurrentEducation 1	"Part time", add
label define lblCurrentEducation 2	"Full time", add
label define lblCurrentEducation 3	"No", add
label define lblCurrentEducation 5	"Refused", add
label define lblCurrentEducation 6	"Unknown", add	

* Highest School Level
label define lblHighestSchoolLevel  -1	"Unknown", add
label define lblHighestSchoolLevel  0	"Grade 0", add
label define lblHighestSchoolLevel  1	"Grade 1", add
label define lblHighestSchoolLevel  2	"Grade 2", add
label define lblHighestSchoolLevel  3	"Grade 3", add
label define lblHighestSchoolLevel  4	"Grade 4", add
label define lblHighestSchoolLevel  5	"Grade 5", add
label define lblHighestSchoolLevel  6	"Grade 6", add
label define lblHighestSchoolLevel  7	"Grade 7", add
label define lblHighestSchoolLevel  8	"Grade 8", add
label define lblHighestSchoolLevel  9	"Grade 9", add
label define lblHighestSchoolLevel  10	"Grade 10", add
label define lblHighestSchoolLevel  11	"Grade 11", add
label define lblHighestSchoolLevel  12	"Grade 12", add
label define lblHighestSchoolLevel  13	"Special School", add
label define lblHighestSchoolLevel  21	"Higher Certificate", add
label define lblHighestSchoolLevel  22	"National Diploma", add
label define lblHighestSchoolLevel  23	"Degree", add
label define lblHighestSchoolLevel  24	"Honours", add
label define lblHighestSchoolLevel  25	"Masters", add
label define lblHighestSchoolLevel  26	"Doctors", add
label define lblHighestSchoolLevel  31	"ABET 1", add
label define lblHighestSchoolLevel  32	"ABET 2", add
label define lblHighestSchoolLevel  33	"ABET 3", add
label define lblHighestSchoolLevel  34	"ABET 4", add
label define lblHighestSchoolLevel  41	"FET 1", add
label define lblHighestSchoolLevel  42	"FET 2", add
label define lblHighestSchoolLevel  43	"FET 3", add
label define lblHighestSchoolLevel  50	"LT 1 year", add
label define lblHighestSchoolLevel  51	"Creche", add
label define lblHighestSchoolLevel  96	"Refused", add
label define lblHighestSchoolLevel  97	"NAD", add
label define lblHighestSchoolLevel  98	"No education", add
label define lblHighestSchoolLevel  99	"Not attending", add


* Highest Non-School Level

label define lblHighestNonSchoolLevel  -1  "Unknown", add
label define lblHighestNonSchoolLevel  21  "Higher Certificate", add
label define lblHighestNonSchoolLevel  22  "National Diploma", add
label define lblHighestNonSchoolLevel  23  "Degree", add
label define lblHighestNonSchoolLevel  24  "Honours", add
label define lblHighestNonSchoolLevel  25  "Masters", add
label define lblHighestNonSchoolLevel  26  "Doctors", add
label define lblHighestNonSchoolLevel  31  "ABET 1", add
label define lblHighestNonSchoolLevel  32  "ABET 2", add
label define lblHighestNonSchoolLevel  33  "ABET 3", add
label define lblHighestNonSchoolLevel  34  "ABET 4", add
label define lblHighestNonSchoolLevel  41  "FET 1", add
label define lblHighestNonSchoolLevel  42  "FET 2", add
label define lblHighestNonSchoolLevel  43  "FET 3", add
label define lblHighestNonSchoolLevel  96  "Refused", add
label define lblHighestNonSchoolLevel  97  "NAD", add
label define lblHighestNonSchoolLevel  98  "No education", add
label define lblHighestNonSchoolLevel  99  "Not attending", add


* Attach value labels

label values CurrentEducation lblCurrentEducation
label values HighestSchoolLevel  lblHighestSchoolLevel
label values HighestNonSchoolLevel   lblHighestNonSchoolLevel

saveold "#datafile#", replace version(13)


	

	


	
	