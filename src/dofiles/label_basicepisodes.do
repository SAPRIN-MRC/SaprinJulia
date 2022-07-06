use "#datafile#", clear
compress
label data "#node# Basic Exposure Episodes V#version#"
label var IndividualId "Unique individual identifier"
label var Sex "Sex of individual"
label var DoB "Date of birth"
label var DoD "Date of death"
label var MotherId "Mother's IndividualId"
label var FatherId "Father's IndividualId"
label var Episode "This episode number (first=1, last=Episodes)"
label var Episodes "Total number of episodes for individual"
label var StartDate "Start date of episode (inclusive)"
label var EndDate "End date of episode (inclusive)"
label var Days "Duration in days of episode"
label var LocationId "Where individual was resident, household residence if non-resident"
label var Resident "Whether individual is resident for duration of episode"
label var HouseholdId "Unique household identifier of the household the individual is a member of"
label var Enumeration "Episode starts with an enumeration"
label var Born "Episode starts with the birth of the individual"
label var InMigration "Episode starts with an in-migration"
label var LocationEntry "Episode starts with an internal migration"
label var Died "Episode ends with the death of the individual"
label var OutMigration "Episode ends with the out-migration"
label var LocationExit "Episode ends with an internal migration"
label var LostToFollowUp "Individual was lost to follow-up at the end of the episode"
label var Current "Individual still under surveillance"
label var Refusal "Individual refused follow-up"
label var ExtResStart "Flag to indicate start of external residence"
label var ExtResEnd "Flag to indicate end of external residence"
label var MembershipStart "Flag to indicate start of household membership"
label var MembershipEnd "Flag to indicate end of household membership"
label var Memberships "Number of concurrent household memberships"
label var Participation "Resume participation after refusal"
label var Gap "Gap in individual exposure record"
order IndividualId Sex DoB DoD MotherId FatherId ///
      StartDate EndDate Episodes Episode LocationId HouseholdId Resident ///
      Enumeration Born InMigration LocationEntry ExtResStart Participation ///
      Died OutMigration LocationExit ExtResEnd LostToFollowUp Refusal ///
      MembershipStart MembershipEnd Memberships Gap
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
label define lblMaritalStatus 0 "Unknown", add
label define lblMaritalStatus 1 "Not married", add
label define lblMaritalStatus 2 "Married", add
label define lblMaritalStatus 3 "Widowed", add
label define lblMaritalStatus 4 "Separated/divorced", add
label define lblMaritalStatus 5 "Informal union", add
label values Enumeration lblYesNo
label values Resident lblYesNo
label values Sex lblSex
label values Born lblYesNo
label values InMigration lblYesNo
label values LocationEntry lblYesNo
label values Died lblYesNo
label values OutMigration lblYesNo
label values LocationExit lblYesNo
label values LostToFollowUp blYesNo
label values Refusal lblYesNo
label values Current lblYesNo
label values ExtResStart lblYesNo
label values ExtResEnd lblYesNo
label values MembershipStart lblYesNo
label values MembershipEnd lblYesNo
label define lblHIVStatus 0  "Unknown"
label define lblHIVStatus 1 "Pre-negative", add
label define lblHIVStatus 2 "Negative", add
label define lblHIVStatus 3 "Post-negative", add
label define lblHIVStatus 4 "Pre-positive", add
label define lblHIVStatus 5 "Positive", add
label define lblHIVStatus 9 "Error", add
label define lblOnART 1 "On ART"
label define lblOnART 2 "Presumed on ART", add
label define lblOnART 3 "ART Interupted", add
label define lblEmployment 0 "Unknown"
label define lblEmployment 1 "Employed", add
label define lblEmployment 2 "Part-time employment", add
label define lblEmployment 10 "Household duties/care", add
label define lblEmployment 11 "Household duties/care", add
label define lblEmployment 12 "Job seeking", add
label define lblEmployment 13 "Education", add
label define lblEmployment 14 "Disabled", add
label define lblEmployment 100 "Unknown", add
label define lblEmployment 101 "Agriculture", add
label define lblEmployment 102 "Mining", add
label define lblEmployment 103 "Manufacturing", add
label define lblEmployment 104 "Utilities", add
label define lblEmployment 105 "Construction", add
label define lblEmployment 106 "Wholesale/retail", add
label define lblEmployment 107 "Hospitality/Sport", add
label define lblEmployment 108 "Transport/Communication", add
label define lblEmployment 109 "Finance", add
label define lblEmployment 110 "Education", add
label define lblEmployment 111 "Health", add
label define lblEmployment 112 "Legal", add
label define lblEmployment 113 "Research", add
label define lblEmployment 114 "Domestic", add
label define lblEmployment 115 "Armed forces", add
label define lblEmployment 116 "Informal sector", add
label define lblEmployment 200 "Unknown", add
label define lblEmployment 201 "Employee", add
label define lblEmployment 202 "Self-employed", add
label define lblEmployment 203 "Informally employed", add
label define lblEmployment 300 "Unknown", add
label define lblEmployment 301 "National Government", add
label define lblEmployment 302 "Provincial Government", add
label define lblEmployment 303 "Local Government", add
label define lblEmployment 304 "Public Corporation", add
label define lblEmployment 305 "Private sector employer", add
label define lblEmployment 306 "Non-profit", add
label define lblEmployment 307 "Self-employment", add
label define lblEmployment 308 "Household", add
label values Gap lblYesNo
label values LostToFollowUp lblYesNo
label values Participation lblYesNo
saveold "#datafile#", replace version(13)