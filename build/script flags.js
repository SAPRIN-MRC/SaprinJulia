//Script here
java;
//Start Flags
var Born          =(PrevResident==null && (parseInt(StartType)==2 || parseInt(StartType)==10))?1:0;
var Enumeration   =(PrevResident==null && parseInt(StartType)==1)  || 
                   (parseInt(GapEnd)==1 && parseInt(StartType)==1) || 
                   (PrevResident!=null && parseInt(PrevResident)==0 && parseInt(Resident)==1 && parseInt(StartType)==1)?1:0; 
var InMigration   =(PrevResident==null && parseInt(Resident)==1 && ~inArray.indexOf(parseInt(StartType))) ||
                   (parseInt(Resident)==1 && parseInt(GapEnd)==1 && ~inArray.indexOf(parseInt(StartType)))||
                   (PrevResident!=null && parseInt(Resident)==1 && parseInt(PrevResident)==1 && parseInt(GapEnd)==1) ||
                   (PrevResident!=null && parseInt(Resident)==1 && parseInt(PrevResident)==0 && ~inArray.indexOf(parseInt(StartType)))?1:0;
var ExtResStart   =(PrevResident==null && parseInt(Resident)==0 && ~inArray.indexOf(parseInt(StartType))) ||
                   (parseInt(Resident)==0 && parseInt(GapEnd)==1 && ~inArray.indexOf(parseInt(StartType)))||
                   (PrevResident!=null && parseInt(Resident)==0 && parseInt(PrevResident)==0 && parseInt(GapEnd)==1) ||
                   (PrevResident!=null && parseInt(Resident)==0 && parseInt(PrevResident)==1)?1:0;
var Participation =(parseInt(GapEnd)==1 && parseInt(StartType)==301)?1:0;
var LocationEntry =(parseInt(Born)==0 && parseInt(Enumeration)==0 && parseInt(InMigration)==0 && parseInt(ExtResStart)==0 && parseInt(Participation)==0) && 
                   (PrevResident!=null && parseInt(Resident)==parseInt(PrevResident) && PrevLocation!=null && !LocationUid.equals(PrevLocation))?1:0;
//End Flags
var Died          =(NextResident==null && parseInt(EndType)==7)?1:0; 
var Refusal       =(NextResident==null && parseInt(EndType)==300) ||
                   (parseInt(GapStart)==1 && parseInt(EndType)==300) ||
                   (NextResident!=null && parseInt(NextResident)==0 && parseInt(Resident)==1 && parseInt(EndType)==300)?1:0;  
var LostToFollowUp=(NextResident==null && DayDate.getTime()<LTFCutOff.getTime() && ~visitArray.indexOf(parseInt(EndType))) ||
                   (parseInt(GapStart)==1 && DayDate.getTime()<LTFCutOff.getTime() && ~visitArray.indexOf(parseInt(EndType))) ||
                   (NextResident!=null && parseInt(NextResident)==0 && parseInt(Resident)==1 && DayDate.getTime()<LTFCutOff.getTime() && ~visitArray.indexOf(parseInt(EndType)))?1:0; 
var OutMigration  =(NextResident==null && parseInt(Resident)==1 && ~outArray.indexOf(parseInt(EndType)))   ||
                   (parseInt(Resident)==1 && parseInt(GapStart)==1 && ~outArray.indexOf(parseInt(EndType)))||
                   (NextResident!=null && parseInt(NextResident)==0 && parseInt(Resident)==1 && ~outArray.indexOf(parseInt(EndType)))?1:0; 
var ExtResEnd     =(NextResident==null && parseInt(Resident)==0 && ~outArray.indexOf(parseInt(EndType)))   ||
                   (parseInt(Resident)==0 && parseInt(GapStart)==1 && ~outArray.indexOf(parseInt(EndType)))||
                   (NextResident!=null && parseInt(NextResident)==1 && parseInt(Resident)==0 && parseInt(LostToFollowUp)==0)?1:0; 
var Current       =(NextResident==null && DayDate.getTime()>=LTFCutOff.getTime() && ~visitArray.indexOf(parseInt(EndType))) ||
                   (NextResident!=null && parseInt(NextResident)==0 && parseInt(Resident)==1 && DayDate.getTime()>=LTFCutOff.getTime() && ~visitArray.indexOf(parseInt(EndType)))?1:0; 
var LocationExit  =(parseInt(Died)==0 && parseInt(Refusal)==0 && parseInt(LostToFollowUp)==0 && parseInt(OutMigration)==0 && parseInt(ExtResEnd)==0) && 
                   (NextResident!=null && parseInt(NextResident)==parseInt(Resident) && NextLocation!=null && !LocationUid.equals(NextLocation))?1:0;
//Household membership
var MembershipStart=(PrevHousehold==null) ||
                    (PrevHousehold!=null && !HouseholdUid.equals(PrevHousehold))?1:0;
var MembershipEnd  =(NextHousehold==null && parseInt(Current)==0) ||
                    (NextHousehold!=null && !HouseholdUid.equals(NextHousehold))?1:0;
//Gap sequence == rather use Episode
if (PrevDay==null) {
  Gap=0;
}
else {
  Gap=dateDiff(PrevDay,DayDate,"d")>1?Gap==0?1:0:Gap;
}