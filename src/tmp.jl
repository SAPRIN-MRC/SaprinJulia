using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using CategoricalArrays
using TableOperations
using Tables
using SAPRINCore

"Determine whether the born flag should be set"
function bornflag(r, birthtypes)
    if ismissing(r.PrevResident) && in(r.StartType, birthtypes) #BTH or DLV
        return Int8(1)
    end
    return Int8(0)
end
"Determine whether the enumeration flag should be set"
function enumerationflag(r)
    if (ismissing(r.PrevResident) && r.StartType == 1) ||
        (r.GapEnd == 1 && r.StartType == 1) ||
        (!ismissing(r.PrevResident) && r.PrevResident == 0 && r.Resident == 1 && r.StartType == 1)
        return Int8(1)
    end 
    return Int8(0)
end
"Determine whether the inmigration flag should be set"
function inmigrationflag(r, intypes)
    if (ismissing(r.PrevResident) && r.Resident == 1 && in(r.StartType, intypes)) ||
       (r.Resident == 1 && r.GapEnd == 1 && in(r.StartType, intypes)) ||
       (!ismissing(r.PrevResident) && r.Resident == 1 && r.PrevResident == 1 && r.GapEnd == 1) ||
       (!ismissing(r.PrevResident) && r.Resident == 1 && r.PrevResident == 0 && in(r.StartType, intypes))
       return Int8(1)
    end
    return Int8(0)
end
"Determine whether the external residency start flag should be set"
function extresstartflg(r, intypes)
    if (ismissing(r.PrevResident) && r.Resident == 0 && in(r.StartType, intypes)) ||
       (r.Resident == 0 && r.GapEnd == 1 && in(r.StartType, intypes)) ||
       (!ismissing(r.PrevResident) && r.Resident == 0 && r.PrevResident == 0 && r.GapEnd == 1) ||
       (!ismissing(r.PrevResident) && r.Resident == 0 && r.PrevResident == 1)
       return Int8(1)
    end
    return Int8(0)
end
"Determine whether the participation start flag should be set"
function participationflag(r)
    if r.GapEnd ==1 && r.StartType == 301
        return Int8(1)
    end
    return Int8(0)
end
"Determine whether the participation start flag should be set"
function locationentryflag(r)
    if r.Born == 0 && r.Enumeration == 0 && r.InMigration == 0 && r.ExtResStart == 0 && r.Participation == 0 &&
        !ismissing(r.PrevResident) && r.Resident == r.PrevResident && 
        !ismissing(r.PrevLocation) && r.LocationId != r.PrevLocation
        return Int8(1)
    end
    return Int8(0)
end
"Determine whether the died end flag should be set"
function diedflag(r)
    if ismissing(r.NextResident) && r.EndType == 7
        return Int8(1)
    end
    return Int8(0)
end
"Determine whether the died end flag should be set"
function refusalflag(r)
    if (ismissing(r.NextResident) && r.EndType == 300) ||
       (r.GapStart == 1 && r.EndType == 300) ||
       (!ismissing(r.NextResident) && r.NextResident == 0 && r.Resident == 1 && r.EndType == 300)
        return Int8(1)
    end
    return Int8(0)
end
"Determine whether the lost to follow-up end flag should be set"
function ltfuflag(r, visittypes, ltfcutoff::DateTime)
    if (ismissing(r.NextResident) && r.DayDate < ltfcutoff && in(r.EndType, visittypes)) ||
       (r.GapStart == 1 && r.DayDate < ltfcutoff && in(r.EndType, visittypes)) ||
       (!ismissing(r.NextResident) && r.NextResident == 0 && r.Resident == 1 && r.DayDate < ltfcutoff && in(r.EndType, visittypes))
        return Int8(1)
    else
        return Int8(0)
    end
end
function outmigrationflag(r, outtypes)
    if (ismissing(r.NextResident) && r.Resident == 1 && in(r.EndType, outtypes)) ||
       (r.Resident ==1 && r.GapStart == 1 && in(r.EndType, outtypes)) ||
       (!ismissing(r.NextResident) && r.NextResident == 0 && r.Resident == 1 && in(r.EndType, outtypes))
       return Int8(1)
    else
        return Int8(0)
    end
end
function extresendflag(r, outtypes)
    if (ismissing(r.NextResident) && r.Resident == 0 && in(r.EndType, outtypes)) ||
       (r.Resident == 0 && r.GapStart == 1 && in(r.EndType, outtypes)) ||
       (!ismissing(r.NextResident) && r.NextResident == 1 && r.Resident == 0 && r.LostToFollowUp == 0)
       return Int8(1)
    else
        return Int8(0)
    end
end
function currentflag(r, visittypes, ltfcutoff::DateTime)
    if (ismissing(r.NextResident) && r.DayDate >= ltfcutoff && in(r.EndType, visittypes)) ||
        (!ismissing(r.NextResident) && r.NextResident == 0 && r.Resident ==1 && r.DayDate >= ltfcutoff && in(r.EndType, visittypes))
        return Int8(1)
    else
        return Int8(0)
    end
end
function locationexitflag(r)
    if (r.Died == 0 && r.Refusal == 1 && r.LostToFollowUp == 0 && r.OutMigration == 0 && r.ExtResEnd ==0) &&
       (!ismissing(r.NextResident) && r.NextResident == r.Resident && !ismissing(r.NextLocation) && r.LocationId != r.NextLocation)
       return Int8(1)
    else
        return Int8(0)
    end
end
"Set residency start and end flags for a day"
function setdayflags!(r, ltfcutoff::DateTime, birthtypes, intypes, visittypes, outtypes)
    if !ismissing(r.NextDay) && r.NextDay - r.DayDate > Day(1)
        r.GapStart = Int8(1)
    end
    if !ismissing(r.PrevDay) && r.DayDate - r.PrevDay > Day(1)
        r.GapEnd = Int8(1)
    end
    #Start flags
    r.Born = bornflag(r, birthtypes)
    r.Enumeration = enumerationflag(r)
    r.InMigration = inmigrationflag(r, intypes)
    r.ExtResStart = extresstartflg(r, intypes)
    r.Participation = participationflag(r)
    r.LocationEntry = locationentryflag(r)
    #End flags
    r.Died = diedflag(r)
    r.Refusal = refusalflag(r)
    r.LostToFollowUp = ltfuflag(r, visittypes, ltfcutoff)
    r.OutMigration = outmigrationflag(r, outtypes)
    r.ExtResEnd = extresendflag(r, outtypes)
    r.Current = currentflag(r, visittypes, ltfcutoff)
    r.LocationExit = locationexitflag(r)
    #Household Memberships
    r.MembershipStart = ismissing(r.PrevHousehold) || 
                          (!ismissing(r.PrevHousehold) && r.PrevHousehold != r.HouseholdId) ? Int8(1) : Int8(0)
    r.MembershipEnd = (ismissing(r.NextHousehold) && r.Current == 0) || 
                        (!ismissing(r.NextHousehold) && r.NextHousehold != r.HouseholdId) ? Int8(1) : Int8(0)
    if !ismissing(r.PrevDay)
       r.Gap = r.DayDate - r.PrevDay > Day(1) ? r.Gap == 0 ? 1 : 0 : r.Gap 
    end
    return nothing
end
"Determine whether a particular day starts or end a residency episode and the type of start or end"
function setresidencyflags(basedirectory::String, node::String)
    ltfcutoff = DateTime("2018-01-01T00:00:00") #todo: replace with settings
    birthtypes = Int32[2, 10] # BTH, DLV
    outtypes = Int32[4, 5, 101 ,103, 104] # OMG, EXT, HDS, HME, HRC
    intypes = Int32[3, 6, 100, 102] # IMG, OMG, HFM, HMS
    visittypes = Int32[9, 18] # OBE, OBS
    householdstarts = Int32[100, 102] # HFM, HMS
    householdend = Int32[101, 103] # HDS, HME
    preferredhhbatches = Arrow.Stream(joinpath(basedirectory, node, "DayExtraction", "IndividualPreferredHHConsolidatedDays_batched.arrow"))
    hstate = iterate(preferredhhbatches)
    i = 1
    while hstate !== nothing
        t = now()
        println("Node $(node) batch $(i) at $(t)")
        h, hst = hstate
        hd = h |> DataFrame
        df = transform(hd, AsTable([:IndResLocationId,:HHResLocationId]) => ByRow(x -> ismissing(x.IndResLocationId) ? x.HHResLocationId : x.IndResLocationId) => :LocationId,
                           :IndResLocationId => ByRow(x -> ismissing(x) ? Int8(0) : Int8(1)) => :Resident,
                           AsTable([:IndResStartType, :HHMemStartType]) => ByRow(x -> ismissing(x.IndResStartType) ? x.HHMemStartType : x.IndResStartType) => :StartType,
                           AsTable([:IndResEndType, :HHMemEndType]) => ByRow(x -> ismissing(x.IndResEndType) ? x.HHMemEndType : x.IndResEndType) => :EndType)
        select!(df, Not([:IndResLocationId,:HHResLocationId,:IndResStartType, :HHMemStartType,:IndResEndType, :HHMemEndType, :IndResStart, :IndResEnd, :HHMemStart, :HHMemEnd]))
        s = combine(groupby(df, :IndividualId), :DayDate, :LocationId, :HouseholdId, :HHRelationshipTypeId, :Resident, :StartType, :EndType, :Memberships,
                    :DayDate => Base.Fix2(lead,1) => :NextDay, :DayDate => Base.Fix2(lag,1) => :PrevDay, 
                    :StartType => Base.Fix2(lead,1) => :NextStart, :StartType => Base.Fix2(lag,1) => :PrevStart,
                    :EndType => Base.Fix2(lag,1) => :PrevEnd,
                    :Resident => Base.Fix2(lead,1) => :NextResident, :Resident => Base.Fix2(lag,1) => :PrevResident, 
                    :LocationId => Base.Fix2(lead,1) => :NextLocation, :LocationId => Base.Fix2(lag,1) => :PrevLocation, 
                    :HouseholdId => Base.Fix2(lead,1) => :NextHousehold, :HouseholdId => Base.Fix2(lag,1) => :PrevHousehold)
        insertcols!(s, :GapStart => Int8(0), :GapEnd => Int8(0), :Gap =>  Int8(0),
                       :Born => Int8(0), :Enumeration => Int8(0), :InMigration => Int8(0), :ExtResStart => Int8(0), :Participation => Int8(0), :LocationEntry => Int8(0),
                       :Died => Int8(0), :Refusal => Int8(0), :LostToFollowUp => Int8(0), :OutMigration => Int8(0), :ExtResEnd => Int8(0), :LocationExit => Int8(0), :Current => Int8(0),
                       :MembershipStart => Int8(0), :MembershipEnd => Int8(0))
        for row in eachrow(s)
            setdayflags!(row, ltfcutoff, birthtypes, intypes, visittypes, outtypes)
        end
        select!(s,[:IndividualId, :DayDate, :HouseholdId, :LocationId, :Resident, :Gap, 
                   :Enumeration, :Born, :Participation, :InMigration, :LocationEntry, :ExtResStart, 
                   :Died, :Refusal, :LostToFollowUp, :Current, :OutMigration, :LocationExit, :ExtResEnd, :MembershipStart, :MembershipEnd, 
                   :HHRelationshipTypeId, :Memberships, :GapStart, :GapEnd])
        open(joinpath(basedirectory, node, "DayExtraction", "DayDatasetStep01$(i).arrow"),"w"; lock = true) do io
            Arrow.write(io, s, compress=:zstd)
        end
        hstate = iterate(preferredhhbatches, hst)
        println("Node $(node) batch $(i) completed after $(round(now()-t, Dates.Second))")
        i = i + 1
    end
    combinedaybatch(basedirectory,node,"DayDatasetStep01", i-1)
    return nothing
end
@info "Started execution $(now())"
t = now()
# testreadwrite("D:\\Data\\SAPRIN_Data","Agincourt")
# @info "Finished Agincourt $(now())"
setresidencyflags("D:\\Data\\SAPRIN_Data","DIMAMO")
@info "Finished DIMAMO $(now())"
#testreadwrite("D:\\Data\\SAPRIN_Data","AHRI")
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
