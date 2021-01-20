using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using CategoricalArrays

function consolidatepreferredhousehold(basedirectory::String, node::String)
    memberships = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "HouseholdMembershipDays.arrow")) |> DataFrame
    hhresidencies = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays.arrow")) |> DataFrame
    println("$(node) read $(nrow(memberships)) membership days $(nrow(hhresidencies)) residency days at $(now())")
    hm = innerjoin(memberships,hhresidencies, on = [:HouseholdId, :DayDate], makeunique = true)
    select!(hm,[:HouseholdId, :DayDate, :IndividualId, :LocationId, :StartType, :EndType, :Start, :End, :HHRelationshipTypeId])
    rename!(hm, [:LocationId =>:HHResLocationId, :StartType => :HHMemStartType, :EndType => :HHMemEndType, :Start => :HHMemStart, :End => :HHMemEnd])
    residencydays = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "IndividualResidencyDays.arrow")) |> DataFrame
    rename!(residencydays,[:LocationId => :IndResLocationId, :StartType => :IndResStartType, :EndType => :IndResEndType, :Start => :IndResStart, :End => :IndResEnd])
    rd = outerjoin(hm, residencydays, on = [:IndividualId, :DayDate], makeunique=true, indicator=:result)
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "ResDaysNoMember.arrow"), 
                select(filter(x -> x.result == "right_only", rd), [:IndividualId,:DayDate,:IndResLocationId,:IndResStartType,:IndResEndType, :IndResStart, :IndResEnd]), compress=:zstd)
    filter!(x -> x.result != "right_only", rd)
    insertcols!(rd,:HHRelationshipTypeId, :HHRank => 999)
    for i = 1:nrow(rd)
        if ismissing(rd[i,:IndResLocationId])
            rd[i,:HHRank] = rd[i,:HHRelationshipTypeId] + 100
        elseif rd[i,:IndResLocationId]==rd[i,:HHResLocationId]
            rd[i,:HHRank] = rd[i,:HHRelationshipTypeId]
        else
            rd[i,:HHRank] = rd[i,:HHRelationshipTypeId] + 100
        end
    end
    sort!(rd,[:IndividualId,:DayDate,:HHRank])
    unique!(rd,[:IndividualId,:DayDate])
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "IndividualPreferredHHDays.arrow"), rd, compress=:zstd)
    println("$(node) wrote $(nrow(rd)) preferred household days at $(now())")
    return nothing
end

@info "Started execution $(now())"
t = now()
consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","Agincourt")
#consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","DIMAMO")
#consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","AHRI")
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
