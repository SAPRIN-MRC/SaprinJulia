using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using CategoricalArrays
using TableOperations
using SAPRINCore

function batchmembershipdayswithlocation(basedirectory::String, node::String)
    hhresidencies = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays.arrow")) |> DataFrame
    println("$(node) read $(nrow(hhresidencies)) household residency days at $(now())")
    membershipdaybatches = Arrow.Stream(joinpath(basedirectory, node, "DayExtraction", "HouseholdMembershipDays_batched.arrow"));
    batch = 1
    for b in membershipdaybatches
        memberships = b |> DataFrame
        hm = innerjoin(memberships,hhresidencies, on = [:HouseholdId, :DayDate], makeunique = true)
        select!(hm,[:HouseholdId, :DayDate, :IndividualId, :LocationId, :StartType, :EndType, :Start, :End, :HHRelationshipTypeId])
        rename!(hm, [:LocationId =>:HHResLocationId, :StartType => :HHMemStartType, :EndType => :HHMemEndType, :Start => :HHMemStart, :End => :HHMemEnd])
        open(joinpath(basedirectory,node,"DayExtraction","HouseholdMembershipDaysWithLocation$(batch).arrow"),"w"; lock = true) do io
            Arrow.write(io, hm, compress=:zstd)
        end
        batch = batch + 1
    end
    combinedaybatch(basedirectory,node,"HouseholdMembershipDaysWithLocation",batch-1)
    return nothing
end
function processconsolidatehhbatch(basedirectory::String, node::String, md, rd, batch)
    rename!(rd,[:LocationId => :IndResLocationId, :StartType => :IndResStartType, :EndType => :IndResEndType, :Start => :IndResStart, :End => :IndResEnd])
    df = outerjoin(md, rd, on = [:IndividualId, :DayDate], makeunique=true, indicator=:result)
    open(joinpath(basedirectory, node, "DayExtraction", "ResDaysNoMember$(batch).arrow"),"w"; lock = true) do io
        Arrow.write(io, select(filter(x -> x.result == "right_only", df), [:IndividualId,:DayDate,:IndResLocationId,:IndResStartType,:IndResEndType, :IndResStart, :IndResEnd]), compress=:zstd)
    end
    filter!(x -> x.result != "right_only", df)
    insertcols!(df, :HHRelationshipTypeId, :HHRank => Int32(999))
    for i = 1:nrow(df)
        if ismissing(df[i,:IndResLocationId])
            df[i,:HHRank] = df[i,:HHRelationshipTypeId] + Int32(100)
        elseif df[i,:IndResLocationId]==df[i,:HHResLocationId]
            df[i,:HHRank] = df[i,:HHRelationshipTypeId]
        else
            df[i,:HHRank] = df[i,:HHRelationshipTypeId] + Int32(100)
        end
    end
    sort!(df, [:IndividualId,:DayDate,:HHRank])
    unique!(df,[:IndividualId,:DayDate])
    select!(df,[:IndividualId,:HouseholdId,:DayDate,:HHResLocationId,:IndResLocationId,:HHMemStartType, :HHMemEndType, :HHMemStart, :HHMemEnd, :IndResStartType, :IndResEndType, :Episode, :IndResStart, :IndResEnd, :HHRank, :HHRelationshipTypeId])
    open(joinpath(basedirectory, node, "DayExtraction", "IndividualPreferredHHDays$(batch).arrow"),"w"; lock = true) do io
        Arrow.write(io, df, compress=:zstd)
    end
    println("$(node) batch $(batch) wrote $(nrow(df)) preferred household days at $(now())")
end
function consolidatepreferredhousehold(basedirectory::String, node::String)
    membershipbatches = Arrow.Stream(joinpath(basedirectory, node, "DayExtraction", "HouseholdMembershipDaysWithLocation_batched.arrow"))
    residencybatches = Arrow.Stream(joinpath(basedirectory, node, "DayExtraction", "IndividualResidencyDays_batched.arrow"))
    mstate = iterate(membershipbatches)
    rstate = iterate(residencybatches)
    i = 1
    while mstate !== nothing && rstate !== nothing
        m, mst = mstate
        md = m |> DataFrame
        r, rst = rstate
        rd = r |> DataFrame
        processconsolidatehhbatch(basedirectory, node, md, rd, i)
        mstate = iterate(membershipbatches, mst)
        rstate = iterate(residencybatches, rst)
        i = i + 1
    end
    combinedaybatch(basedirectory,node,"ResDaysNoMember", i-1)
    combinedaybatch(basedirectory,node,"IndividualPreferredHHDays", i-1)
    return nothing
end

@info "Started execution $(now())"
t = now()
# batchmembershipdayswithlocation("D:\\Data\\SAPRIN_Data","Agincourt")
# consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","Agincourt")
# d = now()-t
# @info "Finished Agincourt execution $(now()) duration $(round(d, Dates.Second))"
batchmembershipdayswithlocation("D:\\Data\\SAPRIN_Data","DIMAMO")
consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","DIMAMO")
# t = now()
# batchmembershipdayswithlocation("D:\\Data\\SAPRIN_Data","AHRI")
# consolidatepreferredhousehold("D:\\Data\\SAPRIN_Data","AHRI")
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
