using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using Query

function getmembershipdays(basedirectory::String, node::String, fromId::Int64, toId::Int64)
    memberships =Arrow.Table(joinpath(basedirectory, node, "Staging", "HouseholdMemberships.arrow")) |> DataFrame
    f = filter([:IndividualId] => id -> id >= fromId && id <= toId, memberships)
    select!(f,[:IndividualId, :HouseholdId, :Episode, :StartDate, :StartType, :EndDate, :EndType])
    m = similar(f,0)
    for row in eachrow(f)
        tf = DataFrame(row)
        ttf=repeat(tf, Dates.value.(row.EndDate-row.StartDate) + 1)
        ttf.DayDate = ttf.StartDate .+ Dates.Day.(0:nrow(ttf)-1)
        ttf.Start = ttf.DayDate .== ttf.StartDate
        ttf.End = ttf.DayDate .== ttf.EndDate
        append!(m, ttf, cols = :union)
    end
    unique!(m,[:IndividualId,:HouseholdId,:DayDate])
    println("Unique membership rows $(nrow(m))")
    return m
end
function getrelationshipdays(basedirectory::String, node::String, fromId::Int64, toId::Int64)
    relationships = Arrow.Table(joinpath(basedirectory, node, "Staging", "HHeadRelationships.arrow")) |> DataFrame
    f =filter([:IndividualId] => id -> id >= fromId && id <= toId, relationships)
    select!(f,[:IndividualId, :HouseholdId, :Episode, :StartDate, :StartType, :EndDate, :EndType, :HHRelationshipTypeId])
    r = similar(relationships,0)
    for row in eachrow(f)
        tf = DataFrame(row)
        ttf=repeat(tf, Dates.value.(row.EndDate-row.StartDate) + 1)
        ttf.DayDate = ttf.StartDate .+ Dates.Day.(0:nrow(ttf)-1)
        ttf.Start = ttf.DayDate .== ttf.StartDate
        ttf.End = ttf.DayDate .== ttf.EndDate
        append!(r, ttf, cols = :union)
    end
    unique!(r,[:IndividualId,:HouseholdId,:DayDate])
    println("Unique relationship rows $(nrow(r))")
    return r
end
function individualmemberships(basedirectory::String, node::String, fromId::Int64, toId::Int64, batch::Int64)
    mr = leftjoin(getmembershipdays(basedirectory,node,fromId,toId), getrelationshipdays(basedirectory,node,fromId,toId) , on = [:IndividualId => :IndividualId, :HouseholdId => :HouseholdId, :DayDate => :DayDate], makeunique=true, matchmissing=:equal)
    select!(mr,[:IndividualId, :HouseholdId, :HHRelationshipTypeId, :DayDate, :StartType, :EndType, :Episode])
    replace!(mr.HHRelationshipTypeId, missing => 12)
    disallowmissing!(mr,[:HHRelationshipTypeId, :DayDate])
    println("$(nrow(mr)) $(node) day rows")
    mr = combine(groupby(mr,[:IndividualId,:HouseholdId]), :HHRelationshipTypeId, :DayDate, :StartType, :EndType, :Episode, 
                             :HHRelationshipTypeId => Base.Fix2(lag,1) => :LastRelation, :HHRelationshipTypeId => Base.Fix2(lead,1) => :NextRelation)
    for i = 1:nrow(mr)
        if !ismissing(mr[i,:LastRelation])
            if mr[i, :LastRelation] != mr[i, :HHRelationshipTypeId]
                mr[i, :StartType] = 104
            end
        end
        if !ismissing(mr[i,:NextRelation])
            if mr[i, :NextRelation] != mr[i, :HHRelationshipTypeId]
                mr[i, :EndType] = 104
            end
        end
    end
    memberships = combine(groupby(mr,[:IndividualId, :HouseholdId, :HHRelationshipTypeId, :Episode]), :DayDate => minimum => :StartDate, :StartType => first => :StartType,
                                                                                            :DayDate => maximum => :EndDate, :EndType => last => :EndType)
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMemberships$(batch).arrow"), memberships, compress=:zstd)
    println("Wrote $(nrow(memberships)) $(node) individual membership episodes")
    return nothing
end
function openchunk(basedirectory::String, node::String, chunk::Int64)
    open(joinpath(basedirectory, node, "Staging", "IndividualMemberships$(1).arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end;
end
"Concatenate membership batches"
function combinemembershipbatch(basedirectory::String, node::String, batches::Int64)
    memberships = openchunk(basedirectory::String, node::String, 1)
    r = similar(memberships,0)
    for i = 1:batches
        m = openchunk(basedirectory::String, node::String, i)
        append!(r, m)
    end
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMemberships.arrow"), r, compress=:zstd)
    return r
end

function batchmemberships(basedirectory::String, node::String, batchsize::Int64)
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    minId = minimum(individualmap[!,:IndividualId])
    maxId = maximum(individualmap[!,:IndividualId])
    idrange = (maxId - minId) + 1
    batches = ceil(Int32, idrange / batchsize)
    println("Minimum id $(minId), maximum Id $(maxId), idrange $(idrange), batches $(batches)")
    for i = 1:batches
        fromId = minId + batchsize * (i-1)
        toId = min(maxId, (minId + batchsize * i)-1)
        println("Batch $(i) from $(fromId) to $(toId)")
        @time individualmemberships(basedirectory,node,fromId,toId,i)
    end
    return nothing
end
combinemembershipbatch("D:\\Data\\SAPRIN_Data","Agincourt",9)
