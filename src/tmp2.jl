using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using XLSX
using CategoricalArrays

function readindividualmemberships(node::String, batchsize::Int64)
    batchmemberships("D:\\Data\\SAPRIN_Data", node, batchsize)
end
function processmembershipdays(startdate, enddate, starttype, endtype)
    start = startdate[1]
    stop = enddate[1]
    startt = starttype[1]
    endt = endtype[1]
    res_daydate = collect(start:Day(1):stop)
    res_startdate = fill(start, length(res_daydate))
    res_enddate = fill(stop, length(res_daydate))
    res_starttype = fill(startt, length(res_daydate))
    res_endtype = fill(endt, length(res_daydate))
    episode = 1
    res_episode = fill(episode, length(res_daydate))
    for i in 2:length(startdate)
        if startdate[i] > res_daydate[end]
            start = startdate[i]
        elseif enddate[i] > res_daydate[end]
            start = res_daydate[end] + Day(1)
        else
            continue #this episode is contained within the previous episode
        end
        episode = episode + 1
        stop = enddate[i]
        startt = starttype[i]
        endt = endtype[i]
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_startdate, fill(startdate[i], length(new_daydate)))
        append!(res_enddate, fill(stop, length(new_daydate)))
        append!(res_starttype, fill(startt, length(new_daydate)))
        append!(res_endtype, fill(endt, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
    end

    return (daydate = res_daydate, startdate = res_startdate, enddate = res_enddate, starttype = res_starttype, endtype = res_endtype, episode = res_episode)
end
function getmembershipdays(f)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId, :HouseholdId], sort=true), [:StartDate, :EndDate, :StartType, :EndType] => processmembershipdays => AsTable)
     rename!(s,Dict(:daydate=>"DayDate",:episode=>"Episode",:startdate=>"StartDate",:enddate => "EndDate",:starttype => "StartType",:endtype => "EndType"))
    return s
end #getmembershipdays
function processrelationshipdays(startdate, enddate, starttype, endtype, hhrelationtype)
    start = startdate[1]
    stop = enddate[1]
    startt = starttype[1]
    endt = endtype[1]
    relation = hhrelationtype[1]
    res_daydate = collect(start:Day(1):stop)
    res_startdate = fill(start, length(res_daydate))
    res_enddate = fill(stop, length(res_daydate))
    res_starttype = fill(startt, length(res_daydate))
    res_endtype = fill(endt, length(res_daydate))
    res_relation = fill(relation, length(res_daydate))
    episode = 1
    res_episode = fill(episode, length(res_daydate))
    for i in 2:length(startdate)
        if startdate[i] > res_daydate[end]
            start = startdate[i]
        elseif enddate[i] > res_daydate[end]
            start = res_daydate[end] + Day(1)
        else
            continue #this episode is contained within the previous episode
        end
        episode = episode + 1
        stop = enddate[i]
        startt = starttype[i]
        endt = endtype[i]
        relation = hhrelationtype[i]
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_startdate, fill(startdate[i], length(new_daydate)))
        append!(res_enddate, fill(stop, length(new_daydate)))
        append!(res_starttype, fill(startt, length(new_daydate)))
        append!(res_endtype, fill(endt, length(new_daydate)))
        append!(res_relation, fill(relation, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
    end

    return (daydate = res_daydate, startdate = res_startdate, enddate = res_enddate, starttype = res_starttype, endtype = res_endtype, hhrelationtype = res_relation ,episode = res_episode)
end
function getrelationshipdays(f)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId, :HouseholdId], sort=true), [:StartDate, :EndDate, :StartType, :EndType, :HHRelationshipTypeId] => processrelationshipdays => AsTable)
    rename!(s,Dict(:daydate=>"DayDate",:episode=>"Episode",:startdate=>"StartDate",:enddate => "EndDate",:starttype => "StartType",:endtype => "EndType",:hhrelationtype => "HHRelationshipTypeId"))
    return s
end #getrelationshipdays
function individualmemberships(basedirectory::String, node::String, m, r, batch::Int64)
    mr = leftjoin(getmembershipdays(m), getrelationshipdays(r), on = [:IndividualId => :IndividualId, :HouseholdId => :HouseholdId, :DayDate => :DayDate], makeunique=true, matchmissing=:equal)
    select!(mr,[:IndividualId, :HouseholdId, :HHRelationshipTypeId, :DayDate, :StartType, :EndType, :Episode])
    replace!(mr.HHRelationshipTypeId, missing => 12)
    disallowmissing!(mr,[:HHRelationshipTypeId, :DayDate])
    @info "$(nrow(mr)) $(node) day rows in batch $(batch)"
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
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMemberships$(batch).arrow"), memberships)
    @info "Wrote $(nrow(memberships)) $(node) individual membership episodes in batch $(batch)"
    return nothing
end #individualmemberships
function openchunk(basedirectory::String, node::String, chunk::Int64)
    open(joinpath(basedirectory, node, "Staging", "IndividualMemberships$(chunk).arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end;
end
"Concatenate membership batches"
function combinemembershipbatch(basedirectory::String, node::String, batches)
    memberships = openchunk(basedirectory::String, node::String, 1)
    r = similar(memberships,0)
    for i = 1:batches
        m = openchunk(basedirectory::String, node::String, i)
        append!(r, m)
    end
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMemberships.arrow"), r, compress=:zstd)
    @info "Final individual membership rows $(nrow(r)) for $(node)"
    #delete chunks
    for i = 1:batches
        rm(joinpath(basedirectory, node, "Staging", "IndividualMemberships$(i).arrow"))
    end
    return nothing
end #combinemembershipbatch
"Normalise memberships in batches"
function batchmemberships(basedirectory::String, node::String, batchsize::Int64)
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    minId = minimum(individualmap[!,:IndividualId])
    maxId = maximum(individualmap[!,:IndividualId])
    idrange = (maxId - minId) + 1
    batches = ceil(Int32, idrange / batchsize)
    @info "Node $(node) Batch size $(batchsize) Minimum id $(minId), maximum Id $(maxId), idrange $(idrange), batches $(batches)"
    relationships = open(joinpath(basedirectory, node, "Staging", "HHeadRelationships.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(relationships,[:IndividualId, :HouseholdId, :Episode, :StartDate, :StartType, :EndDate, :EndType, :HHRelationshipTypeId])
    @info "Node $(node) $(nrow(relationships)) relationship episodes"
    memberships = open(joinpath(basedirectory, node, "Staging", "HouseholdMemberships.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(memberships,[:IndividualId, :HouseholdId, :Episode, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(memberships)) memberships episodes"
     Threads.@threads for i = 1:batches
        fromId = minId + batchsize * (i-1)
        toId = min(maxId, (minId + batchsize * i)-1)
        @info "Batch $(i) from $(fromId) to $(toId)"
        m = filter([:IndividualId] => id -> id >= fromId && id <= toId, memberships)
        r = filter([:IndividualId] => id -> id >= fromId && id <= toId, relationships)
        individualmemberships(basedirectory,node,m,r,i)
    end
    combinemembershipbatch(basedirectory,node,batches)
    return nothing
end #batchmemberships
@info "Started execution $(now())"
t = now()
readindividualmemberships("AHRI", 25000)
d = now()-t
@info "Started execution $(now()) duration $(round(d, Dates.Second)) seconds"
