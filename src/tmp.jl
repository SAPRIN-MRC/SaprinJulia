using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using XLSX
using CategoricalArrays

function extractmembershipdays(node::String, batchsize::Int64)
    batchmembershipdays("D:\\Data\\SAPRIN_Data", node, batchsize)
end
function processhhmembershipdays(startdate, enddate, starttype, endtype)
    start = startdate[1]
    stop = enddate[1]
    startt = starttype[1]
    endt = endtype[1]
    res_daydate = collect(start:Day(1):stop)
    res_starttype = fill(startt, length(res_daydate))
    res_endtype = fill(endt, length(res_daydate))
    res_start = fill(Int8(0), length(res_daydate))
    res_start[1] = Int8(1)
    res_end = fill(Int8(0), length(res_daydate))
    res_end[end] = Int8(1)
    episode = Int32(1)
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
        startidx = length(res_daydate) + 1
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_starttype, fill(startt, length(new_daydate)))
        append!(res_endtype, fill(endt, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
        append!(res_start, fill(Int8(0), length(new_daydate)))        
        append!(res_end, fill(Int8(0), length(new_daydate)))  
        res_start[startidx] = Int8(1)
        res_end[end] = Int8(1)
    end

    return (daydate = res_daydate, starttype = res_starttype, endtype = res_endtype, episode = res_episode, startflag = res_start, endflag = res_end)
end #processhhmembershipdays
function gethhmembershipdays(basedirectory::String, node::String, f, batch::Int64)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId,:HouseholdId,:HHRelationshipTypeId], sort=true), [:StartDate, :EndDate, :StartType, :EndType] => processhhmembershipdays => AsTable)
    rename!(s,Dict(:daydate => "DayDate", :episode => "Episode", :starttype => "StartType", :endtype => "EndType", :startflag => "Start", :endflag => "End"))
    disallowmissing!(s, [:DayDate, :Episode, :StartType, :EndType, :Start, :End])
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "HouseholdMembershipDays$(batch).arrow"), s)
    return nothing
end #gethhresidencydays
function openhhmembershipchunk(basedirectory::String, node::String, chunk::Int64)
    open(joinpath(basedirectory, node, "DayExtraction", "HouseholdMembershipDays$(chunk).arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end;
end
"Concatenate household membership day batches"
function combinehhmembershipdaybatch(basedirectory::String, node::String, batches)
    d = openhhmembershipchunk(basedirectory::String, node::String, 1)
    days = copy(d)
    for i = 2:batches
        m = openhhmembershipchunk(basedirectory::String, node::String, i)
        append!(days, m)
    end
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "HouseholdMembershipDays.arrow"), days, compress=:zstd)
    @info "Final household membership day rows $(nrow(days)) for $(node)"
    #delete chunks
    for i = 1:batches
        rm(joinpath(basedirectory, node, "DayExtraction", "HouseholdMembershipDays$(i).arrow"))
    end
    return nothing
end #combinehhresidencydaybatch
"Normalise memberships in batches"
function batchmembershipdays(basedirectory::String, node::String, batchsize::Int64)
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    minId = minimum(individualmap[!,:IndividualId])
    maxId = maximum(individualmap[!,:IndividualId])
    idrange = (maxId - minId) + 1
    batches = ceil(Int32, idrange / batchsize)
    @info "Node $(node) Batch size $(batchsize) Minimum id $(minId), maximum Id $(maxId), idrange $(idrange), batches $(batches)"
    memberships = open(joinpath(basedirectory, node, "Staging", "IndividualMemberships.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    @info "Node $(node) $(nrow(memberships)) individual membership episodes"
    Threads.@threads for i = 1:batches
        fromId = minId + batchsize * (i-1)
        toId = min(maxId, (minId + batchsize * i)-1)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:IndividualId] => id -> id >= fromId && id <= toId, memberships)
        gethhmembershipdays(basedirectory,node,d,i)
    end
    combinehhmembershipdaybatch(basedirectory,node,batches)
    return nothing
end #batchmembershipdays
@info "Started execution $(now())"
t = now()
extractmembershipdays("DIMAMO", 30000)
d = now()-t
@info "Stopped execution $(now()) duration $(round(d, Dates.Second))"
