#region Individual residency days
function extractresidencydays(node::String, batchsize::Int64)
    batchresidencydays(settings.BaseDirectory, node, batchsize)
end
function processresidencydays(locationid, startdate, enddate, starttype, endtype)
    start = startdate[1]
    stop = enddate[1]
    startt = starttype[1]
    endt = endtype[1]
    location = locationid[1]
    res_daydate = collect(start:Day(1):stop)
    res_starttype = fill(startt, length(res_daydate))
    res_endtype = fill(endt, length(res_daydate))
    res_start = fill(Int8(0), length(res_daydate))
    res_start[1] = Int8(1)
    res_end = fill(Int8(0), length(res_daydate))
    res_end[end] = Int8(1)
    episode = Int32(1)
    res_episode = fill(episode, length(res_daydate))
    res_location = fill(location, length(res_daydate))
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
        location = locationid[i]
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_starttype, fill(startt, length(new_daydate)))
        append!(res_endtype, fill(endt, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
        append!(res_location, fill(location, length(new_daydate)))
        append!(res_start, fill(Int8(0), length(new_daydate)))        
        append!(res_end, fill(Int8(0), length(new_daydate)))  
        res_start[startidx] = Int8(1)
        res_end[end] = Int8(1)
    end

    return (locationid = res_location, daydate = res_daydate, starttype = res_starttype, endtype = res_endtype, episode = res_episode, startflag = res_start, endflag = res_end)
end
function getresidencydays(basedirectory::String, node::String, f, batch::Int64)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId], sort=true), [:LocationId, :StartDate, :EndDate, :StartType, :EndType] => processresidencydays => AsTable)
    rename!(s,Dict(:locationid => "LocationId", :daydate => "DayDate", :episode => "Episode", :starttype => "StartType", :endtype => "EndType", :startflag => "Start", :endflag => "End"))
    disallowmissing!(s, [:LocationId, :DayDate, :Episode, :StartType, :EndType, :Start, :End])
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "IndividualResidencyDays$(batch).arrow"), s)
    return nothing
end #getresidencydays
function openresidencychunk(basedirectory::String, node::String, chunk::Int64)
    open(joinpath(basedirectory, node, "DayExtraction", "IndividualResidencyDays$(chunk).arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end;
end
"Concatenate residency day batches"
function combineresidencydaybatch(basedirectory::String, node::String, batches)
    days = openresidencychunk(basedirectory::String, node::String, 1)
    r = similar(days,0)
    for i = 1:batches
        m = openresidencychunk(basedirectory::String, node::String, i)
        append!(r, m)
    end
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "IndividualResidencyDays.arrow"), r, compress=:zstd)
    @info "Final individual residency day rows $(nrow(r)) for $(node)"
    #delete chunks
    for i = 1:batches
        rm(joinpath(basedirectory, node, "DayExtraction", "IndividualResidencyDays$(i).arrow"))
    end
    return nothing
end #combineresidencydaybatch
"Normalise memberships in batches"
function batchresidencydays(basedirectory::String, node::String, batchsize::Int64)
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    minId = minimum(individualmap[!,:IndividualId])
    maxId = maximum(individualmap[!,:IndividualId])
    idrange = (maxId - minId) + 1
    batches = ceil(Int32, idrange / batchsize)
    @info "Node $(node) Batch size $(batchsize) Minimum id $(minId), maximum Id $(maxId), idrange $(idrange), batches $(batches)"
    residencies = open(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(residencies,[:IndividualId, :LocationId, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(residencies)) residency episodes"
    Threads.@threads for i = 1:batches
        fromId = minId + batchsize * (i-1)
        toId = min(maxId, (minId + batchsize * i)-1)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:IndividualId] => id -> id >= fromId && id <= toId, residencies)
        getresidencydays(basedirectory,node,d,i)
    end
    combineresidencydaybatch(basedirectory,node,batches)
    return nothing
end #batchresidencydays
#endregion
#region Household residency days
function extracthhresidencydays(node::String, batchsize::Int64)
    batchhhresidencydays(settings.BaseDirectory, node, batchsize)
end
function processhhresidencydays(locationid, startdate, enddate, starttype, endtype)
    start = startdate[1]
    stop = enddate[1]
    startt = starttype[1]
    endt = endtype[1]
    location = locationid[1]
    res_daydate = collect(start:Day(1):stop)
    res_starttype = fill(startt, length(res_daydate))
    res_endtype = fill(endt, length(res_daydate))
    res_start = fill(Int8(0), length(res_daydate))
    res_start[1] = Int8(1)
    res_end = fill(Int8(0), length(res_daydate))
    res_end[end] = Int8(1)
    episode = Int32(1)
    res_episode = fill(episode, length(res_daydate))
    res_location = fill(location, length(res_daydate))
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
        location = locationid[i]
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_starttype, fill(startt, length(new_daydate)))
        append!(res_endtype, fill(endt, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
        append!(res_location, fill(location, length(new_daydate)))
        append!(res_start, fill(Int8(0), length(new_daydate)))        
        append!(res_end, fill(Int8(0), length(new_daydate)))  
        res_start[startidx] = Int8(1)
        res_end[end] = Int8(1)
    end

    return (locationid = res_location, daydate = res_daydate, starttype = res_starttype, endtype = res_endtype, episode = res_episode, startflag = res_start, endflag = res_end)
end
function gethhresidencydays(basedirectory::String, node::String, f, batch::Int64)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:HouseholdId], sort=true), [:LocationId, :StartDate, :EndDate, :StartType, :EndType] => processhhresidencydays => AsTable)
    rename!(s,Dict(:locationid => "LocationId", :daydate => "DayDate", :episode => "Episode", :starttype => "StartType", :endtype => "EndType", :startflag => "Start", :endflag => "End"))
    disallowmissing!(s, [:LocationId, :DayDate, :Episode, :StartType, :EndType, :Start, :End])
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays$(batch).arrow"), s)
    return nothing
end #gethhresidencydays
function openhhresidencychunk(basedirectory::String, node::String, chunk::Int64)
    open(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays$(chunk).arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end;
end
"Concatenate household residency day batches"
function combinehhresidencydaybatch(basedirectory::String, node::String, batches)
    d = openhhresidencychunk(basedirectory::String, node::String, 1)
    days = copy(d)
    for i = 2:batches
        m = openhhresidencychunk(basedirectory::String, node::String, i)
        append!(days, m)
    end
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays.arrow"), days, compress=:zstd)
    @info "Final household residency day rows $(nrow(days)) for $(node)"
    #delete chunks
    for i = 1:batches
        rm(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays$(i).arrow"))
    end
    return nothing
end #combinehhresidencydaybatch
"Normalise memberships in batches"
function batchhhresidencydays(basedirectory::String, node::String, batchsize::Int64)
    householdmap = Arrow.Table(joinpath(basedirectory,node,"Staging","HouseholdMap.arrow")) |> DataFrame
    minId = minimum(householdmap[!,:HouseholdId])
    maxId = maximum(householdmap[!,:HouseholdId])
    idrange = (maxId - minId) + 1
    batches = ceil(Int32, idrange / batchsize)
    @info "Node $(node) Batch size $(batchsize) Minimum id $(minId), maximum Id $(maxId), idrange $(idrange), batches $(batches)"
    residencies = open(joinpath(basedirectory, node, "Staging", "HouseholdResidences.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(residencies,[:HouseholdId, :LocationId, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(residencies)) household residency episodes"
    Threads.@threads for i = 1:batches
        fromId = minId + batchsize * (i-1)
        toId = min(maxId, (minId + batchsize * i)-1)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:HouseholdId] => id -> id >= fromId && id <= toId, residencies)
        gethhresidencydays(basedirectory,node,d,i)
    end
    combinehhresidencydaybatch(basedirectory,node,batches)
    return nothing
end #batchhhresidencydays
#end region
#region Household Membership days
function extractmembershipdays(node::String, batchsize::Int64)
    batchmembershipdays(settings.BaseDirectory, node, batchsize)
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
#endregion
