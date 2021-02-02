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
    open(joinpath(basedirectory,node,"DayExtraction","IndividualResidencyDays$(batch).arrow"),"w"; lock = true) do io
        Arrow.write(io, s, compress=:zstd)
    end
    return nothing
end #getresidencydays
function readpartitionfile(file)
    open(file,"r"; lock = true) do io
        return Arrow.Table(io)
    end
end
"Concatenate day batches"
function combinedaybatch(basedirectory::String, node::String, file::String, batches)
    files = Array{String,1}()
    for i = 1:batches
        push!(files,joinpath(basedirectory, node, "DayExtraction", "$(file)$(i).arrow"))
    end
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "$(file)_batched.arrow"), Tables.partitioner(x->readpartitionfile(x),files), compress=:zstd)
    #delete chunks
    for i = 1:batches
        rm(joinpath(basedirectory, node, "DayExtraction", "$(file)$(i).arrow"))
    end
    return nothing
end #combinedaybatch
"Normalise residencies in batches"
function batchresidencydays(basedirectory::String, node::String, batchsize::Int64)
    minId, maxId, batches = individualbatch(basedirectory, node, batchsize)
    residencies = open(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(residencies,[:IndividualId, :LocationId, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(residencies)) residency episodes"
    Threads.@threads for i = 1:batches
        fromId, toId = nextidrange(minId, maxId, batchsize, i)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:IndividualId] => id -> fromId <= id <= toId, residencies)
        getresidencydays(basedirectory,node,d,i)
    end
    combinedaybatch(basedirectory,node,"IndividualResidencyDays",batches)
    return nothing
end #batchresidencydays
#endregion
#region Household residency days
function extracthhresidencydays(node::String)
    processresidencydays(settings.BaseDirectory, node)
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
"Ensure household residency span household membership extends"
function normalisehouseholdresidencies(basedirectory::String, node::String)
    memberships = open(joinpath(basedirectory, node, "Staging", "HouseholdMemberships.arrow")) do io
        Arrow.Table(io) |> DataFrame
    end;
    m = combine(groupby(memberships,[:HouseholdId]),:StartDate => minimum => :MemStartDate, :EndDate => maximum => :MemEndDate)
    residencies = open(joinpath(basedirectory, node, "Staging", "HouseholdResidences.arrow")) do io
        Arrow.Table(io) |> DataFrame
    end;
    mr = leftjoin(residencies, m, on = :HouseholdId, makeunique = true)
    for i = 1:nrow(mr)
        episode = mr[i,:Episode]
        episodes = mr[i,:Episodes]
        if episode == 1 && !ismissing(mr[i,:MemStartDate]) && mr[i,:MemStartDate]<mr[i,:StartDate]
            mr[i,:StartDate]=mr[i,:MemStartDate]
        end
        if episode == episodes && !ismissing(mr[i,:MemEndDate]) && mr[i,:MemEndDate]>mr[i,:EndDate]
            mr[i,:EndDate]=mr[i,:MemEndDate]
        end
    end
    return mr
end
"Extract household residency days"
function processresidencydays(basedirectory::String, node::String)
    residencies = normalisehouseholdresidencies(basedirectory, node)
    select!(residencies,[:HouseholdId, :LocationId, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(residencies)) household residency episodes"
    s = combine(groupby(sort(residencies, [:StartDate, order(:EndDate, rev=true)]), [:HouseholdId], sort=true), [:LocationId, :StartDate, :EndDate, :StartType, :EndType] => processhhresidencydays => AsTable)
    rename!(s,Dict(:locationid => "LocationId", :daydate => "DayDate", :episode => "Episode", :starttype => "StartType", :endtype => "EndType", :startflag => "Start", :endflag => "End"))
    disallowmissing!(s, [:LocationId, :DayDate, :Episode, :StartType, :EndType, :Start, :End])
    Arrow.write(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays.arrow"), s, compress=:zstd)
    @info "Node $(node) $(nrow(s)) household residency days"
    return nothing
end #processresidencydays
#endregion
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
    open(joinpath(basedirectory,node,"DayExtraction","HouseholdMembershipDays$(batch).arrow"),"w"; lock = true) do io
        Arrow.write(io, s, compress=:zstd)
    end
    return nothing
end #gethhresidencydays
"Extract membership days in batches"
function batchmembershipdays(basedirectory::String, node::String, batchsize::Int64)
    minId, maxId, batches = individualbatch(basedirectory, node, batchsize)
    memberships = open(joinpath(basedirectory, node, "Staging", "IndividualMemberships.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    @info "Node $(node) $(nrow(memberships)) individual membership episodes"
    Threads.@threads for i = 1:batches
        fromId, toId = nextidrange(minId, maxId, batchsize, i)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:IndividualId] => id -> fromId <= id <= toId, memberships)
        gethhmembershipdays(basedirectory,node,d,i)
    end
    combinedaybatch(basedirectory,node,"HouseholdMembershipDays",batches)
    return nothing
end #batchmembershipdays
#endregion
#region Preferred Household
"Convert a Arrow file into an Arrow file with record batches based on IndividualId"
function batchonindividualid(basedirectory, node, file, batchsize)
    minId, maxId, numbatches = individualbatch(basedirectory, node, batchsize)
    df = Arrow.Table(joinpath(basedirectory,node,"DayExtraction","$(file).arrow"))
    @info "Read $(node) $(file) arrow table ", now()
    partitions = Array{TableOperations.Filter}(undef, 0)
    for i = 1:numbatches 
        fromId, toId = nextidrange(minId, maxId, batchsize, i)
        push!(partitions, TableOperations.filter(x -> fromId <= x.IndividualId <= toId, df))
    end
    open(joinpath(basedirectory,node,"DayExtraction","$(file)_batched.arrow"),"a"; lock = true) do io
        Arrow.write(io, Tables.partitioner(partitions), compress=:zstd)
    end
end
function batchmembershipdayswithlocation(basedirectory::String, node::String)
    hhresidencies = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays.arrow")) |> DataFrame
    @info "$(node) read $(nrow(hhresidencies)) household residency days at $(now())"
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
        @info "Wrote $(nrow(df)) membership with location rows, batch $(i) for node $(node)"
        batch = batch + 1
    end
    combinedaybatch(basedirectory,node,"HouseholdMembershipDaysWithLocation", batch-1)
    @info "Completed writing membership with location on $(now())"
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
    @info "$(node) batch $(batch) wrote $(nrow(df)) preferred household days at $(now())"
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
function preferredhousehold(basedirectory::String, node::String)
    @info "Started preferredhousehold execution for node $(node) at $(now())"
    batchmembershipdayswithlocation(basedirectory,node)
    consolidatepreferredhousehold(basedirectory,node)
    @info "Finished preferredhousehold execution for node $(node) at $(now())"
end
#endregion