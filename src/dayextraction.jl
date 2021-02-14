#region Individual residency days
function extractresidencydays(node::String, batchsize::Int64 = BatchSize)
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
        fromId, toId = nextidrange(minId, maxId, i, batchsize)
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
function extractmembershipdays(node::String, batchsize::Int64 = BatchSize)
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
        fromId, toId = nextidrange(minId, maxId, i, batchsize)
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
function batchonindividualid(basedirectory, node, file, batchsize = BatchSize)
    minId, maxId, numbatches = individualbatch(basedirectory, node, batchsize)
    df = Arrow.Table(joinpath(basedirectory,node,"DayExtraction","$(file).arrow"))
    @info "Read $(node) $(file) arrow table ", now()
    partitions = Array{TableOperations.Filter}(undef, 0)
    for i = 1:numbatches 
        fromId, toId = nextidrange(minId, maxId, i, batchsize)
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
        @info "Wrote $(nrow(hm)) membership with location rows, batch $(batch) for node $(node)"
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
    s = transform!(groupby(sort(df, [:IndividualId,:DayDate,:HHRank]),[:IndividualId,:DayDate]), :IndividualId => eachindex => :rank, nrow => :Memberships)
    filter!(x -> x.rank == 1, s)
    # sort!(df, [:IndividualId,:DayDate,:HHRank])
    # unique!(df,[:IndividualId,:DayDate])
    select!(s,[:IndividualId,:HouseholdId,:DayDate,:HHResLocationId,:IndResLocationId,:HHMemStartType, :HHMemEndType, :HHMemStart, :HHMemEnd, :IndResStartType, :IndResEndType, :Episode, :IndResStart, :IndResEnd, :HHRank, :HHRelationshipTypeId, :Memberships])
    open(joinpath(basedirectory, node, "DayExtraction", "IndividualPreferredHHDays$(batch).arrow"),"w"; lock = true) do io
        Arrow.write(io, s, compress=:zstd)
    end
    @info "$(node) batch $(batch) wrote $(nrow(s)) preferred household days at $(now())"
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
"Recreate membership days for residency days without a membership"
function recoverresidentdays(basedirectory::String, node::String)
    hhresidencies = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "HouseholdResidencyDays.arrow")) |> DataFrame
    @info "Read $(nrow(hhresidencies)) rows for Household Residency Days"
    resdaysnomembership = Arrow.Table(joinpath(basedirectory, node, "DayExtraction", "ResDaysNoMember_batched.arrow")) |> DataFrame
    @info "Read $(nrow(resdaysnomembership)) rows for resdays no membership"
    df = innerjoin(resdaysnomembership,hhresidencies; on = [:DayDate =>:DayDate , :IndResLocationId => :LocationId], makeunique = true)
    insertcols!(df, :HHRank =>  Int32(12), :HHRelationshipTypeId => Int32(12), :Memberships => Int64(1), :HHResLocationId => df[!,:IndResLocationId])
    rename!(df, Dict(:StartType => "HHMemStartType", :EndType => "HHMemEndType", :Start => "HHMemStart", :End => "HHMemEnd"))
    select!(df,[:IndividualId, :HouseholdId, :DayDate, :HHResLocationId, :IndResLocationId, :HHMemStartType, :HHMemEndType, :HHMemStart, :HHMemEnd, 
                :IndResStartType, :IndResEndType, :Episode, :IndResStart, :IndResEnd, :HHRank, :HHRelationshipTypeId, :Memberships])
    sort!(df,[:IndividualId,:DayDate,:HHRank,:HouseholdId])
    unique!(df,[:IndividualId,:DayDate])
    return df
end
"Consolidate daily exposure for an individual to a single preferred household based on relationship to household head"
function addresidentdayswithoutmembership(basedirectory::String, node::String)
    rd = recoverresidentdays(basedirectory, node)
    @info "Recovered $(nrow(rd)) residency days"
    preferredhhbatches = Arrow.Stream(joinpath(basedirectory, node, "DayExtraction", "IndividualPreferredHHDays_batched.arrow"))
    hstate = iterate(preferredhhbatches)
    i = 1
    minId, maxId, batches = individualbatch(basedirectory, node)
    while hstate !== nothing
        h, hst = hstate
        hd = copy(h |> DataFrame)
        fromId, toId = nextidrange(minId, maxId, i)
        r = filter([:IndividualId] => id -> fromId <= id <= toId, rd, view = true)
        @info "Days to be added $(nrow(r)) for batch $(i)"
        append!(hd, r)
        s = combine(groupby(hd, [:IndividualId, :DayDate]), :IndResLocationId => first => :IndResLocationId, 
                            :IndResStartType => first => :IndResStartType, :IndResEndType => first => :IndResEndType, :IndResStart => maximum => :IndResStart, :IndResEnd => maximum => :IndResEnd,
                            :HouseholdId => first => :HouseholdId, :HHResLocationId => first => :HHResLocationId, 
                            :HHMemStartType => first => :HHMemStartType, :HHMemEndType => first => :HHMemEndType, :HHMemStart => first => :HHMemStart, :HHMemEnd => first => :HHMemEnd,
                            :HHRelationshipTypeId => first => :HHRelationshipTypeId, :HHRank => first => :HHRank, :Memberships => sum => :Memberships)
        open(joinpath(basedirectory, node, "DayExtraction", "IndividualPreferredHHConsolidatedDays$(i).arrow"),"w"; lock = true) do io
            Arrow.write(io, s, compress=:zstd)
        end
        hstate = iterate(preferredhhbatches, hst)
        i = i + 1
    end
    combinedaybatch(basedirectory,node,"IndividualPreferredHHConsolidatedDays", i-1)
    return nothing
end
function preferredhousehold(node::String)
    @info "Started preferredhousehold execution for node $(node) at $(now())"
    batchmembershipdayswithlocation(settings.BaseDirectory, node)
    consolidatepreferredhousehold(settings.BaseDirectory, node)
    addresidentdayswithoutmembership(settings.BaseDirectory, node)
    @info "Finished preferredhousehold execution for node $(node) at $(now())"
end
#endregion
#region Residency flags
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
function setresidencyflags(node::String)
    ltfcutoff = settings.LTFCutOff
    basedirectory = settings.BaseDirectory
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
        @info "Node $(node) batch $(i) at $(t)"
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
        @info "Node $(node) batch $(i) completed after $(round(now()-t, Dates.Second))"
        i = i + 1
    end
    combinedaybatch(basedirectory,node,"DayDatasetStep01", i-1)
    return nothing
end
#endregion