#region Individual residency days
function extractresidencydays(node::String, batchsize::Int64=BatchSize)
    batchresidencydays(node, batchsize)
end
function processresidencydays(individualid, locationid, startdate, enddate, starttype, endtype)
    #println("Proccess days $(individualid) length $(length(startdate))")
    if length(startdate) == 0
        return (locationid=[], daydate=[], starttype=[], endtype=[], episode=[], startflag=[], endflag=[])
    end
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
    for i in 2:lastindex(startdate)
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
        # if  startidx>length(res_start)
        #     println(individualid)
        #     println(locationid) 
        #     println(startdate) 
        #     println(enddate) 
        # end
        res_start[startidx] = Int8(1)
        res_end[end] = Int8(1)
    end

    return (locationid=res_location, daydate=res_daydate, starttype=res_starttype, endtype=res_endtype, episode=res_episode, startflag=res_start, endflag=res_end)
end
function getresidencydays(node::String, f, batch::Int64)
    #println("Batch $(batch) $(nrow(f)) episodes to extract")
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId], sort=true), [:IndividualId, :LocationId, :StartDate, :EndDate, :StartType, :EndType] => processresidencydays => AsTable)
    rename!(s, Dict(:locationid => "LocationId", :daydate => "DayDate", :episode => "Episode", :starttype => "StartType", :endtype => "EndType", :startflag => "Start", :endflag => "End"))
    disallowmissing!(s, [:LocationId, :DayDate, :Episode, :StartType, :EndType, :Start, :End])
    serializetofile(joinpath(dayextractionpath(node), "IndividualResidencyDays$(batch)"), s)
    # open(joinpath(dayextractionpath(node), "IndividualResidencyDays$(batch).arrow"),"w"; lock = true) do io
    #     Arrow.write(io, s, compress=:zstd)
    # end
    return nothing
end #getresidencydays
"Normalise residencies in batches"
function batchresidencydays(node::String, batchsize::Int64)
    minId, maxId, batches = individualbatch(node, batchsize)
    residencies = open(joinpath(stagingpath(node), "IndividualResidencies.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(residencies, [:IndividualId, :LocationId, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(residencies)) residency episodes"
    sort!(residencies, [:IndividualId, :StartDate])
    Threads.@threads for i = 1:batches #May cause julia termination
        # for i = 1:batches
        fromId, toId = nextidrange(minId, maxId, i, batchsize)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:IndividualId] => id -> fromId <= id <= toId, residencies)
        getresidencydays(node, d, i)
    end
    return nothing
end #batchresidencydays
#endregion
#region Household residency days
function extracthhresidencydays(node::String)
    @info "Started extracting household residencydays for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    processhousholdresidencydays(node)
    @info "=== Finished extracting household residencydays for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
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
    for i in 2:lastindex(startdate)
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

    return (locationid=res_location, daydate=res_daydate, starttype=res_starttype, endtype=res_endtype, episode=res_episode, startflag=res_start, endflag=res_end)
end
"Ensure household residency span household membership extends"
function normalisehouseholdresidencies(node::String)
    memberships = open(joinpath(stagingpath(node), "HouseholdMemberships.arrow")) do io
        Arrow.Table(io) |> DataFrame
    end
    m = combine(groupby(memberships, [:HouseholdId]), :StartDate => minimum => :MemStartDate, :EndDate => maximum => :MemEndDate)
    residencies = open(joinpath(stagingpath(node), "HouseholdResidences.arrow")) do io
        Arrow.Table(io) |> DataFrame
    end
    mr = leftjoin(residencies, m, on=:HouseholdId, makeunique=true)
    for i = 1:nrow(mr)
        episode = mr[i, :Episode]
        episodes = mr[i, :Episodes]
        if episode == 1 && !ismissing(mr[i, :MemStartDate]) && mr[i, :MemStartDate] < mr[i, :StartDate]
            mr[i, :StartDate] = mr[i, :MemStartDate]
        end
        if episode == episodes && !ismissing(mr[i, :MemEndDate]) && mr[i, :MemEndDate] > mr[i, :EndDate]
            mr[i, :EndDate] = mr[i, :MemEndDate]
        end
    end
    return mr
end
"Extract household residency days"
function processhousholdresidencydays(node::String)
    residencies = normalisehouseholdresidencies(node)
    select!(residencies, [:HouseholdId, :LocationId, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(residencies)) household residency episodes"
    s = combine(groupby(sort(residencies, [:StartDate, order(:EndDate, rev=true)]), [:HouseholdId], sort=true), [:LocationId, :StartDate, :EndDate, :StartType, :EndType] => processhhresidencydays => AsTable)
    rename!(s, Dict(:locationid => "LocationId", :daydate => "DayDate", :episode => "Episode", :starttype => "StartType", :endtype => "EndType", :startflag => "Start", :endflag => "End"))
    disallowmissing!(s, [:LocationId, :DayDate, :Episode, :StartType, :EndType, :Start, :End])
    Arrow.write(joinpath(dayextractionpath(node), "HouseholdResidencyDays.arrow"), s, compress=:zstd)
    @info "Node $(node) $(nrow(s)) household residency days"
    return nothing
end #processresidencydays
#endregion
#region Household Membership days
function extractmembershipdays(node::String, batchsize::Int64=BatchSize)
    @info "Started extractmembershipdays for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    batchmembershipdays(node, batchsize)
    @info "=== Finished extractmembershipdays for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
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
    for i in 2:lastindex(startdate)
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

    return (daydate=res_daydate, starttype=res_starttype, endtype=res_endtype, episode=res_episode, startflag=res_start, endflag=res_end)
end #processhhmembershipdays
function gethhmembershipdays(node::String, f, batch::Int64)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId, :HouseholdId, :HHRelationshipTypeId], sort=true), [:StartDate, :EndDate, :StartType, :EndType] => processhhmembershipdays => AsTable)
    rename!(s, Dict(:daydate => "DayDate", :episode => "Episode", :starttype => "StartType", :endtype => "EndType", :startflag => "Start", :endflag => "End"))
    disallowmissing!(s, [:DayDate, :Episode, :StartType, :EndType, :Start, :End])
    serializetofile(joinpath(dayextractionpath(node), "HouseholdMembershipDays$(batch)"), s)
    # open(joinpath(dayextractionpath(node),"HouseholdMembershipDays$(batch).arrow"),"w"; lock = true) do io
    #     Arrow.write(io, s, compress=:zstd)
    # end
    return nothing
end #gethhresidencydays
"Extract membership days in batches"
function batchmembershipdays(node::String, batchsize::Int64)
    minId, maxId, batches = individualbatch(node, batchsize)
    memberships = open(joinpath(stagingpath(node), "IndividualMemberships.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    @info "Node $(node) $(nrow(memberships)) individual membership episodes"
    Threads.@threads for i = 1:batches
        fromId, toId = nextidrange(minId, maxId, i, batchsize)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:IndividualId] => id -> fromId <= id <= toId, memberships)
        gethhmembershipdays(node, d, i)
    end
    return nothing
end #batchmembershipdays
#endregion
#region Preferred Household
"Convert a Arrow file into an Arrow file with record batches based on IndividualId"
function batchonindividualid(node, file, batchsize=BatchSize)
    minId, maxId, numbatches = individualbatch(node, batchsize)
    df = Arrow.Table(joinpath(dayextractionpath(node), "$(file).arrow"))
    @info "Read $(node) $(file) arrow table ", now()
    partitions = Array{TableOperations.Filter}(undef, 0)
    for i = 1:numbatches
        fromId, toId = nextidrange(minId, maxId, i, batchsize)
        push!(partitions, TableOperations.filter(x -> fromId <= x.IndividualId <= toId, df))
    end
    open(joinpath(dayextractionpath(node), "$(file)_batched.arrow"), "a"; lock=true) do io
        Arrow.write(io, Tables.partitioner(partitions), compress=:zstd)
    end
end
"Create arrow file with memberships that include household location"
function batchmembershipdayswithlocation(node::String)
    hhresidencies = Arrow.Table(joinpath(dayextractionpath(node), "HouseholdResidencyDays.arrow")) |> DataFrame
    @info "Read $(nrow(hhresidencies)) household residency days for node $(node) at $(now())"
    #membershipdaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "HouseholdMembershipDays_batched.arrow"))
    minId, maxId, numbatches = individualbatch(node)
    for batch = 1:numbatches
        memberships = deserializefromfile(joinpath(dayextractionpath(node), "HouseholdMembershipDays$(batch)"))
        hm = innerjoin(memberships, hhresidencies, on=[:HouseholdId, :DayDate], makeunique=true)
        select!(hm, [:HouseholdId, :DayDate, :IndividualId, :LocationId, :StartType, :EndType, :Start, :End, :HHRelationshipTypeId])
        rename!(hm, [:LocationId => :HHResLocationId, :StartType => :HHMemStartType, :EndType => :HHMemEndType, :Start => :HHMemStart, :End => :HHMemEnd])
        serializetofile(joinpath(dayextractionpath(node), "HouseholdMembershipDaysWithLocation$(batch)"), hm)
        # open(joinpath(dayextractionpath(node),"HouseholdMembershipDaysWithLocation$(batch).arrow"),"w"; lock = true) do io
        #     Arrow.write(io, hm, compress=:zstd)
        # end
        @info "Wrote $(nrow(hm)) membership with location rows, batch $(batch) for node $(node)"
        batch = batch + 1
    end
    @info "Completed writing membership with location on $(now())"
    deletebatchfiles(dayextractionpath(node), "HouseholdMembershipDays", numbatches)
    return nothing
end
"Process household membership batch to allocate preferred household"
function processconsolidatehhbatch(node, md, rd, batch)
    rename!(rd, [:LocationId => :IndResLocationId, :StartType => :IndResStartType, :EndType => :IndResEndType, :Start => :IndResStart, :End => :IndResEnd])
    df = outerjoin(md, rd, on=[:IndividualId, :DayDate], makeunique=true, indicator=:result)
    s = select(filter(x -> x.result == "right_only", df), [:IndividualId, :DayDate, :IndResLocationId, :IndResStartType, :IndResEndType, :IndResStart, :IndResEnd])
    serializetofile(joinpath(dayextractionpath(node), "ResDaysNoMember$(batch)"), s)
    # open(joinpath(dayextractionpath(node), "ResDaysNoMember$(batch).arrow"), "w"; lock=true) do io
    #     Arrow.write(io, select(filter(x -> x.result == "right_only", df), [:IndividualId, :DayDate, :IndResLocationId, :IndResStartType, :IndResEndType, :IndResStart, :IndResEnd]), compress=:zstd)
    # end
    filter!(x -> x.result != "right_only", df)
    insertcols!(df, :HHRelationshipTypeId, :HHRank => Int32(999))
    for i = 1:nrow(df)
        if ismissing(df[i, :IndResLocationId])
            df[i, :HHRank] = df[i, :HHRelationshipTypeId] + Int32(100)
        elseif df[i, :IndResLocationId] == df[i, :HHResLocationId]
            df[i, :HHRank] = df[i, :HHRelationshipTypeId]
        else
            df[i, :HHRank] = df[i, :HHRelationshipTypeId] + Int32(100)
        end
    end
    sort!(df, [:IndividualId, :DayDate, :HHRank])
    s = transform(groupby(df, [:IndividualId, :DayDate]; sort=true), :IndividualId => eachindex => :rank, nrow => :Memberships)
    subset!(s, :rank => ByRow(x -> x == 1))
    select!(s, [:IndividualId, :HouseholdId, :DayDate, :HHResLocationId, :IndResLocationId, :HHMemStartType, :HHMemEndType, :HHMemStart,
        :HHMemEnd, :IndResStartType, :IndResEndType, :Episode, :IndResStart, :IndResEnd, :HHRank, :HHRelationshipTypeId, :Memberships])
    serializetofile(joinpath(dayextractionpath(node), "IndividualPreferredHHDays$(batch).zjls"), s)
    # open(joinpath(dayextractionpath(node), "IndividualPreferredHHDays$(batch).arrow"), "w"; lock=true) do io
    #     Arrow.write(io, s, compress=:zstd)
    # end
    @info "$(node) batch $(batch) wrote $(nrow(s)) preferred household days at $(now())"
end
"Produce day file with with preferred household of household member for each day"
function consolidatepreferredhousehold(node::String)
    minId, maxId, numbatches = individualbatch(node)
    for batch = 1:numbatches
        md = deserializefromfile(joinpath(dayextractionpath(node), "HouseholdMembershipDaysWithLocation$(batch)"))
        rd = deserializefromfile(joinpath(dayextractionpath(node), "IndividualResidencyDays$(batch)"))
        processconsolidatehhbatch(node, md, rd, batch)
    end
    deletebatchfiles(dayextractionpath(node), "HouseholdMembershipDaysWithLocation", numbatches)
    deletebatchfiles(dayextractionpath(node), "IndividualResidencyDays", numbatches)
    return nothing
end
"Recreate membership days for residency days without a membership"
function recoverresidentdays(node::String)
    minId, maxId, numbatches = individualbatch(node)
    hhresidencies = Arrow.Table(joinpath(dayextractionpath(node), "HouseholdResidencyDays.arrow")) |> DataFrame
    @info "Read $(nrow(hhresidencies)) rows for Household Residency Days"
    resdaysnomembership = restoredataframe(dayextractionpath(node), "ResDaysNoMember", numbatches) #Arrow.Table(joinpath(dayextractionpath(node), "ResDaysNoMember_batched.arrow")) |> DataFrame
    @info "Read $(nrow(resdaysnomembership)) rows for resdays no membership"
    df = innerjoin(resdaysnomembership, hhresidencies; on=[:DayDate => :DayDate, :IndResLocationId => :LocationId], makeunique=true)
    insertcols!(df, :HHRank => Int32(12), :HHRelationshipTypeId => Int32(12), :Memberships => Int64(1), :HHResLocationId => df[!, :IndResLocationId])
    rename!(df, Dict(:StartType => "HHMemStartType", :EndType => "HHMemEndType", :Start => "HHMemStart", :End => "HHMemEnd"))
    select!(df, [:IndividualId, :HouseholdId, :DayDate, :HHResLocationId, :IndResLocationId, :HHMemStartType, :HHMemEndType, :HHMemStart, :HHMemEnd,
        :IndResStartType, :IndResEndType, :Episode, :IndResStart, :IndResEnd, :HHRank, :HHRelationshipTypeId, :Memberships])
    sort!(df, [:IndividualId, :DayDate, :HHRank, :HouseholdId])
    unique!(df, [:IndividualId, :DayDate])
    deletebatchfiles(dayextractionpath(node), "ResDaysNoMember", numbatches)
    return df
end
"Consolidate daily exposure for an individual to a single preferred household based on relationship to household head"
function addresidentdayswithoutmembership(node::String)
    rd = recoverresidentdays(node)
    @info "Recovered $(nrow(rd)) residency days"
    # preferredhhbatches = Arrow.Stream(joinpath(dayextractionpath(node), "IndividualPreferredHHDays_batched.arrow"))
    # hstate = iterate(preferredhhbatches)
    # i = 1
    minId, maxId, numbatches = individualbatch(node)
    for batch = 1:numbatches
        hd = deserializefromfile(joinpath(dayextractionpath(node), "IndividualPreferredHHDays$(batch)"))
        fromId, toId = nextidrange(minId, maxId, batch)
        r = filter([:IndividualId] => id -> fromId <= id <= toId, rd, view=true)
        @info "Days to be added $(nrow(r)) for batch $(batch)"
        append!(hd, r)
        s = combine(groupby(hd, [:IndividualId, :DayDate]), :IndResLocationId => first => :IndResLocationId,
            :IndResStartType => first => :IndResStartType, :IndResEndType => first => :IndResEndType, :IndResStart => maximum => :IndResStart, :IndResEnd => maximum => :IndResEnd,
            :HouseholdId => first => :HouseholdId, :HHResLocationId => first => :HHResLocationId,
            :HHMemStartType => first => :HHMemStartType, :HHMemEndType => first => :HHMemEndType, :HHMemStart => first => :HHMemStart, :HHMemEnd => first => :HHMemEnd,
            :HHRelationshipTypeId => first => :HHRelationshipTypeId, :HHRank => first => :HHRank, :Memberships => sum => :Memberships)
        sort!(s, [:IndividualId, :DayDate])
        serializetofile(joinpath(dayextractionpath(node), "IndividualPreferredHHConsolidatedDays$(batch).zjls"), s)
        # open(joinpath(dayextractionpath(node), "IndividualPreferredHHConsolidatedDays$(i).arrow"), "w"; lock=true) do io
        #     Arrow.write(io, s, compress=:zstd)
        # end
        # hstate = iterate(preferredhhbatches, hst)
        # i = i + 1
    end
    deletebatchfiles(dayextractionpath(node), "IndividualPreferredHHDays", numbatches)
    return nothing
end
function preferredhousehold(node::String)
    @info "Started preferredhousehold execution for node $(node) at $(now())"
    batchmembershipdayswithlocation(node)
    consolidatepreferredhousehold(node)
    addresidentdayswithoutmembership(node)
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
    if r.GapEnd == 1 && r.StartType == 301
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
"Determine whether the refusal end flag should be set"
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
"Determine whether the out-migration end flag should be set"
function outmigrationflag(r, outtypes)
    if (ismissing(r.NextResident) && r.Resident == 1 && in(r.EndType, outtypes)) ||
       (r.Resident == 1 && r.GapStart == 1 && in(r.EndType, outtypes)) ||
       (!ismissing(r.NextResident) && r.NextResident == 0 && r.Resident == 1 && in(r.EndType, outtypes))
        return Int8(1)
    else
        return Int8(0)
    end
end
"Determine whether the end of external residency end flag should be set"
function extresendflag(r, outtypes)
    if (ismissing(r.NextResident) && r.Resident == 0 && in(r.EndType, outtypes)) ||
       (r.Resident == 0 && r.GapStart == 1 && in(r.EndType, outtypes)) ||
       (!ismissing(r.NextResident) && r.NextResident == 1 && r.Resident == 0 && r.LostToFollowUp == 0)
        return Int8(1)
    else
        return Int8(0)
    end
end
"Determine whether the current member end flag should be set"
function currentflag(r, visittypes, ltfcutoff::DateTime)
    if (ismissing(r.NextResident) && r.DayDate >= ltfcutoff && in(r.EndType, visittypes)) ||
       (!ismissing(r.NextResident) && r.NextResident == 0 && r.Resident == 1 && r.DayDate >= ltfcutoff && in(r.EndType, visittypes))
        return Int8(1)
    else
        return Int8(0)
    end
end
"Determine whether the location exit end flag should be set"
function locationexitflag(r)
    if (r.Died == 0 && r.Refusal == 0 && r.LostToFollowUp == 0 && r.OutMigration == 0 && r.ExtResEnd == 0) &&
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
    return nothing
end
"Determine whether a particular day starts or end a residency episode and the type of start or end"
function setresidencyflags(node::String)
    ltfcutoff = settings.LTFCutOff
    birthtypes = Int32[2, 10] # BTH, DLV
    outtypes = Int32[4, 5, 101, 103, 104] # OMG, EXT, HDS, HME, HRC
    intypes = Int32[3, 6, 100, 102] # IMG, OMG, HFM, HMS
    visittypes = Int32[9, 18, 19] # OBE, OBS
    minId, maxId, numbatches = individualbatch(node)
    for batch = 1:numbatches
        t = now()
        @info "Node $(node) batch $(batch) at $(t)"
        hd = deserializefromfile(joinpath(dayextractionpath(node), "IndividualPreferredHHConsolidatedDays$(batch)"))
        df = transform(hd, AsTable([:IndResLocationId, :HHResLocationId]) => ByRow(x -> ismissing(x.IndResLocationId) ? x.HHResLocationId : x.IndResLocationId) => :LocationId,
            :IndResLocationId => ByRow(x -> ismissing(x) ? Int8(0) : Int8(1)) => :Resident,
            AsTable([:IndResStartType, :HHMemStartType]) => ByRow(x -> ismissing(x.IndResStartType) ? x.HHMemStartType : x.IndResStartType) => :StartType,
            AsTable([:IndResEndType, :HHMemEndType]) => ByRow(x -> ismissing(x.IndResEndType) ? x.HHMemEndType : x.IndResEndType) => :EndType)
        select!(df, Not([:IndResLocationId, :HHResLocationId, :IndResStartType, :HHMemStartType, :IndResEndType, :HHMemEndType, :IndResStart, :IndResEnd, :HHMemStart, :HHMemEnd]))
        s = combine(groupby(sort!(df, [:IndividualId, :DayDate]), :IndividualId), :DayDate, :LocationId, :HouseholdId, :HHRelationshipTypeId, :Resident, :StartType, :EndType, :Memberships,
            :DayDate => ShiftedArrays.lead => :NextDay, :DayDate => ShiftedArrays.lag => :PrevDay,
            :StartType => ShiftedArrays.lead => :NextStart, :StartType => ShiftedArrays.lag => :PrevStart,
            :EndType => ShiftedArrays.lag => :PrevEnd,
            :Resident => ShiftedArrays.lead => :NextResident, :Resident => ShiftedArrays.lag => :PrevResident,
            :LocationId => ShiftedArrays.lead => :NextLocation, :LocationId => ShiftedArrays.lag => :PrevLocation,
            :HouseholdId => ShiftedArrays.lead => :NextHousehold, :HouseholdId => ShiftedArrays.lag => :PrevHousehold)
        insertcols!(s, :GapStart => Int8(0), :GapEnd => Int8(0),
            :Born => Int8(0), :Enumeration => Int8(0), :InMigration => Int8(0), :ExtResStart => Int8(0), :Participation => Int8(0), :LocationEntry => Int8(0),
            :Died => Int8(0), :Refusal => Int8(0), :LostToFollowUp => Int8(0), :OutMigration => Int8(0), :ExtResEnd => Int8(0), :LocationExit => Int8(0), :Current => Int8(0),
            :MembershipStart => Int8(0), :MembershipEnd => Int8(0), :Episode => Int8(0))
        episode = Int8(0)
        for row in eachrow(s)
            setdayflags!(row, ltfcutoff, birthtypes, intypes, visittypes, outtypes)
            # set episode
            if ismissing(row.PrevDay)
                episode = Int8(1)
                row.Episode = episode
            elseif row.Born == 1 || row.Enumeration == 1 || row.InMigration == 1 || row.ExtResStart == 1 || row.Participation == 1 || row.LocationEntry == 1 || row.MembershipStart == 1
                episode = episode + Int8(1)
                row.Episode = episode
            else
                row.Episode = episode
            end
        end
        select!(s, [:IndividualId, :DayDate, :HouseholdId, :LocationId, :Resident, :Episode,
            :Enumeration, :Born, :Participation, :InMigration, :LocationEntry, :ExtResStart,
            :Died, :Refusal, :LostToFollowUp, :Current, :OutMigration, :LocationExit, :ExtResEnd, :MembershipStart, :MembershipEnd,
            :HHRelationshipTypeId, :Memberships, :GapStart])
        serializetofile(joinpath(dayextractionpath(node), "DayDatasetStep01$(batch)"), s)
        @info "Node $(node) batch $(batch) completed after $(round(now()-t, Dates.Second))"
    end
    deletebatchfiles(dayextractionpath(node), "IndividualPreferredHHConsolidatedDays", numbatches)
    @info "=== Finished setresidencyflags for $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
#endregion
#region Individual Attributes
"Set individual parent alive or dead status"
function setparentstatus!(r)
    if ismissing(r.MotherDoD)
        r.MotherDead = Int8(-1)
    elseif r.MotherDoD <= r.DayDate
        r.MotherDead = Int8(1)
    end
    if ismissing(r.FatherDoD)
        r.FatherDead = Int8(-1)
    elseif r.FatherDoD <= r.DayDate
        r.FatherDead = Int8(1)
    end
end
"Add individual characteristics to day records"
function addindividualattributes(node)
    @info "Started addindividualattributes for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    individuals = Arrow.Table(joinpath(stagingpath(node), "Individuals.arrow")) |> DataFrame
    minId, maxId, numbatches = individualbatch(node)
    #residentdaybatches = Arrow.Stream(joinpath(dayextractionpath(node), "DayDatasetStep01_batched.arrow"))
    for batch = 1:numbatches
        t = now()
        @info "Node $(node) batch $(batch) at $(t)"
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep01$(batch)"))
        df = innerjoin(hd, individuals, on=:IndividualId)
        filter!([:DoB, :DayDate] => (x, y) -> y >= x && x > Date(1800), df)               #No days before birth
        filter!([:DoD, :DayDate] => (x, y) -> ismissing(x) || (!ismissing(x) && (y <= x)), df) #No days after death
        insertcols!(df, :MotherDead => Int8(0), :FatherDead => Int8(0))
        for row in eachrow(df)
            setparentstatus!(row)
            # Set Born flag if DayDate == DoB
            if row.DayDate == row.DoB
                row.Born = Int8(1)
            end
            # Set Died flag if DayDate == DoD
            if !ismissing(row.DoD) && row.DoD == row.DayDate
                row.Died = Int8(1)
            end
        end
        select!(df, [:IndividualId, :Sex, :DoB, :DoD, :MotherId, :MotherDead, :FatherId, :FatherDead, :DayDate, :HouseholdId, :LocationId, :Resident, :Episode,
            :Enumeration, :Born, :Participation, :InMigration, :LocationEntry, :ExtResStart,
            :Died, :Refusal, :LostToFollowUp, :Current, :OutMigration, :LocationExit, :ExtResEnd, :MembershipStart, :MembershipEnd,
            :HHRelationshipTypeId, :Memberships, :GapStart])
        serializetofile(joinpath(dayextractionpath(node), "DayDatasetStep02$(batch)"), df)
        # open(joinpath(dayextractionpath(node), "DayDatasetStep02$(i).arrow"), "w"; lock=true) do io
        #     Arrow.write(io, df, compress=:zstd)
        # end
        @info "Node $(node) batch $(batch) completed after $(round(now()-t, Dates.Second))"
     end
    deletebatchfiles(dayextractionpath(node), "DayDatasetStep01", numbatches)
    @info "=== Finished addindividualattributes for node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
#endregion
#region Delivery Days
"Generate delivery days"
function processdeliverydays(motherId, deliveryDate, nextDelivery, endDate, childrenBorn, childrenEverBorn)
    stop = endDate[1]
    if !ismissing(nextDelivery[1])
        stop = nextDelivery[1] - Day(1)
    end
    if endDate[1] < stop
        stop = endDate[1]
    end
    start = deliveryDate[1]
    # if motherId[1] == 3452
    #     println("Delivery dates $(deliveryDate)")
    #     println("MotherDoD $(motherDoD)")
    #     println("start $(start) stop $(stop)")
    # end
    res_daydate = collect(start:Day(1):stop)
    res_childrenBorn = fill(0, length(res_daydate))
    res_childrenBorn[1] = childrenBorn[1]
    res_childrenEverBorn = fill(childrenEverBorn[1], length(res_daydate))
    for i in 2:lastindex(deliveryDate)
        stop = endDate[i]
        if !ismissing(nextDelivery[i])
            stop = nextDelivery[i] - Day(1)
        end
        if endDate[i] < stop
            stop = endDate[i]
        end
        if deliveryDate[i] > res_daydate[end]
            start = deliveryDate[i]
        elseif nextDelivery[i] > res_daydate[end]
            start = res_daydate[end] + Day(1)
        else
            continue #this episode is contained within the previous episode
        end
        # if motherId[i] == 3452
        #     println("MotherDoD $(motherDoD)")
        #     println("start $(start) stop $(stop)")
        # end
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        new_childrenborn = fill(0, length(new_daydate))
        new_childrenborn[1] = childrenBorn[i]
        append!(res_childrenBorn, new_childrenborn)
        append!(res_childrenEverBorn, fill(childrenEverBorn[i], length(new_daydate)))
    end
    return (daydate=res_daydate, childrenBorn=res_childrenBorn, childrenEverBorn=res_childrenEverBorn)
end
"Get delivery days"
function getdeliverydays(node::String, f, batch::Int64)
    @info "Delivery day node $(node) batch $(batch) $(nrow(f)) episodes to extract"
    dfs = sort(f, [:DeliveryDate])
    dropmissing!(dfs, [:IndividualId])
    gdf = groupby(dfs, :IndividualId, sort=true)
    s = combine(gdf, [:IndividualId, :DeliveryDate, :NextDelivery, :EndDate, :LiveBirths, :ChildrenEverBorn] => processdeliverydays => AsTable)
    rename!(s, Dict(:daydate => "DayDate", :childrenBorn => "ChildrenBorn", :childrenEverBorn => "ChildrenEverBorn"))
    serializetofile(joinpath(dayextractionpath(node), "DeliveryDays$(batch).zjls"), s)
    # open(joinpath(dayextractionpath(node), "DeliveryDays$(batch).arrow"), "w"; lock=true) do io
    #     Arrow.write(io, s, compress=:zstd)
    # end
    return nothing
end
#Create deliveries dataset
"Extract delivery day data"
function deliverydays(node)
    @info "Started deliverydays node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    pregnancies = open(joinpath(stagingpath(node), "Pregnancies.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(pregnancies, Not([:StillBirths, :TerminationTypeId]))
    transform!(groupby(pregnancies, [:IndividualId]), :LiveBirths => cumsum => :ChildrenEverBorn)
    # Get Start and End bounds from episodes
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesBasic.arrow")) |> DataFrame
    bounds = combine(groupby(df, :IndividualId), :StartDate => minimum => :StartDate, :EndDate => maximum => :EndDate)
    deliveries = innerjoin(pregnancies, bounds, on=:IndividualId => :IndividualId)
    subset!(deliveries, :LiveBirths => x -> x .> 0)
    sort!(deliveries, [:IndividualId, :DeliveryDate])
    transform!(groupby(deliveries, :IndividualId), :DeliveryDate => ShiftedArrays.lead => :NextDelivery)
    # Deliveries prior to first observation of mother
    earlydeliveries = subset(deliveries, [:DeliveryDate, :StartDate] => (x, y) -> x .< y)
    select!(earlydeliveries, :IndividualId, :StartDate => :DeliveryDate, :LiveBirths, :ChildrenEverBorn, :StartDate, :EndDate, :NextDelivery)
    earlydeliveries.LiveBirths .= 0
    earlydeliveries = combine(groupby(earlydeliveries, :IndividualId), :DeliveryDate => first => :DeliveryDate, :LiveBirths => first => :LiveBirths,
        :ChildrenEverBorn => maximum => :ChildrenEverBorn, :StartDate => first => :StartDate,
        :EndDate => first => :EndDate, :NextDelivery => maximum => :NextDelivery)
    subset!(deliveries, [:DeliveryDate, :StartDate] => (x, y) -> x .>= y, [:DeliveryDate, :EndDate] => (x, y) -> x .<= y)
    deliveries = vcat(deliveries, earlydeliveries)
    sort!(deliveries, [:IndividualId, :DeliveryDate])
    minId, maxId, batches = individualbatch(node)
    for i = 1:batches
        fromId, toId = nextidrange(minId, maxId, i)
        @info "Batch $(i) from $(fromId) to $(toId)"
        d = filter([:IndividualId] => id -> fromId <= id <= toId, deliveries)
        getdeliverydays(node, d, i)
    end
    #combinebatches(dayextractionpath(node), "DeliveryDays", batches)
    @info "=== Finished deliverydays node $(node) $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
    return nothing
end
#endregion
#region Parent Coresidency
"Create the date bounds (earliest and latest dates) for the presence of children and their parents"
function familybounds(node::String)
    minId, maxId, numbatches = individualbatch(node)
    childbounds = DataFrame()
    for batch = 1:numbatches
        t = now()
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep02$(batch)"))
        cd = combine(groupby(hd, :IndividualId), :MotherId => maximum => :MotherId, :FatherId => maximum => :FatherId, :DayDate => minimum => :EarliestDate, :DayDate => maximum => :LatestDate)
        if batch == 1
            childbounds = cd
            allowmissing!(childbounds, [:MotherId, :FatherId])
        else
            append!(childbounds, cd)
        end
        @info "Node $(node) batch $(batch) completed with $(nrow(cd)) episodes after $(round(now()-t, Dates.Second))"
     end
    filter!([:MotherId, :FatherId] => (m, f) -> !(ismissing(m) && ismissing(f)), childbounds) #exclude children with no known parents
    @info "Node $(node) $(nrow(childbounds)) children with one or more parent"
    Arrow.write(joinpath(dayextractionpath(node), "ChildBounds.arrow"), childbounds, compress=:zstd)
    motherbounds = combine(groupby(childbounds, :MotherId), :EarliestDate => minimum => :EarliestDate, :LatestDate => maximum => :LatestDate)
    sort!(motherbounds, :MotherId)
    @info "Node $(node) $(nrow(motherbounds)) mothers"
    Arrow.write(joinpath(dayextractionpath(node), "MotherBounds.arrow"), motherbounds, compress=:zstd)
    fatherbounds = combine(groupby(childbounds, :FatherId), :EarliestDate => minimum => :EarliestDate, :LatestDate => maximum => :LatestDate)
    sort!(fatherbounds, :FatherId)
    @info "Node $(node) $(nrow(fatherbounds)) fathers"
    Arrow.write(joinpath(dayextractionpath(node), "FatherBounds.arrow"), fatherbounds, compress=:zstd)
    return nothing
end
"Create parent day file for only those days during which the parent is known to have had a child. col is the column name for MotherId or FatherId"
function parentdays(node::String, col::String)
    minId, maxId, numbatches = individualbatch(node)
    parentbounds = Arrow.Table(joinpath(dayextractionpath(node), "$(col[1:6])Bounds.arrow")) |> DataFrame
    @info names(parentbounds)
    days = DataFrame()
    for batch = 1:numbatches
    #while hstate !== nothing
        t = now()
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep02$(batch)"))
        pd = innerjoin(hd, parentbounds, on=:IndividualId => Symbol(col), matchmissing=:notequal)
        filter!([:DayDate, :EarliestDate, :LatestDate] => (d, e, l) -> e <= d <= l, pd)
        select!(pd, :IndividualId => Symbol(col), :DayDate, :LocationId => :ParentLocation)
        if batch == 1
            days = pd
        else
            append!(days, pd)
        end
        @info "Node $(node) batch $(batch) completed with $(nrow(pd)) days after $(round(now()-t, Dates.Second))"
    end
    @info "Node $(node) $(nrow(days)) $(eval(col)[1:6]) days"
    Arrow.write(joinpath(dayextractionpath(node), "$(col[1:6])Days.arrow"), days, compress=:zstd)
    return nothing
end
function samelocation(a, b)
    if ismissing(a) || ismissing(b)
        return Int8(0)
    else
        return a == b ? Int8(1) : Int8(0)
    end
end
"Add flag to individual days indicating parent presence in the same location as the child"
function parentcoresidence(node::String, col::String, step::Integer)
    minId, maxId, numbatches = individualbatch(node)
    parentdays = Arrow.Table(joinpath(dayextractionpath(node), "$(col[1:6])Days.arrow")) |> DataFrame
    @info names(parentdays)
    parentcol = Symbol("$(col[1:6])CoResident")
    for batch = 1:numbatches
        t = now()
        hd = deserializefromfile(joinpath(dayextractionpath(node), "DayDatasetStep$(lpad(step, 2, '0'))$(batch)"))
        pd = leftjoin(hd, parentdays, on=[Symbol(col), :DayDate], matchmissing=:notequal, makeunique=true)
        transform!(pd, [:LocationId, :ParentLocation] => ((x, y) -> samelocation.(x, y)) => parentcol)
        #=
        insertcols!(pd,findfirst(occursin.(names(pd),"$(col[1:6])Dead")), parentcol => Int8(0))
        for j = 1:nrow(pd)
            if !ismissing(pd[j, :ParentLocation]) && pd[j, :LocationId] == pd[j, :ParentLocation]
                pd[j,parentcol] = Int8(1)
            end
        end
        =#
        select!(pd, Not(:ParentLocation))
        serializetofile(joinpath(dayextractionpath(node), "DayDatasetStep$(lpad(step+1, 2, '0'))$(batch)"), pd)
        @info "Node $(node) batch $(batch) completed with $(nrow(pd)) days after $(round(now()-t, Dates.Second))"
    end
    deletebatchfiles(dayextractionpath(node), "DayDatasetStep$(lpad(step, 2, '0'))", numbatches)
    return nothing
end
"Add a mother co-residency flag to the day extraction dataset"
function mothercoresident(node::String)
    familybounds(node)
    parentdays(node, "MotherId")
    parentcoresidence(node, "MotherId", 2)
end
"""Add a father co-residency flag to the day extraction dataset.
   This function must be run after the mothercoresident function.
"""
function fathercoresident(node::String)
    parentdays(node, "FatherId")
    parentcoresidence(node, "FatherId", 3)
end
#endregion
