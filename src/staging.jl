#region individuals
"Read individuals and anonimise for specified node specified and save individual data, id map and bounds to to arrow files"
function readindividuals(node::String, io)
    readindividuals_internal(settings.Databases[node], node, io)
    individualobservationbounds(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end

"Read individuals and anonimise for node specified in settings and save id map and individual data to to arrow files"
function readindividuals_internal(db::String, node::String, io)
    con = ODBC.Connection(db)
    sql = """WITH Deaths AS (
        SELECT
          IR.IndividualUid,
          EE.EventDate DoD
        FROM dbo.IndividualResidences IR
          JOIN dbo.Events EE ON IR.EndEventUid = EE.EventUid
        WHERE EE.EventTypeId = 7
        UNION
        SELECT
          IR.IndividualUid,
          EE.EventDate DoD
        FROM dbo.HouseholdMemberships IR
          JOIN dbo.Events EE ON IR.EndEventUid = EE.EventUid
        WHERE EE.EventTypeId = 7
    ),
    IndividualDeaths AS (
      SELECT
        IndividualUid,
        MAX(DoD) DoD
      FROM Deaths
      GROUP BY IndividualUid
    )
    SELECT
        UPPER(CONVERT(nvarchar(50),I.IndividualUid)) IndividualUid,
        I.Sex,
        CONVERT(date,SE.EventDate) DoB,
        CASE
        WHEN EE.EventTypeId=7 THEN CONVERT(date,EE.EventDate)
        WHEN NOT ID.DoD IS NULL THEN CONVERT(date,ID.DoD)
        ELSE NULL
        END DoD,
        UPPER(CONVERT(nvarchar(50),I.MotherUid)) MotherUid,
        UPPER(CONVERT(nvarchar(50),I.FatherUid)) FatherUid,
        I.MotherDoD,
        I.FatherDoD
    FROM dbo.Individuals I
        JOIN dbo.Events SE ON I.BirthEventUid=SE.EventUid
        JOIN dbo.Events EE ON I.EndEventUid=EE.EventUid
        LEFT JOIN IndividualDeaths ID ON I.IndividualUid = ID.IndividualUid
    """
    individuals = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    @info "Read $(nrow(individuals)) $(node) individuals"
    sex = frequency(individuals, :Sex)
    @info "Sex breakdown $(node)"
    pretty_table(io, sex; alignment=[:c, :r], show_subheader=false)
    sort!(individuals, :IndividualUid)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),WomanUid)) WomanUid
        , UPPER(CONVERT(nvarchar(50),I.IndividualUid)) ChildUid
        FROM dbo.Pregnancies P
            JOIN dbo.Individuals I ON P.OutcomeEventUid=I.BirthEventUid
    """
    pregnancies = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(pregnancies)) $(node) pregnancies"
    pregnancies = unique!(pregnancies, :ChildUid)
    @info "Read $(nrow(pregnancies)) $(node) unique children"
    # Add MotherUid from pregnancies
    individuals = leftjoin(individuals, pregnancies, on=:IndividualUid => :ChildUid, makeunique=true, matchmissing=:equal)
    for i = 1:nrow(individuals)
        if ismissing(individuals[i, :MotherUid]) && !ismissing(individuals[i, :WomanUid])
            individuals[i, :MotherUid] = individuals[i, :WomanUid]
        end
    end
    individuals.IndividualId = 1:nrow(individuals)
    # Convert gui ids to integer ids
    map = individuals[!, [:IndividualUid, :IndividualId]]
    Arrow.write(joinpath(stagingpath(node), "IndividualMap.arrow"), map, compress=:zstd)
    # Convert mother and father uids to corresponding integer ids
    individuals = leftjoin(individuals, map, on=:MotherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    individuals = leftjoin(individuals, map, on=:FatherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    # Select and rename final columns
    select!(individuals, [:IndividualId, :Sex, :DoB, :DoD, :IndividualId_1, :IndividualId_2, :MotherDoD, :FatherDoD])
    rename!(individuals, :IndividualId_1 => :MotherId, :IndividualId_2 => :FatherId)
    # Fix parent DoDs
    # Mother DoD
    mothers = select(individuals, [:MotherId])
    dropmissing!(mothers)
    unique!(mothers)
    mothers = innerjoin(mothers, individuals, on=:MotherId => :IndividualId, makeunique=true, matchmissing=:equal)
    select!(mothers, [:MotherId, :DoD])
    rename!(mothers, :DoD => :MotherDoD)
    # Father DoD
    fathers = select(individuals, [:FatherId])
    dropmissing!(fathers)
    unique!(fathers)
    fathers = innerjoin(fathers, individuals, on=:FatherId => :IndividualId, makeunique=true, matchmissing=:equal)
    select!(fathers, [:FatherId, :DoD])
    rename!(fathers, :DoD => :FatherDoD)
    individuals = leftjoin(individuals, mothers, on=:MotherId => :MotherId, makeunique=true, matchmissing=:equal)
    individuals = leftjoin(individuals, fathers, on=:FatherId => :FatherId, makeunique=true, matchmissing=:equal)
    for i = 1:nrow(individuals)
        if !ismissing(individuals[i, :MotherId]) #Always overwrite recorded DoD with linked mother DoD
            individuals[i, :MotherDoD] = individuals[i, :MotherDoD_1]
        end
        if !ismissing(individuals[i, :FatherId]) #Always overwrite recorded DoD with linked father DoD
            individuals[i, :FatherDoD] = individuals[i, :FatherDoD_1]
        end
    end
    select!(individuals, [:IndividualId, :Sex, :DoB, :DoD, :MotherId, :MotherDoD, :FatherId, :FatherDoD])
    disallowmissing!(individuals, [:IndividualId, :Sex, :DoB])
    Arrow.write(joinpath(stagingpath(node), "Individuals.arrow"), individuals, compress=:zstd)
    @info "Wrote $(nrow(individuals)) $(node) individuals"
    return nothing
end # readindividuals
"Creates a dataset with the earliest and latest date at which an individual has been observed within left and rightcensor dates"
function individualobservationbounds(db::String, node::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """SELECT
    UPPER(CONVERT(varchar(50),O.IndividualUid)) IndividualUid,
    CONVERT(date,E.EventDate) EventDate
    FROM dbo.IndividualObservations O
        JOIN dbo.Events E ON O.ObservationUid=E.EventUid
    UNION
    SELECT
    UPPER(CONVERT(varchar(50),IR.IndividualUid)) IndividualUid,
    CONVERT(date,E.EventDate) EventDate
    FROM dbo.IndividualResidences IR
    JOIN dbo.Events E ON IR.StartEventUid = E.EventUid
    UNION
    SELECT
    UPPER(CONVERT(varchar(50),IR.IndividualUid)) IndividualUid,
    CONVERT(date,E.EventDate) EventDate
    FROM dbo.IndividualResidences IR
    JOIN dbo.Events E ON IR.EndEventUid = E.EventUid
    UNION
    SELECT
    UPPER(CONVERT(varchar(50),IR.IndividualUid)) IndividualUid,
    CONVERT(date,E.EventDate) EventDate
    FROM dbo.HouseholdMemberships IR
    JOIN dbo.Events E ON IR.StartEventUid = E.EventUid
    UNION
    SELECT
    UPPER(CONVERT(varchar(50),IR.IndividualUid)) IndividualUid,
    CONVERT(date,E.EventDate) EventDate
    FROM dbo.HouseholdMemberships IR
    JOIN dbo.Events E ON IR.EndEventUid = E.EventUid
    """
    observations = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(observations)) $(node) individual observations"
    #filter!(:EventDate => s -> s <= periodend, observations) # event must be before period end
    filter!(:EventDate => s -> s >= leftcensor, observations) # event must be after left censor date
    @info "Read $(nrow(observations)) $(node) individual observations after bounds"
    bounds = combine(groupby(observations, :IndividualUid), :EventDate => minimum => :EarliestDate, :EventDate => maximum => :LatestDate)
    disallowmissing!(bounds, :IndividualUid)
    @info "Read $(nrow(bounds)) $(node) individuals after group"
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    bounds = innerjoin(bounds, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    select!(bounds, :IndividualId, :EarliestDate, :LatestDate => ByRow(x -> x > periodend ? periodend : x) => :LatestDate)
    sort!(bounds, :IndividualId)
    Arrow.write(joinpath(stagingpath(node), "IndividualBounds.arrow"), bounds, compress=:zstd)
    @info "Wrote $(nrow(bounds)) $(node) individual bounds"
    return nothing
end
#endregion individuals
#region locations
function readlocations(node::String, io)
    readlocations_internal(settings.Databases[node], node, io)
end
"Read locations and anonimise for node save id map and location data to to arrow files"
function readlocations_internal(db::String, node::String, io)
    con = ODBC.Connection(db)
    sql1 = """WITH Areas AS (
        SELECT 
            LocationUid
          , LA.AreaUid
        FROM dbo.LocationsInAreas LA
          LEFT JOIN dbo.Areas A ON LA.AreaUid=A.AreaUid
    """
    sql2 = """  )
        SELECT
        UPPER(CONVERT(nvarchar(50),L.LocationUid)) LocationUid
        , LocationTypeId
        , UPPER(CONVERT(nvarchar(50),A.AreaUid)) AreaUid
        , L.NodeId
        FROM dbo.Locations L
          LEFT JOIN Areas A ON L.LocationUid=A.LocationUid
    """
    sql3 = node == "AHRI" ? "WHERE AreaTypeId=1 AND LA.AreaSubTypeId=1" : " WHERE AreaTypeId=1" # AHRI has AreaSubtype for LocalAreas
    locations = DBInterface.execute(con, sql1 * sql3 * sql2; iterate_rows=true) |> DataFrame
    @info "Read $(nrow(locations)) $(node) locations"
    DBInterface.close!(con)
    sort!(locations, :LocationUid)
    locations.LocationId = 1:nrow(locations)
    # Convert gui ids to integer ids
    map = locations[!, [:LocationUid, :LocationId]]
    Arrow.write(joinpath(stagingpath(node), "LocationMap.arrow"), map, compress=:zstd)
    areas = select(locations, [:AreaUid]) # |> @select(:AreaUid) |> @filter(!isna(_.AreaUid)) |> @unique() |> DataFrame
    dropmissing!(areas)
    unique!(areas)
    areas.AreaId = 1:nrow(areas)
    Arrow.write(joinpath(stagingpath(node), "AreaMap.arrow"), areas, compress=:zstd)
    locations = leftjoin(locations, areas, on=:AreaUid => :AreaUid, makeunique=true, matchmissing=:equal)
    locations = select!(locations, [:LocationId, :NodeId, :LocationTypeId, :AreaId])
    Arrow.write(joinpath(stagingpath(node), "Locations.arrow"), locations, compress=:zstd)
    a = frequency(locations, :AreaId)
    @info "Area breakdown for $(node)"
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    return nothing
end # readlocations
#endregion locations
#region residencies
"Retrieve and save residence episodes, assumes individual and locations have been read"
function readresidences(node::String, io)
    readresidences_internal(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]), io)
    if node in ["Agincourt", "DIMAMO"]
        eliminateresidenceoverlaps(node)
        readresidencestatus(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
        dropnonresidentepisodes(node)
    end
end
"Retrieve and save residence episodes directly from database"
function readresidences_internal(db::String, node::String, periodend::Date, leftcensor::Date, io)
    con = ODBC.Connection(db)
    sql = """WITH ResidentStatus AS (
        SELECT
          IndividualUid
        , O.LocationUid
        , EO.EventDate ObservationDate
        , ResidentStatus
        , CASE
            WHEN ResStatusCode='P' AND (ResidentStatus>6 OR ResidentStatus IS NULL) THEN CAST(5 AS int)
            WHEN ResStatusCode='P' AND ResidentStatus<=6 THEN CAST(4 AS int)
            WHEN ResStatusCode IN ('X','Q') AND (ResidentStatus>6) THEN CAST(4 AS int)
            WHEN ResStatusCode IN ('X','Q') AND (ResidentStatus<=6) THEN CAST(1 AS int)
            WHEN ResStatusCode IN ('X','Q') THEN CAST(0 AS int)
            WHEN ResStatusCode IN ('M','V','C','E','O') THEN CAST(1 AS int)
            WHEN ResidentStatus>=12 THEN CAST(3 AS int)
            WHEN ResidentStatus BETWEEN 11 AND 7 THEN CAST(2 AS int)
            ELSE CAST(1 AS int)
          END ResidentIndex
        FROM dbo.IndividualObservations IO
          JOIN dbo.Events EO ON IO.ObservationUid=EO.EventUid
          JOIN dbo.Observations O ON IO.ObservationUid=O.EventUid
        WHERE NOT ResStatusCode IS NULL
    ),
    DatedResidences AS (
      SELECT
        IR.IndividualResidenceUid,
        IR.IndividualUid,
        IR.LocationUid,
        CONVERT(date,SE.EventDate) StartDate,
        SE.EventTypeId StartType,
        SSE.EventDate StartObservationDate,
        CONVERT(date,EE.EventDate) EndDate,
        EE.EventTypeId EndType,
        EEE.EventDate EndObservationDate
      FROM dbo.IndividualResidences IR
        JOIN dbo.Events SE ON IR.StartEventUid=SE.EventUid
        JOIN dbo.Events EE ON IR.EndEventUid=EE.EventUid
        LEFT JOIN dbo.Events SSE ON SE.ObservationEventUid=SSE.EventUid
        LEFT JOIN dbo.Events EEE ON EE.ObservationEventUid=EEE.EventUid
      WHERE EE.EventTypeId<>0 -- Drop NYO - not yet occured end events
    )
    SELECT
      IndividualResidenceUid,
      UPPER(CONVERT(nvarchar(50),MAX(R.IndividualUid))) IndividualUid,
      UPPER(CONVERT(nvarchar(50),MAX(R.LocationUid))) LocationUid,
      MAX(StartDate) StartDate,
      MAX(StartType) StartType,
      MAX(StartObservationDate) StartObservationDate,
      MAX(EndDate) EndDate,
      MAX(EndType) EndType,
      MAX(EndObservationDate) EndObservationDate,
      AVG(CAST(ISNULL(ResidentIndex,5) AS float)) ResidentIndex
    FROM DatedResidences R
      LEFT JOIN ResidentStatus S ON R.IndividualUid=S.IndividualUid 
                                AND R.LocationUid=S.LocationUid 
                                AND R.StartDate<=S.ObservationDate AND R.EndDate>=S.ObservationDate
    GROUP BY IndividualResidenceUid;    
    """
    residences = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(residences)) $(node) residences"
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    residences = innerjoin(residences, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(stagingpath(node), "LocationMap.arrow")) |> DataFrame
    residences = innerjoin(residences, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(residences, [:IndividualId, :LocationId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
    a = frequency(residences, :StartType)
    @info "Start types $(node) before normalisation"
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    years = DataFrame(yr=Dates.year.(residences.StartDate))
    a = frequency(years, :yr)
    @info "Start years breakdown $(node) before normalisation"
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(residences, :EndType)
    @info "End types $(node) before normalisation"
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    years = DataFrame(yr=Dates.year.(residences.EndDate))
    a = frequency(years, :yr)
    @info "End years breakdown $(node) before normalisation"
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    # Do recodes
    recodeStarts = Set([100, 102, 999])
    recodeEnds = Set([103, 999])
    for i = 1:nrow(residences)
        if residences[i, :StartDate] < leftcensor
            residences[i, :StartType] = 1 # set to enumeration
            residences[i, :StartDate] = Date(residences[i, :StartObservationDate])
        end
        if residences[i, :EndDate] > periodend
            residences[i, :EndDate] = periodend # right censor to period end
            residences[i, :EndType] = 9 # end of episode beyond periodend => OBE
        end
        if residences[i, :StartType] in recodeStarts
            residences[i, :StartType] = 3
        end
        if residences[i, :EndType] in recodeEnds
            residences[i, :EndType] = 4
        end
    end
    filter!(:StartDate => s -> s <= periodend, residences)        # drop episodes that start after period end
    filter!([:StartDate, :EndDate] => (s, e) -> s <= e, residences) # start date must be smaller or equal to end date

    sort!(residences, [:IndividualId, :StartDate, :StartType])
    residences.ResidenceId = 1:nrow(residences)
    insertcols!(residences, :ResidentIndex, :GapStart => 0, :GapEnd => 0)
    df = combine(groupby(residences, :IndividualId), :LocationId, :ResidenceId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex, :GapStart, :GapEnd,
        :StartDate => ShiftedArrays.lead => :NextStart, :EndDate => ShiftedArrays.lag => :LastEnd)
    for i = 1:nrow(df)
        if !ismissing(df[i, :NextStart])
            gap = Dates.value(df[i, :NextStart] - df[i, :EndDate])
            if gap <= 0
                df[i, :EndType] = 5 # internal outmigration EXT
            elseif gap > 180 && df[i, :EndType] != 300 # exclude refusals
                df[i, :EndType] = 4 # external outmigration OMG
            end
        end
        if !ismissing(df[i, :LastEnd])
            gap = Dates.value(df[i, :StartDate] - df[i, :LastEnd])
            if gap <= 0
                df[i, :StartDate] = df[i, :LastEnd] + Dates.Day(1)
                df[i, :StartType] = 6 # internal inmigration ENT
            elseif gap > 0 && gap <= 180 && df[i, :StartType] == 6
                df[i, :StartDate] = df[i, :LastEnd] + Dates.Day(1) # close internal migration gap
            elseif gap > 180 && !(df[i, :StartType] == 300 || df[i, :StartType] == 301) # exclude refusals
                df[i, :StartType] = 3 # external inmigration
            end
        end
    end
    filter!([:StartDate, :EndDate] => (s, e) -> s <= e, df) # start date must be smaller or equal to end date
    select!(df, [:ResidenceId, :IndividualId, :LocationId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
    disallowmissing!(df, [:StartDate, :StartType, :EndDate, :EndType, :ResidentIndex])
    # filter!(:IndividualId => x -> x < 100, df)
    transform!(groupby(sort(df, [:IndividualId, :StartDate]), :IndividualId), :IndividualId => eachindex => :Episode, nrow => :Episodes)
    Arrow.write(joinpath(stagingpath(node), "IndividualResidencies.arrow"), df, compress=:zstd)
    years = DataFrame(yr=Dates.year.(residences.StartDate))
    a = frequency(years, :yr)
    @info "Start years breakdown $(node)"
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    years = DataFrame(yr=Dates.year.(residences.EndDate))
    a = frequency(years, :yr)
    @info "End years breakdown $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(residences, :StartType)
    @info "Start types $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(residences, :EndType)
    @info "End types $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    return nothing
end
"Decompose residency episodes into days and eliminate overlaps"
function processresidencedays(startdate, enddate, starttype, endtype, residentidx, locationid)
    start = startdate[1]
    stop = enddate[1]
    startt = starttype[1]
    endt = endtype[1]
    idx = residentidx[1]
    location = locationid[1]
    res_daydate = collect(start:Day(1):stop)
    res_startdate = fill(start, length(res_daydate))
    res_enddate = fill(stop, length(res_daydate))
    res_starttype = fill(startt, length(res_daydate))
    res_endtype = fill(endt, length(res_daydate))
    res_residentidx = fill(idx, length(res_daydate))
    res_locationid = fill(location, length(res_daydate))
    episode = 1
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
        idx = residentidx[i]
        location = locationid[i]
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_startdate, fill(startdate[i], length(new_daydate)))
        append!(res_enddate, fill(stop, length(new_daydate)))
        append!(res_starttype, fill(startt, length(new_daydate)))
        append!(res_endtype, fill(endt, length(new_daydate)))
        append!(res_residentidx, fill(idx, length(new_daydate)))
        append!(res_locationid, fill(location, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
    end

    return (daydate=res_daydate, startdate=res_startdate, enddate=res_enddate, starttype=res_starttype, endtype=res_endtype, residentidx=res_residentidx, locationid=res_locationid, episode=res_episode)
end
"Decompose residences into days and eliminate overlaps"
function eliminateresidenceoverlaps(node::String)
    residences = Arrow.Table(joinpath(stagingpath(node), "IndividualResidencies.arrow")) |> DataFrame
    @info "Node $(node) $(nrow(residences)) episodes before overlap elimination at $(now())"
    select!(residences, [:ResidenceId, :IndividualId, :LocationId, :StartDate, :StartType, :EndDate, :EndType, :ResidentIndex])
    insertcols!(residences, :ResidentIndex, :GapStart => 0, :GapEnd => 0, :Gap => 0)
    s = combine(groupby(sort(residences, [:StartDate, order(:EndDate, rev=true)]), :IndividualId, sort=true), [:StartDate, :EndDate, :StartType, :EndType, :ResidentIndex, :LocationId] => processresidencedays => AsTable)
    @info "$(nrow(s)) day rows for $(node) at $(now())"
    df = combine(groupby(s, [:IndividualId, :episode, :locationid]), :daydate => minimum => :StartDate, :starttype => first => :StartType,
        :daydate => maximum => :EndDate, :endtype => last => :EndType,
        :residentidx => mean => :ResidentIndex, :episode => maximum => :episodes)
    @info "Node $(node) $(nrow(df)) episodes after overlap elimination at $(now())"
    rename!(df, Dict(:episode => "Episode", :locationid => "LocationId", :episodes => "Episodes"))
    Arrow.write(joinpath(stagingpath(node), "IndividualResidenciesIntermediate.arrow"), df, compress=:zstd)
    return nothing
end
"Read resident status observations"
function readresidencestatus(db::String, node::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """/*
    Designed to smooth single instances of changed resident status over - changed to not exclude
    */
    WITH ExpandedStatus AS (
      SELECT
        IndividualUid
      , O.LocationUid
      , EO.EventDate ObservationDate
      , ResidentStatus
      , ResStatusCode
      , LAG(ResStatusCode) OVER (ORDER BY IndividualUid, O.LocationUid, EO.EventDate) LastStatus
      , LEAD(ResStatusCode) OVER (ORDER BY IndividualUid, O.LocationUid, EO.EventDate) NextStatus
      FROM dbo.IndividualObservations IO
        JOIN dbo.Events EO ON IO.ObservationUid=EO.EventUid
        JOIN dbo.Observations O ON IO.ObservationUid=O.EventUid
      WHERE ResidentStatus > 0
         OR NOT ResStatusCode IS NULL
       ),
    SmoothedStatus AS (
      SELECT
        IndividualUid,
        LocationUid,
        ObservationDate,
        --CASE
        --  WHEN LastStatus=NextStatus THEN LastStatus
        --  ELSE ResStatusCode
        --END 
        ResStatusCode,
        ResidentStatus
      FROM ExpandedStatus
    )
    SELECT
      UPPER(CONVERT(nvarchar(50),IndividualUid)) IndividualUid
    , UPPER(CONVERT(nvarchar(50),LocationUid)) LocationUid
    , CONVERT(date, ObservationDate) ObservationDate
    , CASE
        WHEN ResStatusCode = 'P' THEN CAST(1 AS int)
        WHEN ResStatusCode = 'O' AND ResidentStatus>6 THEN CAST(1 AS int)
        WHEN ResStatusCode = 'O' AND ResidentStatus<=6 THEN CAST(2 AS int)
        WHEN ResStatusCode IN ('X','Q') AND (ResidentStatus>=6) THEN CAST(1 AS int)
        WHEN ResStatusCode IN ('X','Q') AND (ResidentStatus<6) THEN CAST(2 AS int)
        WHEN ResStatusCode IN ('X','Q') THEN CAST(1 AS int)
        WHEN ResStatusCode <> 'P' THEN CAST(2 AS int)
     WHEN ResidentStatus < 6 THEN CAST(2 AS int)
     ELSE CAST(1 AS int)
      END ResidentStatus -- 1 Resident 2 Non-resident
    FROM SmoothedStatus
    """
    resstatuses = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(resstatuses)) $(node) residence statuses"
    filter!(:ObservationDate => s -> s <= periodend, resstatuses)        # drop statuses after period end
    filter!(:ObservationDate => s -> s >= leftcensor, resstatuses)       # drop statuses before leftcensor date
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    resstatuses = innerjoin(resstatuses, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(stagingpath(node), "LocationMap.arrow")) |> DataFrame
    resstatuses = innerjoin(resstatuses, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(resstatuses, [:IndividualId, :LocationId, :ObservationDate, :ResidentStatus])
    disallowmissing!(resstatuses, [:ObservationDate, :ResidentStatus])
    sort!(resstatuses, [:IndividualId, :LocationId, :ObservationDate])
    Arrow.write(joinpath(stagingpath(node), "ResidentStatus.arrow"), resstatuses, compress=:zstd)
    @info "Wrote $(nrow(resstatuses)) $(node) residence statuses"
    return nothing
end # readresidencestatus
"Use resident status observations to identify non-resident episodes and drop those from residency episodes"
function dropnonresidentepisodes(node::String)
    r = Arrow.Table(joinpath(stagingpath(node), "IndividualResidenciesIntermediate.arrow")) |> DataFrame
    @info "$(nrow(r)) $(node) residence rows"
    rs = open(joinpath(stagingpath(node), "ResidentStatus.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    @info "$(nrow(rs)) $(node) residence status rows"
    s = outerjoin(r, rs, on=[:IndividualId => :IndividualId, :LocationId => :LocationId])
    ###### Debug only
    # filter!(row -> row.IndividualId == 184, s)
    #######
    # eliminate records that didn't join properly
    dropmissing!(s, :LocationId, disallowmissing=true)
    dropmissing!(s, :Episode, disallowmissing=true)
    replace!(s.ResidentStatus, missing => 1)
    disallowmissing!(s, [:IndividualId, :StartDate, :StartType, :EndDate, :EndType, :Episode, :Episodes, :ResidentIndex, :ResidentStatus])

    for i = 1:nrow(s)
        # od = s[i,:ObservationDate]
        if ismissing(s[i, :ObservationDate])
            s[i, :ObservationDate] = s[i, :StartDate]
        end
    end
    filter!(row -> (row.ObservationDate >= row.StartDate) & (row.ObservationDate <= row.EndDate), s)
    df = combine(groupby(sort(s, [:StartDate, :ObservationDate]), :IndividualId), :LocationId, :StartDate, :StartType, :EndDate, :EndType, :ResidentIndex, :Episode, :Episodes, :ObservationDate, :ResidentStatus,
        :ResidentStatus => ShiftedArrays.lag => :LastResidentStatus,
        :LocationId => ShiftedArrays.lag => :LastLocationId, :Episode => ShiftedArrays.lag => :LastEpisode)
    insertcols!(df, :episode => 0)
    episode = 0
    # println(df)
    # Arrow.write(joinpath(stagingpath(node), "dropnonresidentepisodes.arrow"), df, compress=:zstd)
    for i = 1:nrow(df)
        if ismissing(df[i, :LastLocationId])
            episode = 1
            df[i, :episode] = episode
        else
            location = df[i, :LocationId]
            lastlocation = df[i, :LastLocationId]
            e = df[i, :Episode]
            laste = df[i, :LastEpisode]
            if location != lastlocation || e != laste
                episode = episode + 1
                df[i, :episode] = episode
            else
                residentstatus = df[i, :ResidentStatus]
                lastresidentstatus = df[i, :LastResidentStatus]
                if residentstatus != lastresidentstatus
                    episode = episode + 1
                    df[i, :episode] = episode
                else
                    df[i, :episode] = episode
                end
            end
        end
    end
    s = combine(groupby(df, [:IndividualId, :LocationId, :episode]), :StartDate => first => :StartDate, :StartType => first => :StartType,
        :EndDate => first => :EndDate, :EndType => first => :EndType,
        :ResidentStatus => first => :ResidentStatus, :ResidentIndex => mean => :ResidentIndex,
        :ObservationDate => minimum => :StartObservationDate, :ObservationDate => maximum => :EndObservationDate)
    @info "Node $(node) $(nrow(s)) episodes after resident split"
    df = combine(groupby(s, :IndividualId), :LocationId, :episode, :StartDate, :StartType, :EndDate, :EndType, :ResidentStatus, :StartObservationDate, :EndObservationDate, :ResidentIndex,
        :ResidentStatus => ShiftedArrays.lead => :NextResidentStatus,
        :ResidentStatus => ShiftedArrays.lag => :LastResidentStatus,
        :StartObservationDate => ShiftedArrays.lead => :NextStartObsDate,
        :EndObservationDate => ShiftedArrays.lag => :LastEndObsDate)
    for i = 1:nrow(df)
        if df[i, :ResidentStatus] == 1 && !ismissing(df[i, :NextResidentStatus]) && df[i, :NextResidentStatus] == 2
            df[i, :EndDate] = newrandomdate(df[i, :EndObservationDate], df[i, :EndObservationDate], df[i, :NextStartObsDate])
            df[i, :EndType] = 4
        end
        if df[i, :ResidentStatus] == 1 && !ismissing(df[i, :LastResidentStatus]) && df[i, :LastResidentStatus] == 2
            df[i, :StartDate] = newrandomdate(df[i, :LastEndObsDate], df[i, :LastEndObsDate], df[i, :StartObservationDate])
            df[i, :StartType] = 3
        end
    end
    filter!(:ResidentStatus => s -> s == 1, df)
    @info "Node $(node) $(nrow(df)) episodes after dropping non-resident episodes"
    df.ResidenceId = 1:nrow(df)
    select!(df, [:ResidenceId, :IndividualId, :LocationId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
    mv(joinpath(stagingpath(node), "IndividualResidencies.arrow"), joinpath(stagingpath(node), "IndividualResidencies_old.arrow"), force=true)
    transform!(groupby(sort(df, [:IndividualId, :StartDate]), :IndividualId), :IndividualId => eachindex => :Episode, nrow => :Episodes)
    Arrow.write(joinpath(stagingpath(node), "IndividualResidencies.arrow"), df, compress=:zstd)
    return nothing
end # dropnonresidentepisodes
#endregion
#region Households
"Read households and save household map and data to staging directory"
function readhouseholds(node::String)
    readhouseholds_internal(settings.Databases[node], node)
    readhouseholdresidences(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end
"Read households and save household map and data to staging directory"
function readhouseholds_internal(db::String, node::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),H.HouseholdUid)) HouseholdUid,
        CONVERT(date,SE.EventDate) StartDate,
        SE.EventTypeId StartType,
        CONVERT(date,EE.EventDate) EndDate,
        EE.EventTypeId EndType
    FROM dbo.Households H
        JOIN dbo.Events SE ON H.StartEventUid=SE.EventUid
        JOIN dbo.Events EE ON H.EndEventUid=EE.EventUid
    ORDER BY HouseholdUid
    """
    households = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    @info "Read $(nrow(households)) $(node) households"
    DBInterface.close!(con)
    households.HouseholdId = 1:nrow(households)
    map = households[!, [:HouseholdUid, :HouseholdId]]
    Arrow.write(joinpath(stagingpath(node), "HouseholdMap.arrow"), map, compress=:zstd)
    select!(households, [:HouseholdId, :StartDate, :StartType, :EndDate, :EndType])
    Arrow.write(joinpath(stagingpath(node), "Households.arrow"), households, compress=:zstd)
    return nothing
end # readhouseholds
"Retrieve household residencies with left and right censor dates"
function readhouseholdresidences(db::String, node::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),HouseholdResidenceUid)) HouseholdResidenceUid
    , UPPER(CONVERT(nvarchar(50),HR.HouseholdUid)) HouseholdUid
    , UPPER(CONVERT(nvarchar(50),HR.LocationUid)) LocationUid
    , CONVERT(date, SE.EventDate) StartDate
    , SE.EventTypeId StartType
    , SO.EventDate StartObservationDate
    , CONVERT(date, EE.EventDate) EndDate
    , EE.EventTypeId EndType
    , EO.EventDate EndObservationDate
    FROM dbo.HouseholdResidences HR
        JOIN dbo.Events SE ON HR.StartEventUid=SE.EventUid
        JOIN dbo.Events EE ON HR.EndEventUid=EE.EventUid
        JOIN dbo.Events SO ON SE.ObservationEventUid=SO.EventUid
        JOIN dbo.Events EO ON EE.ObservationEventUid=EO.EventUid
    """
    residences = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    @info "Read $(nrow(residences)) $(node) household residences"
    sql = """SELECT
      HM.IndividualUid
    , UPPER(CONVERT(nvarchar(50),HM.HouseholdUid)) HouseholdUid
    , HHRelationshipTypeId
    , CONVERT(date, HRS.EventDate) StartDate
    , CASE 
        WHEN CONVERT(date, HRS.EventDate)=CONVERT(date, HMS.EventDate) THEN HMS.EventTypeId
        ELSE HRS.EventTypeId
        END StartType
    , HRSS.EventDate StartObservationDate
    , CONVERT(date, HRE.EventDate) EndDate
    , CASE 
        WHEN CONVERT(date, HRE.EventDate)=CONVERT(date, HME.EventDate) THEN HME.EventTypeId
        ELSE HRE.EventTypeId
        END EndType
    , CONVERT(date,HRSE.EventDate) EndObservationDate
    FROM dbo.HHeadRelationships HR
        JOIN dbo.HouseholdMemberships HM ON HR.HouseholdMembershipUid=HM.HouseholdMembershipUid
        JOIN dbo.Events HRS ON HR.StartEventUid=HRS.EventUid
        JOIN dbo.Events HRE ON HR.EndEventUid=HRE.EventUid
        JOIN dbo.Events HMS ON HM.StartEventUid=HMS.EventUid
        JOIN dbo.Events HME ON HM.EndEventUid=HME.EventUid
        JOIN dbo.Events HRSS ON HRS.ObservationEventUid=HRSS.EventUid
        JOIN dbo.Events HRSE ON HRE.ObservationEventUid=HRSE.EventUid;  
    """
    relationships = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(relationships)) $(node) household relationships"
    lastseen = combine(groupby(relationships, :HouseholdUid), :EndDate => maximum => :LastHHDate, :EndObservationDate => maximum => :LastObservationDate)
    @info "Grouped $(nrow(lastseen)) $(node) households from household relationships"
    sort!(residences, [:HouseholdUid, :StartDate])
    residences = combine(groupby(residences, :HouseholdUid), sdf -> sort(sdf, [:StartDate, order(:EndDate, rev=true)]), s -> 1:nrow(s), nrow => :Episodes)
    rename!(residences, :x1 => :Episode)
    @info "Household residences $(nrow(residences)) $(node) after episode counts"
    r = leftjoin(residences, lastseen, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    for i = 1:nrow(r)
        if r[i, :StartDate] < leftcensor
            r[i, :StartType] = 1 # set to enumeration
            r[i, :StartDate] = Date(residences[i, :StartObservationDate])
        end
        if !ismissing(r[i, :LastHHDate]) && r[i, :Episode] == r[i, :Episodes] && r[i, :LastHHDate] > r[i, :EndDate]
            r[i, :EndDate] = r[i, :LastHHDate]
            r[i, :EndObservationDate] = r[i, :LastObservationDate]
        end
        if r[i, :EndDate] > periodend
            r[i, :EndDate] = periodend # right censor to period end
            r[i, :EndType] = 9 # end of episode beyond periodend => OBE
        end
    end
    filter!(:StartDate => s -> s <= periodend, r)        # drop episodes that start after period end
    filter!([:StartDate, :EndDate] => (s, e) -> s <= e, r) # start date must be smaller or equal to end date
    select!(r, [:HouseholdUid, :LocationUid, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate])
    @info "Household residences $(nrow(r)) $(node) after right censor"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    r = innerjoin(r, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(stagingpath(node), "LocationMap.arrow")) |> DataFrame
    r = innerjoin(r, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(r, [:HouseholdId, :LocationId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate])
    disallowmissing!(r, [:StartDate, :EndDate])
    df = combine(groupby(sort(r, [:StartDate, order(:EndDate, rev=true)]), :HouseholdId), :LocationId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate,
        :StartDate => ShiftedArrays.lead => :NextStart, :EndDate => ShiftedArrays.lag => :LastEnd)
    for i = 1:nrow(df)
        if !ismissing(df[i, :NextStart])
            gap = Dates.value(df[i, :NextStart] - df[i, :EndDate])
            if gap <= 0
                df[i, :EndType] = 5 # internal outmigration EXT
            elseif gap > 180 && df[i, :EndType] != 300 # exclude refusals
                df[i, :EndType] = 4 # external outmigration OMG
            end
        end
        if !ismissing(df[i, :LastEnd])
            gap = Dates.value(df[i, :StartDate] - df[i, :LastEnd])
            if gap <= 0
                df[i, :StartDate] = df[i, :LastEnd] + Dates.Day(1)
                df[i, :StartType] = 6 # internal inmigration ENT
            elseif gap > 0 && gap <= 180 && df[i, :StartType] == 6
                df[i, :StartDate] = df[i, :LastEnd] + Dates.Day(1) # close internal migration gap
            elseif gap > 180 && !(df[i, :StartType] == 300 || df[i, :StartType] == 301) # exclude refusals
                df[i, :StartType] = 3 # external inmigration
            end
        end
    end
    filter!([:StartDate, :EndDate] => (s, e) -> s <= e, df) # start date must be smaller or equal to end date
    sort!(df, [:HouseholdId, :StartDate, order(:EndDate, rev=true)])
    select!(df, [:HouseholdId, :LocationId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate])
    transform!(groupby(df, [:HouseholdId]), :HouseholdId => eachindex => :Episode, nrow => :Episodes)
    Arrow.write(joinpath(stagingpath(node), "HouseholdResidences.arrow"), df, compress=:zstd)
    @info "Wrote $(nrow(df)) $(node) household residences"
    return nothing
end
#endregion
#region HouseholdMemberships
"Retrieve and save household membership and householdhead relationships data"
function readhouseholdmemberships(node::String)
    readhouseholdmemberships_internal(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
    readhouseholdheadrelationships(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end
"Retrieve household memberships with left and right censor dates"
function readhouseholdmemberships_internal(db::String, node::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """SELECT
      UPPER(CONVERT(varchar(50),HM.IndividualUid)) IndividualUid
    , UPPER(CONVERT(varchar(50),HM.HouseholdUid)) HouseholdUid
    , CONVERT(date,HMS.EventDate) StartDate
    , HMS.EventTypeId StartType
    , HRSS.EventDate StartObservationDate
    , CONVERT(date,HME.EventDate) EndDate
    , HME.EventTypeId EndType
    , HRSE.EventDate EndObservationDate
    FROM dbo.HouseholdMemberships HM
        JOIN dbo.Events HMS ON HM.StartEventUid=HMS.EventUid
        JOIN dbo.Events HME ON HM.EndEventUid=HME.EventUid
        JOIN dbo.Events HRSS ON HMS.ObservationEventUid=HRSS.EventUid
        JOIN dbo.Events HRSE ON HME.ObservationEventUid=HRSE.EventUid
    WHERE HME.EventTypeId>0 -- get rid of NYO episodes
    """
    memberships = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(memberships)) $(node) membership episodes from database"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    memberships = innerjoin(memberships, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(memberships)) $(node) membership episodes"
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    memberships = innerjoin(memberships, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(memberships)) $(node) membership episodes"
    individualbounds = Arrow.Table(joinpath(stagingpath(node), "IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(memberships, individualbounds, on=:IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(m)) $(node) membership episodes after individual bounds join"
    #adjust start and end dates
    for i = 1:nrow(m)
        if ismissing(m[i, :EarliestDate])
            m[i, :EarliestDate] = leftcensor
        end
        if m[i, :StartDate] < leftcensor
            m[i, :StartDate] = m[i, :EarliestDate]
            m[i, :StartObservationDate] = m[i, :EarliestDate]
            m[i, :StartType] = 1
        end
        if m[i, :EndDate] > periodend
            m[i, :EndDate] = periodend
            m[i, :EndType] = 9
        end
    end
    filter!(:StartDate => s -> s <= periodend, m)         # drop episodes that start after period end
    filter!([:StartDate, :EndDate] => (s, e) -> s <= e, m) # start date must be smaller or equal to end date
    sort!(m, [:IndividualId, :HouseholdId, :StartDate])
    m.MembershipId = 1:nrow(m)
    transform!(groupby(m, [:IndividualId, :HouseholdId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
    select!(m, [:MembershipId, :IndividualId, :HouseholdId, :StartDate, :StartType, :StartObservationDate, :EndDate, :EndType, :EndObservationDate, :Episode, :Episodes])
    Arrow.write(joinpath(stagingpath(node), "HouseholdMemberships.arrow"), m, compress=:zstd)
    @info "Wrote $(nrow(m)) $(node) membership episodes"
    return nothing
end #readhouseholdmemberships
"Retrieve household head relationships with left and right censor dates"
function readhouseholdheadrelationships(db::String, node::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HM.IndividualUid)) IndividualUid
    , UPPER(CONVERT(varchar(50),HM.HouseholdUid)) HouseholdUid
    , HR.HHRelationshipTypeId
    , CONVERT(date,HMS.EventDate) StartDate
    , HMS.EventTypeId StartType
    , CONVERT(date,HME.EventDate) EndDate
    , HME.EventTypeId EndType
    , HRSS.EventDate StartObservationDate
    , HRSE.EventDate EndObservationDate
    FROM dbo.HHeadRelationships HR
        JOIN dbo.HouseholdMemberships HM ON HR.HouseholdMembershipUid=HM.HouseholdMembershipUid
        JOIN dbo.Events HMS ON HR.StartEventUid=HMS.EventUid
        JOIN dbo.Events HME ON HR.EndEventUid=HME.EventUid
        JOIN dbo.Events HRSS ON HMS.ObservationEventUid=HRSS.EventUid
        JOIN dbo.Events HRSE ON HME.ObservationEventUid=HRSE.EventUid
    WHERE HME.EventTypeId>0 -- get rid of NYO episodes
    """
    relationships = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(relationships)) $(node) relationships episodes from database"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    relationships = innerjoin(relationships, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(relationships)) $(node) relationships episodes"
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    relationships = innerjoin(relationships, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(relationships)) $(node) relationships episodes"
    individualbounds = Arrow.Table(joinpath(stagingpath(node), "IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(relationships, individualbounds, on=:IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(m)) $(node) relationships episodes after individual bounds join"
    #adjust start and end dates
    for i = 1:nrow(m)
        if ismissing(m[i, :EarliestDate])
            m[i, :EarliestDate] = leftcensor
        end
        if m[i, :StartDate] < leftcensor
            m[i, :StartDate] = m[i, :EarliestDate]
            m[i, :StartObservationDate] = m[i, :EarliestDate]
            m[i, :StartType] = 1
        end
        if m[i, :EndDate] > periodend
            m[i, :EndDate] = periodend
            m[i, :EndType] = 9
        end
    end
    filter!(:StartDate => s -> s <= periodend, m)         # drop episodes that start after period end
    filter!([:StartDate, :EndDate] => (s, e) -> s <= e, m) # start date must be smaller or equal to end date
    sort!(m, [:IndividualId, :HouseholdId, :StartDate, :HHRelationshipTypeId])
    m.RelationshipId = 1:nrow(m)
    transform!(groupby(m, [:IndividualId, :HouseholdId]), :IndividualId => eachindex => :Episode, nrow => :Episodes)
    select!(m, [:RelationshipId, :IndividualId, :HouseholdId, :HHRelationshipTypeId, :StartDate, :StartType, :StartObservationDate, :EndDate, :EndType, :EndObservationDate, :Episode, :Episodes])
    Arrow.write(joinpath(stagingpath(node), "HHeadRelationships.arrow"), m, compress=:zstd)
    @info "Wrote $(nrow(m)) $(node) relationships episodes"
    return nothing
end #readhouseholdheadrelationships
#endregion
#region IndividualMemberships
"Decompose household memberships to days and combine with houeshold relationships and produced consolidated individual hosuehold membership episodes with household head relationship"
function readindividualmemberships(node::String, batchsize::Int64=BatchSize)
    batchmemberships(node, batchsize)
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
        new_daydate = start:Day(1):stop
        append!(res_daydate, new_daydate)
        append!(res_startdate, fill(startdate[i], length(new_daydate)))
        append!(res_enddate, fill(stop, length(new_daydate)))
        append!(res_starttype, fill(startt, length(new_daydate)))
        append!(res_endtype, fill(endt, length(new_daydate)))
        append!(res_episode, fill(episode, length(new_daydate)))
    end

    return (daydate=res_daydate, startdate=res_startdate, enddate=res_enddate, starttype=res_starttype, endtype=res_endtype, episode=res_episode)
end
function getmembershipdays(f)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId, :HouseholdId], sort=true), [:StartDate, :EndDate, :StartType, :EndType] => processmembershipdays => AsTable)
    rename!(s, Dict(:daydate => "DayDate", :episode => "Episode", :startdate => "StartDate", :enddate => "EndDate", :starttype => "StartType", :endtype => "EndType"))
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

    return (daydate=res_daydate, startdate=res_startdate, enddate=res_enddate, starttype=res_starttype, endtype=res_endtype, hhrelationtype=res_relation, episode=res_episode)
end
function getrelationshipdays(f)
    s = combine(groupby(sort(f, [:StartDate, order(:EndDate, rev=true)]), [:IndividualId, :HouseholdId], sort=true), [:StartDate, :EndDate, :StartType, :EndType, :HHRelationshipTypeId] => processrelationshipdays => AsTable)
    rename!(s, Dict(:daydate => "DayDate", :episode => "Episode", :startdate => "StartDate", :enddate => "EndDate", :starttype => "StartType", :endtype => "EndType", :hhrelationtype => "HHRelationshipTypeId"))
    return s
end #getrelationshipdays
function individualmemberships(node::String, m, r, batch::Int64)
    mr = leftjoin(getmembershipdays(m), getrelationshipdays(r), on=[:IndividualId => :IndividualId, :HouseholdId => :HouseholdId, :DayDate => :DayDate], makeunique=true, matchmissing=:equal)
    select!(mr, [:IndividualId, :HouseholdId, :HHRelationshipTypeId, :DayDate, :StartType, :EndType, :Episode])
    replace!(mr.HHRelationshipTypeId, missing => 12)
    disallowmissing!(mr, [:HHRelationshipTypeId, :DayDate])
    @info "$(nrow(mr)) $(node) day rows in batch $(batch)"
    mr = combine(groupby(mr, [:IndividualId, :HouseholdId]), :HHRelationshipTypeId, :DayDate, :StartType, :EndType, :Episode,
        :HHRelationshipTypeId => ShiftedArrays.lag => :LastRelation, :HHRelationshipTypeId => ShiftedArrays.lead => :NextRelation)
    for i = 1:nrow(mr)
        if !ismissing(mr[i, :LastRelation])
            if mr[i, :LastRelation] != mr[i, :HHRelationshipTypeId]
                mr[i, :StartType] = 104
            end
        end
        if !ismissing(mr[i, :NextRelation])
            if mr[i, :NextRelation] != mr[i, :HHRelationshipTypeId]
                mr[i, :EndType] = 104
            end
        end
    end
    memberships = combine(groupby(mr, [:IndividualId, :HouseholdId, :HHRelationshipTypeId, :Episode]), :DayDate => minimum => :StartDate, :StartType => first => :StartType,
        :DayDate => maximum => :EndDate, :EndType => last => :EndType)
    serializetofile(joinpath(stagingpath(node), "IndividualMemberships$(batch)"), memberships)
    @info "Wrote $(nrow(memberships)) $(node) individual membership episodes in batch $(batch)"
    memberships = nothing
    return nothing
end #individualmemberships
function openchunk(node::String, chunk::Int64)
    open(joinpath(stagingpath(node), "IndividualMemberships$(chunk).arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end;
end
"Concatenate membership batches"
function combinemembershipbatch(node::String, batches)
    memberships = openchunk(node::String, 1)
    r = similar(memberships, 0)
    for i = 1:batches
        m = openchunk(node::String, i)
        append!(r, m)
        m = nothing
    end
    open(joinpath(stagingpath(node), "IndividualMemberships.arrow"), "w"; lock=false) do io
        Arrow.write(io, r, compress=:zstd)
    end
    @info "Final individual membership rows $(nrow(r)) for $(node)"
    r = nothing
    GC.gc(); GC.gc(); GC.gc()
    #delete chunks
    for i = 1:batches
        rm(joinpath(stagingpath(node), "IndividualMemberships$(i).arrow"))
    end
    return nothing
end #combinemembershipbatch
"Normalise memberships in batches"
function batchmemberships(node::String, batchsize::Int64=BatchSize)
    relationships = open(joinpath(stagingpath(node), "HHeadRelationships.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(relationships, [:IndividualId, :HouseholdId, :Episode, :StartDate, :StartType, :EndDate, :EndType, :HHRelationshipTypeId])
    @info "Node $(node) $(nrow(relationships)) relationship episodes"
    memberships = open(joinpath(stagingpath(node), "HouseholdMemberships.arrow")) do io
        return Arrow.Table(io) |> DataFrame
    end
    select!(memberships, [:IndividualId, :HouseholdId, :Episode, :StartDate, :StartType, :EndDate, :EndType])
    @info "Node $(node) $(nrow(memberships)) memberships episodes"
    minId, maxId, batches = individualbatch(node, batchsize)
    #    Threads.@threads for i = 1:batches
    for i = 1:batches
        fromId, toId = nextidrange(minId, maxId, i, batchsize)
        @info "Batch $(i) from $(fromId) to $(toId)"
        m = filter([:IndividualId] => id -> fromId <= id <= toId, memberships)
        r = filter([:IndividualId] => id -> fromId <= id <= toId, relationships)
        individualmemberships(node, m, r, i)
    end
    memberships = nothing
    relationships = nothing
    r = restoredataframe(stagingpath(node),"IndividualMemberships", batches)
    open(joinpath(stagingpath(node), "IndividualMemberships.arrow"), "w"; lock=false) do io
        Arrow.write(io, r, compress=:zstd)
    end
    r = nothing
    return nothing
end #batchmemberships

#endregion
#region EducationStatuses
function readeducationstatuses(node::String, io)
    educationstatus(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]), io)
end
function educationstatus(db::String, node::String, periodend::Date, leftcensor::Date, io)
    con = ODBC.Connection(db)
    sql = """SELECT
    UPPER(CONVERT(varchar(50),IndividualUid)) IndividualUid
    , CONVERT(date, OE.EventDate) ObservationDate
    , CurrentEducation
    , HighestSchoolLevel
    , HighestNonSchoolLevel
    FROM dbo.IndividualObservations IO
        JOIN dbo.Events OE ON IO.ObservationUid=OE.EventUid
    """
    s = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) education statuses from database"
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    individualbounds = Arrow.Table(joinpath(stagingpath(node), "IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(si, individualbounds, on=:IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    insertcols!(m, :OutsideBounds => false)
    for i = 1:nrow(m)
        if (!ismissing(m[i, :EarliestDate]) && m[i, :ObservationDate] < m[i, :EarliestDate]) || (m[i, :ObservationDate] < leftcensor)
            m[i, :OutsideBounds] = true
        end
        if (!ismissing(m[i, :LatestDate]) && m[i, :ObservationDate] > m[i, :LatestDate]) || (m[i, :ObservationDate] > periodend)
            m[i, :OutsideBounds] = true
        end
    end
    #filter if outside bounds
    filter!([:OutsideBounds] => x -> !x, m)
    @info "Read $(nrow(m)) $(node) education statuses inside bounds"
    filter!([:CurrentEducation, :HighestSchoolLevel, :HighestNonSchoolLevel] => (x, y, z) -> !(x < 0 && y < 0 && z < 0), m)
    @info "Read $(nrow(m)) $(node) education statuses not missing"
    disallowmissing!(m, [:ObservationDate])
    select!(m, [:IndividualId, :ObservationDate, :CurrentEducation, :HighestSchoolLevel, :HighestNonSchoolLevel])
    sort!(m, [:IndividualId, :ObservationDate])
    Arrow.write(joinpath(stagingpath(node), "EducationStatuses.arrow"), m, compress=:zstd)
    a = frequency(m, :CurrentEducation)
    @info "Current Education breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(m, :HighestSchoolLevel)
    @info "Highest School Level breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(m, :HighestNonSchoolLevel)
    @info "Highest Non-School Level breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    return nothing
end
#endregion
#region household socio-economic
"Read and save household socio-economic data"
function readhouseholdsocioeconomic(node::String, io)
    householdassets(settings.Databases[node], node, io)
    householdsocioeconomic(settings.Databases[node], node, io)
    retrieve_asset_items(settings.Databases[node], node)
    retrieve_hse(settings.Databases[node], node)
end
function householdassets(db::String, node::String, io)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HA.HouseholdObservationUid)) HouseholdObservationUid,
        HA.AssetId,
        HA.AssetStatusId
    FROM dbo.HouseholdAssets HA
    WHERE HA.AssetStatusId>0;
    """
    s = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) asset statuses from database"
    assetmap = DataFrame(XLSX.readtable(joinpath(pwd(), "src", "Assets.xlsx"), "Consolidated"))
    assetmap[!, :AssetId] = map(convertanytoint, assetmap[!, :AssetId])
    assetmap[!, :Id] = map(convertanytoint, assetmap[!, :Id])
    assetmap[!, :AssetName] = map(convertanytostr, assetmap[!, :AssetName])
    assetmap[!, :Name] = map(convertanytostr, assetmap[!, :Name])
    assetmap[!, :AssetIdx] = map(convertanytostr, assetmap[!, :AssetIdx])
    si = innerjoin(s, assetmap, on=:AssetId => :AssetId, makeunique=true, matchmissing=:equal)
    g = combine(groupby(si, [:HouseholdObservationUid, :Id]), :AssetStatusId => minimum => :AssetStatus, :AssetIdx => first => :AssetGroup)
    @info "$(nrow(g)) $(node) grouped asset statuses"
    filter!([:AssetStatus] => x -> x == 1, g)
    @info "$(nrow(g)) $(node) present asset statuses"
    gg = combine(groupby(g, [:HouseholdObservationUid, :AssetGroup]), :AssetStatus => sum => :Idx)
    @info "$(nrow(gg)) $(node) asset groups"
    filter!([:AssetGroup] => x -> x != "0", gg)
    w = unstack(gg, :HouseholdObservationUid, :AssetGroup, :Idx)
    replace!(w.Modern, missing => 0)
    replace!(w.Livestock, missing => 0)
    disallowmissing!(w, [:HouseholdObservationUid, :Modern, :Livestock])
    a = frequency(w, :Modern)
    @info "Modern asset breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(w, :Livestock)
    @info "Livestock asset breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    Arrow.write(joinpath(stagingpath(node), "AssetStatus.arrow"), w, compress=:zstd)
    return nothing
end #householdassets
function householdsocioeconomic(db::String, node::String, io)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HouseholdObservationUid)) HouseholdObservationUid,
        UPPER(CONVERT(varchar(50),HouseholdUid)) HouseholdUid,
        CONVERT(date,E.EventDate) ObservationDate,
        WaterSource,
        Toilet,
        ConnectedToGrid,
        CookingFuel,
        WallMaterial,
        FloorMaterial,
        Bedrooms,
        Crime,
        FinancialStatus,
        CutMeals,
        CutMealsFrequency,
        NotEat,
        NotEatFrequency,
        ChildMealSkipCut,
        ChildMealSkipCutFrequency,
        ConsentToCall
    FROM dbo.HouseholdObservations HO
        JOIN dbo.Events E ON HO.ObservationUid = E.EventUid;
    """
    s = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) HSE observations from database"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    si = innerjoin(s, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(si)) $(node) HSE observations after household map"
    a = frequency(si, :WaterSource)
    @info "Watersource breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(si, :Toilet)
    @info "Toilet breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    recode!(si[!, :Toilet], missing => 0, [0, 8] => 0, [1, 15, 16, 17] => 1, 2 => 2, 3 => 3, 4 => 4, [5, 18] => 5, [6, 19] => 6, [7, 11] => 7)
    a = frequency(si, :Toilet)
    @info "Toilet breakdown for $(node) after recode" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(si, :CookingFuel)
    @info "CookingFuel breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(si, :WallMaterial)
    @info "WallMaterial breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(si, :FloorMaterial)
    @info "FloorMaterial breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(si, :Bedrooms)
    @info "Bedrooms breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    recode!(si[!, :Bedrooms], missing => 0, 0 => 0, [1, 2] => 1, [3, 4] => 2, [5, 6] => 3, 7:90 => 4, 91:99 => 0, 100:9999 => 4)
    a = frequency(si, :Bedrooms)
    @info "Bedrooms breakdown for $(node) after recode" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(si, :ConnectedToGrid)
    @info "ConnectedToGrid breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    replace!(si.ConnectedToGrid, missing => 0, true => 1, false => 0)
    a = frequency(si, :ConnectedToGrid)
    @info "ConnectedToGrid for $(node) after recode" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    select!(si, [:HouseholdId, :ObservationDate, :WaterSource, :Toilet, :ConnectedToGrid, :CookingFuel, :WallMaterial, :FloorMaterial, :Bedrooms,
        :Crime, :FinancialStatus, :CutMeals, :CutMealsFrequency, :NotEat, :NotEatFrequency, :ChildMealSkipCut, :ChildMealSkipCutFrequency, :ConsentToCall])
    sort!(si, [:HouseholdId, :ObservationDate])
    Arrow.write(joinpath(stagingpath(node), "SocioEconomic.arrow"), si, compress=:zstd)
    return nothing
end #householdsocioeconomic
"Retrieve asset items"
function retrieve_asset_items(db::String, node::String)
    con = ODBC.Connection(db)
    sql = """
    SELECT
        UPPER(CONVERT(varchar(50),HO.HouseholdUid)) HouseholdUid,
        CONVERT(date,E.EventDate) ObservationDate,
        HA.AssetId,
        HA.AssetStatusId
    FROM dbo.HouseholdAssets HA
    JOIN dbo.HouseholdObservations HO ON HA.HouseholdObservationUid = HO.HouseholdObservationUid
    JOIN dbo.Events E ON HO.ObservationUid=E.EventUid
    WHERE HA.AssetStatusId>0;
    """
    s = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) asset statuses from database"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    si = innerjoin(s, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    disallowmissing!(si, :ObservationDate)
    select!(si, :HouseholdId, :ObservationDate, :AssetId, :AssetStatusId)
    Arrow.write(joinpath(stagingpath(node), "AssetStatusRaw.arrow"), si, compress=:zstd)
    return nothing
end
"Retrieve household socioeconmic variables"
function retrieve_hse(db::String, node::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HouseholdUid)) HouseholdUid,
        CONVERT(date,E.EventDate) ObservationDate,
        WaterSource,
        Toilet,
        ConnectedToGrid,
        CookingFuel,
        WallMaterial,
        FloorMaterial,
        Bedrooms,
        Crime,
        FinancialStatus,
        CutMeals,
        CutMealsFrequency,
        NotEat,
        NotEatFrequency,
        ChildMealSkipCut,
        ChildMealSkipCutFrequency,
        ConsentToCall
    FROM dbo.HouseholdObservations HO
        JOIN dbo.Events E ON HO.ObservationUid = E.EventUid;
    """
    s = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) HSE observations from database"
    householdmap = Arrow.Table(joinpath(stagingpath(node), "HouseholdMap.arrow")) |> DataFrame
    si = innerjoin(s, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(si)) $(node) HSE observations after household map"
    select!(si, Not([:HouseholdUid]))
    sort!(si, [:HouseholdId, :ObservationDate])
    Arrow.write(joinpath(stagingpath(node), "SocioEconomicRaw.arrow"), si, compress=:zstd)
    return nothing
end
#endregion
#region Marital status
"Read and save individual marital status observations"
function readmaritalstatuses(node::String, io)
    maritalstatus(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]), io)
end
function maritalstatus(db::String, node::String, periodend::Date, leftcensor::Date, io)
    con = ODBC.Connection(db)
    sql = """SELECT
      UPPER(CONVERT(varchar(50),IndividualUid)) IndividualUid
    , CONVERT(date, EO.EventDate) ObservationDate
    , MaritalStatus
    FROM dbo.IndividualObservations IO
        JOIN dbo.Events EO ON IO.ObservationUid=EO.EventUid
    WHERE NOT MaritalStatus IS NULL
      AND MaritalStatus > 0
    """
    s = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) marital statuses from database"
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "$(nrow(si)) $(node) marital statuses after individual map"
    select!(si, [:IndividualId, :ObservationDate, :MaritalStatus])
    individualbounds = Arrow.Table(joinpath(stagingpath(node), "IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(si, individualbounds, on=:IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    insertcols!(m, :OutsideBounds => false)
    for i = 1:nrow(m)
        if (!ismissing(m[i, :EarliestDate]) && m[i, :ObservationDate] < m[i, :EarliestDate]) || (m[i, :ObservationDate] < leftcensor)
            m[i, :OutsideBounds] = true
        end
        if (!ismissing(m[i, :LatestDate]) && m[i, :ObservationDate] > m[i, :LatestDate]) || (m[i, :ObservationDate] > periodend)
            m[i, :OutsideBounds] = true
        end
    end
    #filter if outside bounds
    filter!([:OutsideBounds] => x -> !x, m)
    @info "$(nrow(m)) $(node) marital statuses inside bounds"
    a = frequency(m, :MaritalStatus)
    @info "Marital Status breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    select!(m, [:IndividualId, :ObservationDate, :MaritalStatus])
    disallowmissing!(m, :ObservationDate)
    sort!(m, [:IndividualId, :ObservationDate])
    Arrow.write(joinpath(stagingpath(node), "MaritalStatus.arrow"), m, compress=:zstd)
    return nothing
end #maritalstatus
#endregion
#region Labour status
function readlabourstatuses(node::String, io)
    labourstatus(settings.Databases[node], node, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]), io)
end
function labourstatus(db::String, node::String, periodend::Date, leftcensor::Date, io)
    con = ODBC.Connection(db)
    sql = """SELECT
      UPPER(CONVERT(varchar(50),IndividualUid)) IndividualUid
    , CONVERT(date, EO.EventDate) ObservationDate
    , CurrentEmployment
    , EmploymentSector
    , EmploymentType
    , Employer
    , Unemployment
    FROM dbo.IndividualObservations IO
        JOIN dbo.Events EO ON IO.ObservationUid=EO.EventUid  
    --WHERE NOT (CurrentEmployment IN (0,100)
    --AND EmploymentSector IN (0,100)
    --AND EmploymentType IN (0,200)
    --AND Employer IN (0,300));
    """
    s = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) labour statuses from database"
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "$(nrow(si)) $(node) labour statuses after individual map"
    select!(si, [:IndividualId, :ObservationDate, :CurrentEmployment, :EmploymentSector, :EmploymentType, :Employer, :Unemployment])
    individualbounds = Arrow.Table(joinpath(stagingpath(node), "IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(si, individualbounds, on=:IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    insertcols!(m, :OutsideBounds => false)
    for i = 1:nrow(m)
        if (!ismissing(m[i, :EarliestDate]) && m[i, :ObservationDate] < m[i, :EarliestDate]) || (m[i, :ObservationDate] < leftcensor)
            m[i, :OutsideBounds] = true
        end
        if (!ismissing(m[i, :LatestDate]) && m[i, :ObservationDate] > m[i, :LatestDate]) || (m[i, :ObservationDate] > periodend)
            m[i, :OutsideBounds] = true
        end
    end
    #filter if outside bounds
    filter!([:OutsideBounds] => x -> !x, m)
    select!(m, [:IndividualId, :ObservationDate, :CurrentEmployment, :EmploymentSector, :EmploymentType, :Employer, :Unemployment])
    disallowmissing!(m, :ObservationDate)
    @info "Read $(nrow(m)) $(node) labour statuses inside bounds"
    a = frequency(m, :CurrentEmployment)
    @info "CurrentEmployment breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(m, :EmploymentSector)
    @info "EmploymentSector breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(m, :EmploymentType)
    @info "EmploymentType breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(m, :Employer)
    @info "Employer breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    a = frequency(m, :Unemployment)
    @info "Unemployment breakdown for $(node)" 
    pretty_table(io, a; alignment=[:c, :r], show_subheader=false)
    sort!(m, [:IndividualId, :ObservationDate])
    Arrow.write(joinpath(stagingpath(node), "LabourStatus.arrow"), m, compress=:zstd)
    return nothing
end #labourstatus
#endregion
#region Pregnancies
function readpregnancies(node::String, io)
    con = ODBC.Connection(settings.Databases[node])
    sql = """    
        SELECT
          UPPER(CONVERT(nvarchar(50),P.WomanUid)) WomanUid,
          CAST(E.EventDate AS DATE) DeliveryDate,
          CASE WHEN PO.LiveBirths >= 10 THEN 1 ELSE PO.LiveBirths END LiveBirths,
          CASE WHEN PO.StillBirths >= 10 THEN 1 ELSE PO.StillBirths END StillBirths,
          PO.TerminationTypeId
        FROM dbo.Pregnancies P
          JOIN dbo.PregnancyOutcomeEvents PO ON P.OutcomeEventUid = PO.EventUid
          JOIN dbo.Events E ON PO.EventUid = E.EventUid
    """
    pregnancies = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    @info "Read $(nrow(pregnancies)) $(node) pregnancies"
    DBInterface.close!(con)
    individualmap = Arrow.Table(joinpath(stagingpath(node), "IndividualMap.arrow")) |> DataFrame
    @info "Read $(nrow(individualmap)) $(node) individualmap entries"
    pregnancies = innerjoin(pregnancies, individualmap, on=:WomanUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(pregnancies)) $(node) pregnancies after join"
    select!(pregnancies, [:IndividualId, :DeliveryDate, :LiveBirths, :StillBirths, :TerminationTypeId])
    sort!(pregnancies, [:IndividualId, :DeliveryDate])
    pregnancies = combine(groupby(pregnancies, [:IndividualId, :DeliveryDate]), :LiveBirths => maximum => :LiveBirths, :StillBirths => maximum => :StillBirths, :TerminationTypeId => maximum => :TerminationTypeId)
    Arrow.write(joinpath(stagingpath(node), "Pregnancies.arrow"), pregnancies, compress=:zstd)
    @info "Wrote $(nrow(pregnancies)) $(node) pregnancies"
    livebirths = frequency(pregnancies, :LiveBirths)
    @info "LiveBirths breakdown $(node)" 
    pretty_table(io, livebirths; alignment=[:c, :r], show_subheader=false)
    stillbirths = frequency(pregnancies, :StillBirths)
    @info "StillBirths breakdown $(node)" 
    pretty_table(io, stillbirths; alignment=[:c, :r], show_subheader=false)
    return nothing
end
#endregion