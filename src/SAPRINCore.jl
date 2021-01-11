module SAPRINCore

using Parameters
using Dates
using ODBC
using DBInterface
using DataFrames
using JSON
using FreqTables
using Arrow
using DataValues
using ShiftedArrays
using XLSX
using Statistics

export readindividuals, readlocations, readresidences, readhouseholds, readhouseholdmemberships, readindividualmemberships,
       readeducationstatuses, readhouseholdsocioeconomic, readmaritalstatuses, readlabourstatuses

#region Settings
function readsettings(f)
    return JSON.parsefile(f; dicttype=Dict, inttype=Int32, use_mmap=false)
end
@with_kw struct Settings 
    PeriodEnd::DateTime = DateTime(s["PeriodEnd"])
    BaseDirectory::String = s["BaseDirectory"]
    Server::String = s["Server"]
    Databases = s["Databases"]
    LeftCensorDates = s["LeftCensorDates"]
    Nodes = s["Nodes"]
end # struct
"Create directories to output staging data"
function createstagingdirectories()
    if !isdir(joinpath(settings.BaseDirectory, "AHRI"))
        mkdir(joinpath(settings.BaseDirectory, "AHRI"))
    end
    if !isdir(joinpath(settings.BaseDirectory, "AHRI", "Staging"))
        mkdir(joinpath(settings.BaseDirectory, "AHRI", "Staging"))
    end
    if !isdir(joinpath(settings.BaseDirectory, "DIMAMO"))
        mkdir(joinpath(settings.BaseDirectory, "DIMAMO"))
    end
    if !isdir(joinpath(settings.BaseDirectory, "DIMAMO", "Staging"))
        mkdir(joinpath(settings.BaseDirectory, "DIMAMO", "Staging"))
    end
    if !isdir(joinpath(settings.BaseDirectory, "Agincourt"))
        mkdir(joinpath(settings.BaseDirectory, "Agincourt"))
    end
    if !isdir(joinpath(settings.BaseDirectory, "Agincourt", "Staging"))
        mkdir(joinpath(settings.BaseDirectory, "Agincourt", "Staging"))
    end
    return nothing
end
#endregion
#region startup
s = readsettings("settings.json")
settings = Settings()
createstagingdirectories()
#endregion
#region Utility functions
"Constrain date a to be no larger than b"
function rightcensor(a::DataValue{Date}, b::Date)::Date
    return rightcensor(get(a, b), b) # get returns underlying Date, with default b if a is missing (isna)
end
function rightcensor(a::Date, b::Date)
    return a <= b ? a : b
end
"Returns a random date relative to base, constraint by start and end date"
function newrandomdate(base::Date, startdate::Date, enddate::Date)
    return base + Dates.Day(trunc(Int64, Dates.value(enddate - startdate) * rand(Float64)))
end
function convertanytoint(a)
    return convert(Int64, a)
end
function convertanytostr(a)
    return string(a)
end
#endregion
#region individuals
"Read individuals and anonimise for specified node specified and save individual data, id map and bounds to to arrow files"
function readindividuals(node::String)
    readindividuals_internal(settings.Databases[node], node, settings.BaseDirectory)
    individualobservationbounds(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end

"Read individuals and anonimise for node specified in settings and save id map and individual data to to arrow files"
function readindividuals_internal(db::String, node::String, basedirectory::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),I.IndividualUid)) IndividualUid,
        I.Sex,
        CONVERT(date,SE.EventDate) DoB,
        CASE
        WHEN EE.EventTypeId=7 THEN CONVERT(date,EE.EventDate)
        ELSE NULL
        END DoD,
        UPPER(CONVERT(nvarchar(50),I.MotherUid)) MotherUid,
        UPPER(CONVERT(nvarchar(50),I.FatherUid)) FatherUid,
        I.MotherDoD,
        I.FatherDoD
    FROM dbo.Individuals I
        JOIN dbo.Events SE ON I.BirthEventUid=SE.EventUid
        JOIN dbo.Events EE ON I.EndEventUid=EE.EventUid
    """    
    individuals = DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    @info "Read $(nrow(individuals)) $(node) individuals"
    sex = freqtable(individuals, :Sex)
    @info "Sex breakdown $(node)" sex
    sort!(individuals, :IndividualUid)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),WomanUid)) WomanUid
        , UPPER(CONVERT(nvarchar(50),I.IndividualUid)) ChildUid
        FROM dbo.Pregnancies P
            JOIN dbo.Individuals I ON P.OutcomeEventUid=I.BirthEventUid
    """  
    pregnancies = DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(pregnancies)) $(node) pregnancies"
    pregnancies = unique!(pregnancies, :ChildUid)
    @info "Read $(nrow(pregnancies)) $(node) unique children"
    # Add MotherUid from pregnancies
    individuals = leftjoin(individuals, pregnancies, on=:IndividualUid => :ChildUid, makeunique=true, matchmissing=:equal)
    for i = 1:nrow(individuals)
        if ismissing(individuals[i,:MotherUid]) && !ismissing(individuals[i,:WomanUid])
            individuals[i,:MotherUid] = individuals[i,:WomanUid]
        end
    end
    individuals.IndividualId = 1:nrow(individuals)
    # Convert gui ids to integer ids
    map = individuals[!,[:IndividualUid,:IndividualId]]
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMap.arrow"), map, compress=:zstd)
    # Convert mother and father uids to corresponding integer ids
    individuals = leftjoin(individuals, map, on=:MotherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    individuals = leftjoin(individuals, map, on=:FatherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    # Select and rename final columns
    select!(individuals, [:IndividualId,:Sex,:DoB,:DoD,:IndividualId_1,:IndividualId_2,:MotherDoD,:FatherDoD])
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
        if !ismissing(individuals[i,:MotherDoD_1])
            individuals[i,:MotherDoD] = individuals[i,:MotherDoD_1]
        end
        if !ismissing(individuals[i,:FatherDoD_1])
            individuals[i,:FatherDoD] = individuals[i,:FatherDoD_1]
        end
    end
    select!(individuals, [:IndividualId, :Sex, :DoB, :DoD, :MotherId, :MotherDoD, :FatherId, :FatherDoD])
    disallowmissing!(individuals, [:IndividualId, :Sex, :DoB])
    Arrow.write(joinpath(basedirectory, node, "Staging", "Individuals.arrow"), individuals, compress=:zstd)
    @info "Wrote $(nrow(individuals)) $(node) individuals"
    return nothing
end # readindividuals
"Creates a dataset with the earliest and latest date at which an individual has been observed within left and rightcensor dates"
function individualobservationbounds(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),O.IndividualUid)) IndividualUid,
        CONVERT(date,E.EventDate) EventDate
    FROM dbo.IndividualObservations O
        JOIN dbo.Events E ON O.ObservationUid=E.EventUid
    """
    observations =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(observations)) $(node) individual observations"
    filter!(:EventDate => s -> s <= periodend, observations) # event must be before period end
    filter!(:EventDate => s -> s >= leftcensor, observations) # event must be after left censor date
    @info "Read $(nrow(observations)) $(node) individual observations after bounds"
    bounds = combine(groupby(observations, :IndividualUid), :EventDate => minimum => :EarliestDate, :EventDate => maximum => :LatestDate)
    disallowmissing!(bounds, :IndividualUid)
    @info "Read $(nrow(bounds)) $(node) individuals after group"
    individualmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "IndividualMap.arrow")) |> DataFrame
    bounds = innerjoin(bounds, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    select!(bounds, [:IndividualId, :EarliestDate, :LatestDate])
    sort!(bounds, :IndividualId)
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualBounds.arrow"), bounds, compress=:zstd)
    @info "Wrote $(nrow(bounds)) $(node) individual bounds"
    return nothing
end
#endregion individuals
#region locations
function readlocations(node::String)
    readlocations_internal(settings.Databases[node], node, settings.BaseDirectory)
end
"Read locations and anonimise for node save id map and location data to to arrow files"
function readlocations_internal(db::String, node::String, basedirectory::String)
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
    map = locations[!,[:LocationUid,:LocationId]]
    Arrow.write(joinpath(basedirectory, node, "Staging", "LocationMap.arrow"), map, compress=:zstd)
    areas = select(locations,[:AreaUid]) # |> @select(:AreaUid) |> @filter(!isna(_.AreaUid)) |> @unique() |> DataFrame
    dropmissing!(areas)
    unique!(areas)
    areas.AreaId = 1:nrow(areas)
    Arrow.write(joinpath(basedirectory, node, "Staging", "AreaMap.arrow"), areas, compress=:zstd)
    locations = leftjoin(locations, areas, on=:AreaUid => :AreaUid, makeunique=true, matchmissing=:equal)
    locations = select!(locations, [:LocationId,:NodeId,:LocationTypeId,:AreaId])
    Arrow.write(joinpath(basedirectory, node, "Staging", "Locations.arrow"), locations, compress=:zstd)
    a = freqtable(locations, :AreaId)
    @info "Area breakdown for $(node)" a
    return nothing
end # readlocations
#endregion locations
#region residencies
"Retrieve and save residence episodes, assumes individual and locations have been read"
function readresidences(node::String)
    readresidences_internal(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
    if node in ["Agincourt", "DIMAMO"]
        eliminateresidenceoverlaps(node, settings.BaseDirectory)
        readresidencestatus(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
        dropnonresidentepisodes(settings.BaseDirectory, node)
    end
end
"Retrieve and save residence episodes directly from database"
function readresidences_internal(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
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
    individualmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "IndividualMap.arrow")) |> DataFrame
    residences = innerjoin(residences, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "LocationMap.arrow")) |> DataFrame
    residences = innerjoin(residences, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(residences, [:IndividualId,:LocationId,:StartDate,:StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
    a = freqtable(residences, :StartType)
    @info "Start types $(node) before normalisation" a
    years = Dates.year.(residences.StartDate) 
    a = freqtable(years)
    @info "Start years breakdown $(node) before normalisation" a
    a = freqtable(residences, :EndType)
    @info "End types $(node) before normalisation" a
    years = Dates.year.(residences.EndDate) 
    a = freqtable(years)
    @info "End years breakdown $(node) before normalisation" a
    # Do recodes
    recodeStarts = Set([100,102,999])
    recodeEnds = Set([103,999])
    for i = 1:nrow(residences)
        if residences[i, :StartDate] < leftcensor
            residences[i, :StartType] = 1 # set to enumeration
            residences[i, :StartDate] = Date(residences[i, :StartObservationDate])
        end
        if residences[i,:EndDate] > periodend
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
    filter!([:StartDate,:EndDate] => (s, e) -> s <= e, residences) # start date must be smaller or equal to end date

    sort!(residences, [:IndividualId,:StartDate,:StartType])
    residences.ResidenceId = 1:nrow(residences)
    insertcols!(residences, :ResidentIndex, :GapStart => 0, :GapEnd => 0)
    df = combine(groupby(residences, :IndividualId), :LocationId, :ResidenceId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex, :GapStart, :GapEnd,
                                     :StartDate => Base.Fix2(lead, 1) => :NextStart, :EndDate => Base.Fix2(lag, 1) => :LastEnd)
    for i = 1:nrow(df)
        if !ismissing(df[i,:NextStart])
            gap = Dates.value(df[i,:NextStart] - df[i,:EndDate])
            if gap <= 0
                df[i,:EndType] = 5 # internal outmigration EXT
             elseif gap > 180 && df[i,:EndType] != 300 # exclude refusals
                df[i,:EndType] = 4 # external outmigration OMG
            end
        end
        if !ismissing(df[i,:LastEnd])
            gap = Dates.value(df[i,:StartDate] - df[i,:LastEnd])
            if gap <= 0
                df[i,:StartDate] = df[i,:LastEnd] + Dates.Day(1)
                df[i,:StartType] = 6 # internal inmigration ENT
            elseif gap > 0 && gap <= 180 && df[i,:StartType] == 6
                df[i,:StartDate] = df[i,:LastEnd] + Dates.Day(1) # close internal migration gap
            elseif gap > 180 && !(df[i,:StartType] == 300 || df[i,:StartType] == 301) # exclude refusals
                df[i,:StartType] = 3 # external inmigration
            end
        end
    end
    filter!([:StartDate,:EndDate] => (s, e) -> s <= e, df) # start date must be smaller or equal to end date
    select!(df, [:ResidenceId, :IndividualId, :LocationId, :StartDate,:StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
    disallowmissing!(df, [:StartDate,:StartType,:EndDate,:EndType,:ResidentIndex])
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow"), df, compress=:zstd)
    years = Dates.year.(residences.StartDate) 
    a = freqtable(years)
    @info "Start years breakdown $(node)" a
    years = Dates.year.(residences.EndDate) 
    a = freqtable(years)
    @info "End years breakdown $(node)" a
    a = freqtable(residences, :StartType)
    @info "Start types $(node)" a
    a = freqtable(residences, :EndType)
    @info "End types $(node)" a
    return nothing
end
"Decompose residences into days and eliminate overlaps"
function eliminateresidenceoverlaps(node::String, basedirectory::String)
    residences = Arrow.Table(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow")) |> DataFrame
    @info "Node $(node) $(nrow(residences)) episodes before overlap elimination"
    select!(residences, [:ResidenceId, :IndividualId, :LocationId, :StartDate,:StartType, :EndDate, :EndType, :ResidentIndex])
    insertcols!(residences, :ResidentIndex, :GapStart => 0, :GapEnd => 0, :Gap => 0)
    s = similar(residences, 0)
    for row in eachrow(residences)
        tf = DataFrame(row)
        ttf = repeat(tf, Dates.value.(row.EndDate - row.StartDate) + 1)
        ttf.DayDate = ttf.StartDate .+ Dates.Day.(0:nrow(ttf) - 1)
        ttf.Start = ttf.DayDate .== ttf.StartDate
        ttf.End = ttf.DayDate .== ttf.EndDate
        append!(s, ttf, cols=:union)
    end
    @info "$(nrow(s)) day rows for $(node)"
    sort!(s, [:IndividualId,:DayDate,order(:ResidentIndex, rev=true), :StartDate, order(:EndDate, rev=true)]);
    unique!(s, [:IndividualId,:DayDate]);
    @info "$(nrow(s)) unique day rows for $(node)"
    lastindividual = -1
    gap = 0
    n = nrow(s)
    for i = 1:n
        if lastindividual != s[i,:IndividualId]
            lastindividual = s[i,:IndividualId]
            gap = 0
        else
            lastgap = Dates.value(s[i,:DayDate] - s[i - 1,:DayDate])
            nextgap = 0
            if i < n
                nextgap = s[i,:IndividualId] != s[i + 1,:IndividualId] ? 0 : Dates.value(s[i + 1,:DayDate] - s[i,:DayDate])
            end
            if lastgap > 1
                s[i,:GapEnd] = 1
                gap = gap == 0 ? 1 : 0
            end
            if nextgap > 1
                s[i,:GapStart] = 1
            end
            s[i,:Gap] = gap
        end
    end
    df = combine(groupby(s, [:IndividualId,:Gap,:LocationId]), :DayDate => minimum => :StartDate, :StartType => first => :StartType, 
                                                               :DayDate => maximum => :EndDate, :EndType => last => :EndType, 
                                                               :GapStart => maximum => :GapStart, :GapEnd => maximum => :GapEnd,
                                                               :ResidentIndex => mean => :ResidentIndex)
    @info "Node $(node) $(nrow(df)) episodes after overlap elimination"
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualResidenciesIntermediate.arrow"), df, compress=:zstd)
    return nothing
end
"Read resident status observations"
function readresidencestatus(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """/*
    Designed to smooth single instances of changed resident status over
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
        WHEN ResStatusCode IN ('X','Q') AND (ResidentStatus>6) THEN CAST(1 AS int)
        WHEN ResStatusCode IN ('X','Q') AND (ResidentStatus<6) THEN CAST(2 AS int)
        WHEN ResStatusCode IN ('X','Q') THEN CAST(1 AS int)
        WHEN ResStatusCode <> 'P' THEN CAST(2 AS int)
        WHEN ResidentStatus>6 THEN CAST(1 AS int)
        ELSE CAST(1 AS int)
      END ResidentStatus -- 1 Resident 2 Non-resident
    FROM SmoothedStatus
    """
    resstatuses = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(resstatuses)) $(node) residence statuses"
    filter!(:ObservationDate => s -> s <= periodend, resstatuses)        # drop statuses after period end
    filter!(:ObservationDate => s -> s >= leftcensor, resstatuses)       # drop statuses before leftcensor date
    individualmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "IndividualMap.arrow")) |> DataFrame
    resstatuses = innerjoin(resstatuses, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "LocationMap.arrow")) |> DataFrame
    resstatuses = innerjoin(resstatuses, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(resstatuses, [:IndividualId,:LocationId,:ObservationDate,:ResidentStatus])
    disallowmissing!(resstatuses, [:ObservationDate,:ResidentStatus])
    sort!(resstatuses, [:IndividualId,:LocationId,:ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "ResidentStatus.arrow"), resstatuses, compress=:zstd)
    @info "Wrote $(nrow(resstatuses)) $(node) residence statuses"
    return nothing
end # readresidencestatus
"Use resident status observations to identify non-resident episodes and drop those from residency episodes"
function dropnonresidentepisodes(basedirectory::String, node::String)
    r = Arrow.Table(joinpath(basedirectory, node, "Staging", "IndividualResidenciesIntermediate.arrow")) |> DataFrame
    @info "$(nrow(r)) $(node) residence rows"
    rs = Arrow.Table(joinpath(basedirectory, node, "Staging", "ResidentStatus.arrow")) |> DataFrame
    @info "$(nrow(rs)) $(node) residence status rows"

    s = outerjoin(r, rs, on=[:IndividualId => :IndividualId, :LocationId => :LocationId])
    # eliminate records that didn't join properly
    dropmissing!(s, :LocationId, disallowmissing=true)
    dropmissing!(s, :Gap, disallowmissing=true)
    replace!(s.ResidentStatus, missing => 1)
    disallowmissing!(s, [:IndividualId,:StartDate,:StartType,:EndDate,:EndType, :GapStart, :GapEnd, :ResidentIndex, :ResidentStatus])
    
    for i = 1:nrow(s)
        od = s[i,:ObservationDate]
        if ismissing(od)
            s[i,:ObservationDate] = s[i,:StartDate]
        end
    end
            
    s = s[((s.ObservationDate .>= s.StartDate) .& (s.ObservationDate .<= s.EndDate)), :] # drop records where observation date is out of bounds
    df = combine(groupby(s, :IndividualId), :LocationId, :StartDate, :StartType, :EndDate, :EndType, :ResidentIndex, :Gap, :GapStart, :GapEnd, :ObservationDate, :ResidentStatus,
                :ResidentStatus => Base.Fix2(lag, 1) => :LastResidentStatus, 
                :LocationId => Base.Fix2(lag, 1) => :LastLocationId, :Gap => Base.Fix2(lag, 1) => :LastGap)
    insertcols!(df, :episode => 0)
    episode = 0
    for i = 1:nrow(df)
        if ismissing(df[i,:LastLocationId])
            episode = 1
            df[i,:episode] = episode
        else
            location = df[i,:LocationId]
            lastlocation = df[i,:LastLocationId]
            gap = df[i,:Gap]
            lastgap = df[i,:LastGap]
            if location != lastlocation || gap != lastgap
                episode = episode + 1
                df[i,:episode] = episode
            else
                residentstatus = df[i,:ResidentStatus]
                lastresidentstatus = df[i,:LastResidentStatus]
                if residentstatus != lastresidentstatus
                episode = episode + 1
                    df[i,:episode] = episode
                else
                    df[i,:episode] = episode
                end
            end
        end
    end
    s = combine(groupby(df, [:IndividualId,:LocationId,:episode]), :StartDate => first => :StartDate, :StartType => first => :StartType, 
                :EndDate => first => :EndDate, :EndType => first => :EndType, 
                :Gap => first => :Gap,
                :GapStart => first => :GapStart, :GapEnd => first => :GapEnd,
                :ResidentStatus => first => :ResidentStatus, :ResidentIndex => mean => :ResidentIndex,
                :ObservationDate => minimum => :StartObservationDate, :ObservationDate => maximum => :EndObservationDate)
    @info "Node $(node) $(nrow(s)) episodes after resident split"
    df = combine(groupby(s, :IndividualId), :LocationId, :episode, :StartDate, :StartType, :EndDate, :EndType, :Gap, :GapStart, :GapEnd, :ResidentStatus, :StartObservationDate, :EndObservationDate, :ResidentIndex,
                :ResidentStatus => Base.Fix2(lead, 1) => :NextResidentStatus, 
                :ResidentStatus => Base.Fix2(lag, 1) => :LastResidentStatus, 
                :StartObservationDate => Base.Fix2(lead, 1) => :NextStartObsDate,
                :EndObservationDate => Base.Fix2(lag, 1) => :LastEndObsDate)
    for i = 1:nrow(df)
        if df[i,:ResidentStatus] == 1 && !ismissing(df[i,:NextResidentStatus]) && df[i,:NextResidentStatus] == 2
            df[i,:EndDate] = newrandomdate(df[i,:EndObservationDate], df[i,:EndObservationDate], df[i,:NextStartObsDate])
            df[i,:EndType] = 4
        end
        if df[i,:ResidentStatus] == 1 && !ismissing(df[i,:LastResidentStatus]) && df[i,:LastResidentStatus] == 2
            df[i,:StartDate] = newrandomdate(df[i,:LastEndObsDate], df[i,:LastEndObsDate], df[i,:StartObservationDate])
            df[i,:StartType] = 3
        end
    end
    filter!(:ResidentStatus => s -> s == 1, df)
    @info "Node $(node) $(nrow(df)) episodes after dropping non-resident episodes"
    df.ResidenceId = 1:nrow(df)
    select!(df, [:ResidenceId, :IndividualId, :LocationId, :StartDate,:StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
    mv(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow"), joinpath(basedirectory, node, "Staging", "IndividualResidencies_old.arrow"), force=true)
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow"), df, compress=:zstd)
    return nothing
end # dropnonresidentepisodes
#endregion
#region Households
"Read households and save household map and data to staging directory"
function readhouseholds(node::String)
    readhouseholds_internal(settings.Databases[node], node, settings.BaseDirectory)
    readhouseholdresidences(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end
"Read households and save household map and data to staging directory"
function readhouseholds_internal(db::String, node::String, basedirectory::String)
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
    households = DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    @info "Read $(nrow(households)) $(node) households"
    DBInterface.close!(con)
    households.HouseholdId = 1:nrow(households)
    map = households[!,[:HouseholdUid,:HouseholdId]]
    Arrow.write(joinpath(basedirectory, node, "Staging", "HouseholdMap.arrow"), map, compress=:zstd)
    select!(households, [:HouseholdId, :StartDate, :StartType, :EndDate, :EndType])
    Arrow.write(joinpath(basedirectory, node, "Staging", "Households.arrow"), households, compress=:zstd)
    return nothing
end # readhouseholds
"Retrieve household residencies with left and right censor dates"
function readhouseholdresidences(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
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
    residences =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
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
    relationships =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(relationships)) $(node) household relationships"
    lastseen = combine(groupby(relationships, :HouseholdUid), :EndDate => maximum => :LastHHDate, :EndObservationDate => maximum => :LastObservationDate)
    @info "Grouped $(nrow(lastseen)) $(node) households from household relationships"
    sort!(residences, [:HouseholdUid,:StartDate])
    residences = combine(groupby(residences, :HouseholdUid), sdf -> sort(sdf, :StartDate), s -> 1:nrow(s), nrow => :Episodes)
    rename!(residences, :x1 => :Episode)
    @info "Household residences $(nrow(residences)) $(node) after episode counts"
    r = leftjoin(residences, lastseen, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    for i = 1:nrow(r)
        if r[i, :StartDate] < leftcensor
            r[i, :StartType] = 1 # set to enumeration
            r[i, :StartDate] = Date(residences[i, :StartObservationDate])
        end
        if !ismissing(r[i, :LastHHDate]) && r[i, :Episode] == r[i, :Episodes] && r[i, :LastHHDate] > r[i, :EndDate]
            r[i,:EndDate] = r[i,:LastHHDate]
            r[i,:EndObservationDate] = r[i,:LastObservationDate]
        end
        if r[i,:EndDate] > periodend
            r[i, :EndDate] = periodend # right censor to period end
            r[i, :EndType] = 9 # end of episode beyond periodend => OBE
        end
    end
    filter!(:StartDate => s -> s <= periodend, r)        # drop episodes that start after period end
    filter!([:StartDate,:EndDate] => (s, e) -> s <= e, r) # start date must be smaller or equal to end date
    select!(r, [:HouseholdUid,:LocationUid,:StartDate,:StartType,:EndDate,:EndType,:StartObservationDate,:EndObservationDate])
    @info "Household residences $(nrow(r)) $(node) after right censor"
    householdmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "HouseholdMap.arrow")) |> DataFrame
    r = innerjoin(r, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(basedirectory, node, "Staging", "LocationMap.arrow")) |> DataFrame
    r = innerjoin(r, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(r, [:HouseholdId,:LocationId,:StartDate,:StartType,:EndDate,:EndType,:StartObservationDate,:EndObservationDate])
    disallowmissing!(r, [:StartDate,:EndDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "HouseholdResidences.arrow"), r, compress=:zstd)
    @info "Wrote $(nrow(r)) $(node) household residences"
    return nothing
end
#endregion
#region HouseholdMemberships
"Retrieve and save household membership and householdhead relationships data"
function readhouseholdmemberships(node::String)
    readhouseholdmemberships_internal(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
    readhouseholdheadrelationships(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end
"Retrieve household memberships with left and right censor dates"
function readhouseholdmemberships_internal(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
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
    memberships =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(memberships)) $(node) membership episodes from database"
    householdmap = Arrow.Table(joinpath(basedirectory,node,"Staging","HouseholdMap.arrow")) |> DataFrame
    memberships = innerjoin(memberships, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(memberships)) $(node) membership episodes"
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    memberships = innerjoin(memberships, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(memberships)) $(node) membership episodes"
    individualbounds = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(memberships,individualbounds, on = :IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(m)) $(node) membership episodes after individual bounds join"
    #adjust start and end dates
    for i = 1:nrow(m)
        if m[i,:StartDate] < leftcensor && !ismissing(m[i,:EarliestDate])
            m[i,:StartDate] = m[i,:EarliestDate]
            m[i,:StartObservationDate] = m[i,:EarliestDate]
            m[i,:StartType] = 1
        end
        if m[i,:EndDate] > periodend
            m[i,:EndDate] = periodend
            m[i,:EndType] = 9
        end
    end
    filter!(:StartDate => s -> s <= periodend, m)         # drop episodes that start after period end
    filter!([:StartDate,:EndDate] => (s, e) -> s <= e, m) # start date must be smaller or equal to end date
    sort!(m,[:IndividualId,:HouseholdId,:StartDate])
    m.MembershipId = 1:nrow(m)
    transform!(groupby(m,[:IndividualId,:HouseholdId]), :IndividualId => eachindex => :Episode)
    select!(m,[:MembershipId, :IndividualId, :HouseholdId, :StartDate, :StartType, :StartObservationDate, :EndDate, :EndType, :EndObservationDate, :Episode])
    Arrow.write(joinpath(basedirectory, node, "Staging", "HouseholdMemberships.arrow"), m, compress=:zstd)
    @info "Wrote $(nrow(m)) $(node) membership episodes"
    return nothing
end #readhouseholdmemberships
"Retrieve household head relationships with left and right censor dates"
function readhouseholdheadrelationships(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
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
    relationships =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(relationships)) $(node) relationships episodes from database"
    householdmap = Arrow.Table(joinpath(basedirectory,node,"Staging","HouseholdMap.arrow")) |> DataFrame
    relationships = innerjoin(relationships, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(relationships)) $(node) relationships episodes"
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    relationships = innerjoin(relationships, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(relationships)) $(node) relationships episodes"
    individualbounds = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(relationships,individualbounds, on = :IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(m)) $(node) relationships episodes after individual bounds join"
    #adjust start and end dates
    for i = 1:nrow(m)
        if m[i,:StartDate] < leftcensor && !ismissing(m[i,:EarliestDate])
            m[i,:StartDate] = m[i,:EarliestDate]
            m[i,:StartObservationDate] = m[i,:EarliestDate]
            m[i,:StartType] = 1
        end
        if m[i,:EndDate] > periodend
            m[i,:EndDate] = periodend
            m[i,:EndType] = 9
        end
    end
    filter!(:StartDate => s -> s <= periodend, m)         # drop episodes that start after period end
    filter!([:StartDate,:EndDate] => (s, e) -> s <= e, m) # start date must be smaller or equal to end date
    sort!(m,[:IndividualId,:HouseholdId, :StartDate, :HHRelationshipTypeId])
    m.RelationshipId = 1:nrow(m)
    transform!(groupby(m,[:IndividualId,:HouseholdId]), :IndividualId => eachindex => :Episode)
    select!(m,[:RelationshipId, :IndividualId, :HouseholdId, :HHRelationshipTypeId, :StartDate, :StartType, :StartObservationDate, :EndDate, :EndType, :EndObservationDate, :Episode])
    Arrow.write(joinpath(basedirectory, node, "Staging", "HHeadRelationships.arrow"), m, compress=:zstd)
    @info "Wrote $(nrow(m)) $(node) relationships episodes"
    return nothing
end #readhouseholdheadrelationships
#endregion
#region IndividualMemberships
"Decompose household memberships to days and combine with houeshold relationships and produced consolidated individual hosuehold membership episodes with household head relationship"
function readindividualmemberships(node::String, batchsize::Int64)
    batchmemberships(settings.BaseDirectory, node, batchsize)
end
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
    @info "Unique membership days $(nrow(m)) from $(fromId) to $(toId)"
    return m
end #getmembershipdays
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
    @info "Unique relationship days $(nrow(r)) from $(fromId) to $(toId)"
    return r
end #getrelationshipdays
function individualmemberships(basedirectory::String, node::String, fromId::Int64, toId::Int64, batch::Int64)
    mr = leftjoin(getmembershipdays(basedirectory,node,fromId,toId), getrelationshipdays(basedirectory,node,fromId,toId), on = [:IndividualId => :IndividualId, :HouseholdId => :HouseholdId, :DayDate => :DayDate], makeunique=true, matchmissing=:equal)
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
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualMemberships$(batch).arrow"), memberships, compress=:zstd)
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
    Threads.@threads for i = 1:batches
        fromId = minId + batchsize * (i-1)
        toId = min(maxId, (minId + batchsize * i)-1)
        @info "Batch $(i) from $(fromId) to $(toId)"
        individualmemberships(basedirectory,node,fromId,toId,i)
    end
    combinemembershipbatch(basedirectory,node,batches)
    return nothing
end #batchmemberships

#endregion
#region EducationStatuses
function readeducationstatuses(node::String)
    educationstatus(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end
function educationstatus(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
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
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) education statuses from database"
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    individualbounds = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(si, individualbounds, on = :IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    insertcols!(m,:OutsideBounds => false)
    for i=1:nrow(m)
        if (!ismissing(m[i,:EarliestDate]) && m[i,:ObservationDate] < m[i,:EarliestDate]) || (m[i,:ObservationDate] < leftcensor)
            m[i,:OutsideBounds]=true
        end
        if (!ismissing(m[i,:LatestDate]) && m[i,:ObservationDate] > m[i,:LatestDate]) || (m[i,:ObservationDate] > periodend)
            m[i,:OutsideBounds]=true
        end
    end
    #filter if outside bounds
    filter!([:OutsideBounds] => x -> !x, m)
    @info "Read $(nrow(m)) $(node) education statuses inside bounds"
    filter!([:CurrentEducation, :HighestSchoolLevel, :HighestNonSchoolLevel] => (x,y,z) -> !(x<0 && y<0 && z<0), m)
    @info "Read $(nrow(m)) $(node) education statuses not missing"
    disallowmissing!(m,[:ObservationDate])
    select!(m,[:IndividualId,:ObservationDate,:CurrentEducation, :HighestSchoolLevel, :HighestNonSchoolLevel])
    sort!(m,[:IndividualId,:ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "EducationStatuses.arrow"), m, compress=:zstd)
    return nothing
end
#endregion
#region household socio-economic
"Read and save household socio-economic data"
function readhouseholdsocioeconomic(node::String)
    householdassets(settings.Databases[node], node, settings.basedirectory)
    householdsocioeconomic(settings.Databases[node], node, settings.basedirectory)
end
function householdassets(db::String, node::String, basedirectory::String)
    con = ODBC.Connection(db)
    sql = """SELECT
        UPPER(CONVERT(varchar(50),HA.HouseholdObservationUid)) HouseholdObservationUid,
        HA.AssetId,
        HA.AssetStatusId
    FROM dbo.HouseholdAssets HA
    WHERE HA.AssetStatusId>0;
    """
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) asset statuses from database"
    assetmap = DataFrame(XLSX.readtable("Assets.xlsx","Consolidated")...)
    assetmap[!,:AssetId] = map(convertanytoint,assetmap[!,:AssetId])
    assetmap[!,:Id] = map(convertanytoint,assetmap[!,:Id])
    assetmap[!,:AssetName] = map(convertanytostr,assetmap[!,:AssetName])
    assetmap[!,:Name] = map(convertanytostr,assetmap[!,:Name])
    assetmap[!,:AssetIdx] = map(convertanytostr,assetmap[!,:AssetIdx])    
    si = innerjoin(s, assetmap, on = :AssetId => :AssetId,  makeunique=true, matchmissing=:equal)
    g = combine(groupby(si,[:HouseholdObservationUid,:Id]), :AssetStatusId => minimum => :AssetStatus, :AssetIdx => first => :AssetGroup)
    @info "$(nrow(g)) $(node) grouped asset statuses"
    filter!([:AssetStatus] => x -> x == 1, g)
    @info "$(nrow(g)) $(node) present asset statuses"
    gg = combine(groupby(g,[:HouseholdObservationUid,:AssetGroup]), :AssetStatus => sum => :Idx)
    @info "$(nrow(gg)) $(node) asset groups"
    filter!([:AssetGroup] => x -> x != "0",gg)
    w = unstack(gg, :HouseholdObservationUid, :AssetGroup, :Idx)
    replace!(w.Modern, missing => 0)
    replace!(w.Livestock, missing => 0)
    disallowmissing!(w,[:HouseholdObservationUid,:Modern,:Livestock])
    a = freqtable(w,:Modern)
    @info "Modern asset breakdown for $(node)" a
    a = freqtable(w,:Livestock)
    @info "Livestock asset breakdown for $(node)" a
    Arrow.write(joinpath(basedirectory, node, "Staging", "AssetStatus.arrow"), w, compress=:zstd)
    return nothing
end #householdassets
function householdsocioeconomic(db::String, node::String, basedirectory::String)
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
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) HSE observations from database"
    householdmap = Arrow.Table(joinpath(basedirectory,node,"Staging","HouseholdMap.arrow")) |> DataFrame
    si = innerjoin(s, householdmap, on=:HouseholdUid => :HouseholdUid, makeunique=true, matchmissing=:equal)
    @info "Read $(nrow(si)) $(node) HSE observations after household map"
    householdassets = Arrow.Table(joinpath(basedirectory,node,"Staging","AssetStatus.arrow")) |> DataFrame
    s = leftjoin(si,householdassets,on = :HouseholdObservationUid => :HouseholdObservationUid, makeunique=true, matchmissing=:equal)
    select!(s,Not([:HouseholdObservationUid,:HouseholdUid]))
    a = freqtable(s,:WaterSource)
    @info "Watersource breakdown for $(node)" a
    recode!(s[!,:WaterSource], missing =>0, 1 =>4, 2 => 3, 3 => 2, 4 => 1, 5 => 1, 6 => 1, 7 => 1, 8 => 1, 9 => 0, 10 => 2, 11 => 1)
    a = freqtable(s,:WaterSource)
    @info "Watersource breakdown for $(node) after recode" a
    a = freqtable(s,:Toilet)
    @info "Toilet breakdown for $(node)" a
    recode!(s[!,:Toilet], missing => 0, 0 => 0, 1 => 3, [2,5] => 2, [3, 4] => 1)
    a = freqtable(s,:Toilet)
    @info "Toilet breakdown for $(node) after recode" a
    a = freqtable(s,:CookingFuel)
    @info "CookingFuel breakdown for $(node)" a
    recode!(s[!,:CookingFuel], missing => 0, 0 => 0, 1 => 1, 2 => 4, 3 => 2, 4 => 5, [5,6] => 3)
    a = freqtable(s,:CookingFuel)
    @info "CookingFuel breakdown for $(node) after recode" a
    a = freqtable(s,:WallMaterial)
    @info "WallMaterial breakdown for $(node)"
    recode!(s[!,:WallMaterial], missing => 0, 0 => 0, [1, 2] => 4, 3 => 3, 4 => 2, [5, 6, 7] => 1)
    a = freqtable(s,:WallMaterial)
    @info "WallMaterial breakdown for $(node) after recode" a
    a = freqtable(s,:FloorMaterial)
    @info "FloorMaterial breakdown for $(node)" a
    recode!(s[!,:FloorMaterial], missing => 0, 0 => 0, [1, 2, 8, 9] => 3, [3, 6, 10] => 2, [11, 12, 13] => 1)
    a = freqtable(s,:FloorMaterial)
    @info "FloorMaterial breakdown for $(node) after recode"
    a = freqtable(s,:Bedrooms)
    @info "Bedrooms breakdown for $(node)" a
    recode!(s[!,:Bedrooms], missing => 0, 0 => 0, [1, 2] => 1, [3, 4] => 2, [5, 6] => 3, 7:90 => 4, 91:99 => 0, 100:9999 => 4)
    @info "Bedrooms breakdown for $(node) after recode"
    a = freqtable(s,:ConnectedToGrid)
    @info "ConnectedToGrid breakdown for $(node)" a
    replace!(s.ConnectedToGrid, missing => 0, true => 1, false => 0)
    a = freqtable(s,:ConnectedToGrid)
    @info "ConnectedToGrid for $(node) after recode"
    transform!(s,[:Bedrooms,:WallMaterial,:FloorMaterial] => ByRow((b,w,f) -> (b/4 + w/4 + f/3)/3) => :DwellingIdx, 
                 [:WaterSource,:Toilet] => ByRow((w,t) -> (w/4 + t/3)/2) => :WaterSanitationIdx, 
                 [:ConnectedToGrid, :CookingFuel] => ByRow((x,y) -> ((x + y/5)/2)) => :PowerSupplyIdx,
                 [:Livestock] => ByRow(x -> x/2) => :LivestockIdx,
                 [:Modern] => ByRow(x -> x/9) => :ModernAssetIdx)
    transform!(s,[:DwellingIdx,:WaterSanitationIdx,:PowerSupplyIdx,:LivestockIdx, :ModernAssetIdx] => ByRow((a,b,c,d,e) -> (a + b + c + d + e)) => :SEIdx)
    select!(s, [:HouseholdId,:ObservationDate,:SEIdx,:DwellingIdx,:WaterSanitationIdx,:PowerSupplyIdx,:LivestockIdx, :ModernAssetIdx, 
                :Crime, :FinancialStatus, :CutMeals, :CutMealsFrequency, :NotEat, :NotEatFrequency, :ChildMealSkipCut, :ChildMealSkipCutFrequency, :ConsentToCall])
    sort!(s, [:HouseholdId,:ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "SocioEconomic.arrow"), s, compress=:zstd)
    return nothing
end #householdsocioeconomic
#endregion
#region Marital status
"Read and save individual marital status observations"
function readmaritalstatuses(node::String)
    maritalstatus(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end
function maritalstatus(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
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
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) marital statuses from database"
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "$(nrow(si)) $(node) marital statuses after individual map"
    select!(si,[:IndividualId, :ObservationDate, :MaritalStatus])
    individualbounds = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(si, individualbounds, on = :IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    insertcols!(m,:OutsideBounds => false)
    for i=1:nrow(m)
        if (!ismissing(m[i,:EarliestDate]) && m[i,:ObservationDate] < m[i,:EarliestDate]) || (m[i,:ObservationDate] < leftcensor)
            m[i,:OutsideBounds]=true
        end
        if (!ismissing(m[i,:LatestDate]) && m[i,:ObservationDate] > m[i,:LatestDate]) || (m[i,:ObservationDate] > periodend)
            m[i,:OutsideBounds]=true
        end
    end
    #filter if outside bounds
    filter!([:OutsideBounds] => x -> !x, m)
    @info "$(nrow(m)) $(node) marital statuses inside bounds"
    a = freqtable(m,:MaritalStatus)
    @info "Marital Status breakdown for $(node)" a
    select!(m,[:IndividualId, :ObservationDate, :MaritalStatus])
    disallowmissing!(m,:ObservationDate)
    sort!(m, [:IndividualId, :ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "MaritalStatus.arrow"), m, compress=:zstd)
    return nothing
end #maritalstatus
#endregion
#region Labour status
function readlabourstatuses(node::String)
    labourstatus(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
end
function labourstatus(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql = """SELECT
      UPPER(CONVERT(varchar(50),IndividualUid)) IndividualUid
    , CONVERT(date, EO.EventDate) ObservationDate
    , CurrentEmployment
    , EmploymentSector
    , EmploymentType
    , Employer
    FROM dbo.IndividualObservations IO
        JOIN dbo.Events EO ON IO.ObservationUid=EO.EventUid  
    --WHERE NOT (CurrentEmployment IN (0,100)
    --AND EmploymentSector IN (0,100)
    --AND EmploymentType   IN (0,200)
    --AND Employer IN (0,300));
    """
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(s)) $(node) labour statuses from database"
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    @info "$(nrow(si)) $(node) labour statuses after individual map"
    select!(si,[:IndividualId, :ObservationDate, :CurrentEmployment, :EmploymentSector, :EmploymentType, :Employer])
    individualbounds = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(si, individualbounds, on = :IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    insertcols!(m,:OutsideBounds => false)
    for i=1:nrow(m)
        if (!ismissing(m[i,:EarliestDate]) && m[i,:ObservationDate] < m[i,:EarliestDate]) || (m[i,:ObservationDate] < leftcensor)
            m[i,:OutsideBounds]=true
        end
        if (!ismissing(m[i,:LatestDate]) && m[i,:ObservationDate] > m[i,:LatestDate]) || (m[i,:ObservationDate] > periodend)
            m[i,:OutsideBounds]=true
        end
    end
    #filter if outside bounds
    filter!([:OutsideBounds] => x -> !x, m)
    select!(m,[:IndividualId, :ObservationDate, :CurrentEmployment, :EmploymentSector, :EmploymentType, :Employer])
    disallowmissing!(m,:ObservationDate)
    @info "Read $(nrow(m)) $(node) labour statuses inside bounds"
    a = freqtable(m,:CurrentEmployment)
    @info "CurrentEmployment breakdown for $(node)" a
    a = freqtable(m,:EmploymentSector)
    @info "EmploymentSector breakdown for $(node)" a
    a = freqtable(m,:EmploymentType)
    @info "EmploymentType breakdown for $(node)" a
    a = freqtable(m,:Employer)
    @info "Employer breakdown for $(node)" a
    sort!(m, [:IndividualId, :ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "LabourStatus.arrow"), m, compress=:zstd)
    return nothing
end #labourstatus
#endregion
end # module
