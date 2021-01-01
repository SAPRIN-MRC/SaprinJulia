using Parameters
using Dates
using ODBC
using DBInterface
using DataFrames
using JSON
using Logging
using FreqTables
using Arrow
using Query
using DataValues
using TimerOutputs
using ShiftedArrays
using Statistics

#region TimerOutputs
const to = TimerOutput()
#endregion

#region Setup Logging
l = open("log.log", "a+")
io = IOContext(l, :displaysize=>(100,100))
logger = SimpleLogger(io)
global_logger(logger)
@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))" 
flush(io)
#endregion

#region Settings
function readsettings(f)
    return JSON.parsefile(f; dicttype=Dict, inttype=Int32, use_mmap=false)
end

s = readsettings("settings.json")
@with_kw struct Settings 
    NodeId::Int32 = s["NodeId"]
    PeriodEnd::DateTime = DateTime(s["PeriodEnd"])
    Node::String = s["Node"]
    BaseDirectory::String = s["BaseDirectory"]
    Server::String = s["Server"]
    Database::String = s["Databases"][s["Node"]]
    Databases = s["Databases"]
end # struct

settings = Settings()
@info "Processing node $(settings.Node)"
flush(io)
#endregion

#region Create data directories
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
#endregion

#region Read Individuals
"Read individuals and anonimise for node specified in settings and save id map and individual data to to arrow files"
@timeit to function readindividuals(db::String, node::String, basedirectory::String)
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
    individuals = individuals |> @mutate(MotherUid = isna(_.MotherUid) ? _.WomanUid : _.MotherUid) |> @select(-:WomanUid) |> DataFrame
    # ids::Array{Int32} = collect(1:nrow(individuals)) #does not result in smaller file size
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
    mothers = individuals |> @select(:MotherId) |> @filter(!isna(_.MotherId)) |> @unique() |> DataFrame
    fathers = individuals |> @select(:FatherId) |> @filter(!isna(_.FatherId)) |> @unique() |> DataFrame
    mothers = mothers |> @join(individuals,get(_.MotherId),_.IndividualId,{_.MotherId,__.DoD}) |> @rename(:DoD => :MotherDoD) |> DataFrame
    fathers = fathers |> @join(individuals,get(_.FatherId),_.IndividualId,{_.FatherId,__.DoD}) |> @rename(:DoD => :FatherDoD) |> DataFrame
    individuals = leftjoin(individuals, mothers, on=:MotherId => :MotherId, makeunique=true, matchmissing=:equal)
    individuals = leftjoin(individuals, fathers, on=:FatherId => :FatherId, makeunique=true, matchmissing=:equal)
    individuals = individuals |> @mutate(MotherDoD = isna(_.MotherDoD_1) ? _.MotherDoD : _.MotherDoD_1, FatherDoD = isna(_.FatherDoD_1) ? _.FatherDoD : _.FatherDoD_1) |> @select(-:MotherDoD_1,-:FatherDoD_1) |> DataFrame
    Arrow.write(joinpath(basedirectory, node, "Staging", "Individuals.arrow"), individuals, compress=:zstd)
    return nothing
end # readindividuals
function readindividuals(s::Settings)
    return readindividuals(s.Database, s.Node, s.BaseDirectory)
end
@info "Reading individuals start = $(now())"
#=readindividuals(settings.Databases["AHRI"],"AHRI",settings.BaseDirectory)
flush(io)
#readindividuals(settings.Databases["DIMAMO"],"DIMAMO",settings.BaseDirectory)
flush(io)
#readindividuals(settings.Databases["Agincourt"],"Agincourt",settings.BaseDirectory)
flush(io)
=#
@info "Completed reading individuals"
#endregion

#region read Locations
"Read location and anonimise for node specified in settings and save id map and location data to to arrow files"
@timeit to function readlocations(db::String, node::String, basedirectory::String)
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
    sql3 = node == "AHRI" ? "WHERE AreaTypeId=1 AND LA.AreaSubTypeId=1" : " WHERE AreaTypeId=1" # AHRI has not standard AreaSubtype for LocalAreas
    locations = DBInterface.execute(con, sql1 * sql3 * sql2; iterate_rows=true) |> DataFrame
    @info "Read $(nrow(locations)) $(node) locations"
    DBInterface.close!(con)
    sort!(locations, :LocationUid)
    locations.LocationId = 1:nrow(locations)
    # Convert gui ids to integer ids
    map = locations[!,[:LocationUid,:LocationId]]
    Arrow.write(joinpath(basedirectory, node, "Staging", "LocationMap.arrow"), map, compress=:zstd)
    areas = locations |> @select(:AreaUid) |> @filter(!isna(_.AreaUid)) |> @unique() |> DataFrame
    areas.AreaId = 1:nrow(areas)
    Arrow.write(joinpath(basedirectory, node, "Staging", "AreaMap.arrow"), areas, compress=:zstd)
    locations = leftjoin(locations, areas, on=:AreaUid => :AreaUid, makeunique=true, matchmissing=:equal)
    locations = select!(locations, [:LocationId,:NodeId,:LocationTypeId,:AreaId])
    Arrow.write(joinpath(basedirectory, node, "Staging", "Locations.arrow"), locations, compress=:zstd)
    a = freqtable(locations, :AreaId)
    @info "Area breakdown for $(node)" a
    return nothing
end # readlocations
function readlocations(s::Settings)
    return readlocations(s.Database, s.Node, s.BaseDirectory)
end
@info "Reading locations start = $(now())"
#readlocations(settings.Databases["AHRI"],"AHRI",settings.BaseDirectory)
flush(io)
#readlocations(settings.Databases["DIMAMO"],"DIMAMO",settings.BaseDirectory)
flush(io)
#readlocations(settings.Databases["Agincourt"],"Agincourt",settings.BaseDirectory)
flush(io)
@info "Completed reading locations"
#endregion
"Constrain date a to be no larger than b"
function rightcensor(a::DataValue{Date}, b::Date)::Date
    return rightcensor(get(a, b) , b) #get returns underlying Date, with default b is a is missing (isna)
end
function rightcensor(a::Date,b::Date)
    return a <= b ? a : b
end
#region individual residence episodes
"Retrieve and save residence episodes directly from database"
@timeit to function readresidences(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
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
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    residences = innerjoin(residences, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(basedirectory,node,"Staging","LocationMap.arrow")) |> DataFrame
    residences = innerjoin(residences, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(residences,[:IndividualId,:LocationId,:StartDate,:StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
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
            residences[i, :StartType] = 1 #set to enumeration
            residences[i, :StartDate] = Date(residences[i, :StartObservationDate])
        end
        if residences[i,:EndDate] > periodend
            residences[i, :EndDate] = periodend #right censor to period end
            residences[i, :EndType] = 9 #end of episode beyond periodend => OBE
        end
        if residences[i, :StartType] in recodeStarts
            residences[i, :StartType] = 3
        end 
        if residences[i, :EndType] in recodeEnds
            residences[i, :EndType] = 4
        end 
    end
    filter!(:StartDate => s -> s<=periodend, residences)        # drop episodes that start after period end
    filter!([:StartDate,:EndDate] => (s,e) -> s<=e, residences) # start date must be smaller or equal to end date

    sort!(residences,[:IndividualId,:StartDate,:StartType])
    residences.ResidenceId = 1:nrow(residences)
    insertcols!(residences,:ResidentIndex, :GapStart => 0, :GapEnd => 0)
    df = combine(groupby(residences, :IndividualId), :LocationId, :ResidenceId, :StartDate, :StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex, :GapStart, :GapEnd,
                                     :StartDate => Base.Fix2(lead,1) => :NextStart, :EndDate => Base.Fix2(lag,1) => :LastEnd)
    for i = 1:nrow(df)
        if !ismissing(df[i,:NextStart])
            gap = Dates.value(df[i,:NextStart]-df[i,:EndDate])
            if gap<=0
                df[i,:EndType] = 5 #internal outmigration EXT
             elseif gap>180 && df[i,:EndType]!=300 #exclude refusals
                df[i,:EndType] = 4 #external outmigration OMG
            end
        end
        if !ismissing(df[i,:LastEnd])
            gap = Dates.value(df[i,:StartDate]-df[i,:LastEnd])
            if gap<=0
                df[i,:StartDate]=df[i,:LastEnd] + Dates.Day(1)
                df[i,:StartType] = 6 #internal inmigration ENT
            elseif gap>0 && gap<=180 && df[i,:StartType]==6
                df[i,:StartDate]=df[i,:LastEnd] + Dates.Day(1) #close internal migration gap
            elseif gap>180 && !(df[i,:StartType]==300 || df[i,:StartType]==301) #exclude refusals
                df[i,:StartType] = 3 #external inmigration
            end
        end
    end
    filter!([:StartDate,:EndDate] => (s,e) -> s<=e, df) # start date must be smaller or equal to end date
    select!(df,[:ResidenceId, :IndividualId, :LocationId, :StartDate,:StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate, :ResidentIndex])
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
@timeit to function eliminateresidenceoverlaps(node::String, basedirectory::String)
    residences = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualResidencies.arrow")) |> DataFrame
    @info "Node $(node) $(nrow(residences)) episodes before overlap elimination"
    select!(residences, [:ResidenceId, :IndividualId, :LocationId, :StartDate,:StartType, :EndDate, :EndType, :ResidentIndex])
    insertcols!(residences,:ResidentIndex, :GapStart => 0, :GapEnd => 0, :Gap => 0)
    s = similar(residences,0)
    @time for row in eachrow(residences)
        tf = DataFrame(row)
        ttf=repeat(tf, Dates.value.(row.EndDate-row.StartDate) + 1)
        ttf.DayDate = ttf.StartDate .+ Dates.Day.(0:nrow(ttf)-1)
        ttf.Start = ttf.DayDate .== ttf.StartDate
        ttf.End = ttf.DayDate .== ttf.EndDate
        append!(s,ttf, cols = :union)
    end
    n = nrow(s)
    @info "$(n) day rows for $(node)"
    @time sort!(s,[:IndividualId,:DayDate,order(:ResidentIndex, rev=true), :StartDate, order(:EndDate, rev=true)]);
    @time unique!(s,[:IndividualId,:DayDate]);
    n = nrow(s)
    @info "$(n) unique day rows for $(node)"
    lastindividual = -1
    gap = 0
    n = nrow(s)
    for i = 1:n
        if lastindividual != s[i,:IndividualId]
            lastindividual=s[i,:IndividualId]
            gap = 0
        else
            lastgap = Dates.value(s[i,:DayDate]-s[i-1,:DayDate])
            nextgap = 0
            if i < n
                nextgap = s[i,:IndividualId] != s[i+1,:IndividualId] ? 0 : Dates.value(s[i+1,:DayDate]-s[i,:DayDate])
            end
            if lastgap>1
                s[i,:GapEnd] = 1
                gap = gap == 0 ? 1 : 0
            end
            if nextgap>1
                s[i,:GapStart]=1
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
@timeit to function readresidencestatus(db::String, node::String, basedirectory::String, periodend::Date, leftcensor::Date)
    con = ODBC.Connection(db)
    sql ="""/*
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
        CASE
          WHEN LastStatus=NextStatus THEN LastStatus
          ELSE ResStatusCode
        END ResStatusCode,
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
    filter!(:ObservationDate => s -> s<=periodend, resstatuses)        # drop statuses after period end
    filter!(:ObservationDate => s -> s>=leftcensor, resstatuses)       # drop statuses before leftcensor date
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    resstatuses = innerjoin(resstatuses, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(basedirectory,node,"Staging","LocationMap.arrow")) |> DataFrame
    resstatuses = innerjoin(resstatuses, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(resstatuses,[:IndividualId,:LocationId,:ObservationDate,:ResidentStatus])
    sort!(resstatuses, [:IndividualId,:LocationId,:ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "ResidentStatus.arrow"), resstatuses, compress=:zstd)
    @info "Wrote $(nrow(resstatuses)) $(node) residence statuses"
    return nothing
end #readresidencestatus
# readresidences(settings.Databases["AHRI"], "AHRI", settings.BaseDirectory, Date(settings.PeriodEnd), Date(2000,01,01))
# readresidences(settings.Databases["DIMAMO"], "DIMAMO", settings.BaseDirectory, Date(settings.PeriodEnd), Date(1995,01,26))
# readresidences(settings.Databases["Agincourt"], "Agincourt", settings.BaseDirectory, Date(settings.PeriodEnd), Date(1992,03,01))
# eliminateresidenceoverlaps("DIMAMO", settings.BaseDirectory)
# eliminateresidenceoverlaps("Agincourt", settings.BaseDirectory)
readresidencestatus(settings.Databases["DIMAMO"], "DIMAMO", settings.BaseDirectory, Date(settings.PeriodEnd), Date(1995,01,26))
readresidencestatus(settings.Databases["Agincourt"], "Agincourt", settings.BaseDirectory, Date(settings.PeriodEnd), Date(1992,03,01))
#endregion
#region clean up
println(io)
show(io,to)
println(io)
close(io)
#endregion