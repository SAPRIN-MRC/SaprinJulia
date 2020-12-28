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

# region TimerOutputs
const to = TimerOutput()
# endregion

# region Setup Logging
io = open("log.log", "a+")
logger = SimpleLogger(io)
global_logger(logger)
@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))" 
flush(io)
# endregion

# region Settings
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
# endregion

# region Create data directories
if !isdir(joinpath(settings.BaseDirectory, settings.Node))
    mkdir(joinpath(settings.BaseDirectory, settings.Node))
end
if !isdir(joinpath(settings.BaseDirectory, settings.Node, "Staging"))
    mkdir(joinpath(settings.BaseDirectory, settings.Node, "Staging"))
end
# endregion

# region Read Individuals
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
    @info "Read $(nrow(individuals)) individuals"
    sex = freqtable(individuals, :Sex)
    @info "Sex breakdown" sex
    sort!(individuals, :IndividualUid)
    sql = """SELECT
        UPPER(CONVERT(nvarchar(50),WomanUid)) WomanUid
        , UPPER(CONVERT(nvarchar(50),I.IndividualUid)) ChildUid
        FROM dbo.Pregnancies P
            JOIN dbo.Individuals I ON P.OutcomeEventUid=I.BirthEventUid
    """  
    pregnancies = DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(pregnancies)) pregnancies"
    pregnancies = unique!(pregnancies, :ChildUid)
    @info "Read $(nrow(pregnancies)) unique children"
    # Add MotherUid from pregnancies
    individuals = leftjoin(individuals, pregnancies, on=:IndividualUid => :ChildUid, makeunique=true, matchmissing=:equal)
    individuals = individuals |> @mutate(MotherUid = isna(_.MotherUid) ? _.WomanUid : _.MotherUid) |> @select(-:WomanUid) |> DataFrame
    # Arrow.write(joinpath(s.BaseDirectory, s.Node, "Staging", "Individuals.arrow"), individuals, compress=:zstd)
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
@info "Reading $(settings.Node) individuals start = $(now())"
# readindividuals(settings.Databases["AHRI"],"AHRI",settings.BaseDirectory)
# readindividuals(settings.Databases["DIMAMO"],"DIMAMO",settings.BaseDirectory)
@info "Completed reading $(settings.Node) individuals"
# endregion
# region read Locations
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
    @info "Read $(nrow(locations)) locations"
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
    @info "Area breakdown" a
    return nothing
end # readlocations
function readlocations(s::Settings)
    return readlocations(s.Database, s.Node, s.BaseDirectory)
end
@info "Reading $(settings.Node) locations start = $(now())"
# readlocations(settings.Databases["AHRI"],"AHRI",settings.BaseDirectory)
# readlocations(settings.Databases["DIMAMO"],"DIMAMO",settings.BaseDirectory)
@info "Completed reading $(settings.Node) locations"
# endregion
function rightcensor(a::DataValue{Date}, b::Date)::Date
    return a <= b ? get(a, b) : b
end
# region individual residence episodes
@timeit to function readresidences(db::String, node::String, basedirectory::String, periodend::DateTime)
    con = ODBC.Connection(db)
    sql = """SELECT
      IndividualResidenceUid
    , UPPER(CONVERT(nvarchar(50),IndividualUid)) IndividualUid
    , UPPER(CONVERT(nvarchar(50),IR.LocationUid)) LocationUid
    , CASE
        WHEN SE.EventDate<'20000101' THEN CONVERT(date,SO.EventDate)
        ELSE CONVERT(date,SE.EventDate)
        END StartDate
    , SE.EventTypeId StartType
    , CONVERT(date,SO.EventDate) StartObservationDate
    , CONVERT(date,EE.EventDate) EndDate
    , EE.EventTypeId EndType
    , CONVERT(date,EO.EventDate) EndObservationDate
    FROM dbo.IndividualResidences IR
        JOIN dbo.Events SE ON IR.StartEventUid=SE.EventUid
        JOIN dbo.Events EE ON IR.EndEventUid=EE.EventUid
        JOIN dbo.Events SO ON SE.ObservationEventUid=SO.EventUid
        JOIN dbo.Events EO ON EE.ObservationEventUid=EO.EventUid
    WHERE EE.EventTypeId>0 --get rid of NYO episodes
    """
    residences = DBInterface.execute(con, sql; iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(residences)) residences"
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    residences = innerjoin(residences, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    locationmap = Arrow.Table(joinpath(basedirectory,node,"Staging","LocationMap.arrow")) |> DataFrame
    residences = innerjoin(residences, locationmap, on=:LocationUid => :LocationUid, makeunique=true, matchmissing=:equal)
    select!(residences,[:IndividualId,:LocationId,:StartDate,:StartType, :EndDate, :EndType, :StartObservationDate, :EndObservationDate])
    filter!(:StartDate => s -> s<=periodend, residences)
    filter!([:StartDate,:EndDate] => (s,e) -> s<=e, residences) #start date must be smaller or equal to end date
    p = Date(periodend)
    residences = residences |> @mutate(EndDate = rightcensor(_.EndDate,p)) |> DataFrame
    sort!(residences,[:IndividualId,:StartDate])
    residences.ResidenceId = 1:nrow(residences)
    Arrow.write(joinpath(basedirectory, node, "Staging", "IndividualResidencies.arrow"), residences, compress=:zstd)
    years = Dates.year.(residences.StartDate) 
    a = freqtable(years)
    @info "Start years breakdown" a
    years = Dates.year.(residences.EndDate) 
    a = freqtable(years)
    @info "End years breakdown" a
    return nothing
end
function readresidences(s::Settings)
    return readresidences(s.Database, s.Node, s.BaseDirectory, s.PeriodEnd)
end
# AHRI individual residence can be extracted directly
# DIMAMO and Agincourt must be processed first to remove non-resident portions of the residence episodes
if settings.Node == "AHRI"
    df = readresidences(settings)
else
end
# endregion
# region clean up
println(io)
show(io,to)
println(io)
close(io)
# endregion