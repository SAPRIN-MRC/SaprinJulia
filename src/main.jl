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

# region Setup Logging
io = open("log.log", "w+")
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

function readindividuals(s::Settings)
    con = ODBC.Connection(s.Database)
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
    #Arrow.write(joinpath(s.BaseDirectory, s.Node, "Staging", "Individuals.arrow"), individuals, compress=:zstd)
    # ids::Array{Int32} = collect(1:nrow(individuals)) #does not result in smaller file size
    individuals.IndividualId = 1:nrow(individuals)
    # Convert gui ids to integer ids
    map = individuals[!,[:IndividualUid,:IndividualId]]
    Arrow.write(joinpath(s.BaseDirectory, s.Node, "Staging", "IndividualMap.arrow"), map, compress=:zstd)
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
    Arrow.write(joinpath(s.BaseDirectory, s.Node, "Staging", "Individuals.arrow"), individuals, compress=:zstd)
    return individuals
end

@time df = readindividuals(settings)
# endregion

close(io)