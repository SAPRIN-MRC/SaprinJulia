using Parameters
using Dates
using ODBC
using DBInterface
using DataFrames
using JSON
using Logging
using FreqTables
using Arrow

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
        I.IndividualUid,
        I.Sex,
        CONVERT(date,SE.EventDate) DoB,
        CASE
        WHEN EE.EventTypeId=7 THEN CONVERT(date,EE.EventDate)
        ELSE NULL
        END DoD,
        I.MotherUid,
        I.FatherUid,
        I.MotherDoD,
        I.FatherDoD
    FROM dbo.Individuals I
        JOIN dbo.Events SE ON I.BirthEventUid=SE.EventUid
        JOIN dbo.Events EE ON I.EndEventUid=EE.EventUid
    """    
    @time individuals = DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(individuals)) individuals"
    sex = freqtable(individuals, :Sex)
    @info "Sex breakdown" sex
    individuals.IndividualId = 1:nrow(individuals)
    map = individuals[!,[:IndividualUid,:IndividualId]]
    @time Arrow.write(joinpath(s.BaseDirectory, s.Node, "Staging", "IndividualMap.arrow"), map, compress=:zstd)
    individuals = leftjoin(individuals, map, on=:MotherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    individuals = leftjoin(individuals, map, on=:FatherUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    select!(individuals, [:IndividualId,:Sex,:DoB,:DoD,:IndividualId_1,:IndividualId_2,:MotherDoD,:FatherDoD])
    rename!(individuals, :IndividualId_1 => :MotherId, :IndividualId_2 => :FatherId)
    #@time Arrow.write(joinpath(s.BaseDirectory, s.Node, "Staging", "Individuals.arrow"), individuals, compress=:zstd)
    return individuals
end

df = readindividuals(settings)
# endregion

close(io)