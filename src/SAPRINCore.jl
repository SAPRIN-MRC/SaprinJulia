module SAPRINCore

using JSON
using Parameters
using Dates
using ODBC
using DBInterface
using DataFrames

greet() = print("Hello World!")

function readsettings(f)
    return JSON.parsefile(f; dicttype=Dict, inttype=Int32, use_mmap=false)
end

settings = readsettings("settings.json")

@with_kw struct Settings 
    NodeId::Int = settings["NodeId"]
    PeriodEnd::DateTime = DateTime(settings["PeriodEnd"])
    Node::String = settings["Node"]
    BaseDirectory::String = settings["BaseDirectory"]
    Server::String = settings["Server"]
    Database::String = settings["Databases"][settings["Node"]]
end #struct

function readindividuals!(s::Settings, df::DataFrame)
    db = s.Database
    con = ODBC.Connection(db)
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
    df = DBInterface.execute(con,sql) |> DataFrame
    println(nrow(df))
    nothing
end #readindividuals

end # module
