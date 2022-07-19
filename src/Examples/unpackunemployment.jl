using SAPRINCore
using DataFrames
using Arrow
using ODBC
using DBInterface
using CSV

function isset(value, flag)
    value & flag > 0 ? 1 : 0
end

function unpackunemployment(node::String)
    con = ODBC.Connection(settings.Databases[node])
    sql = """SELECT
      UPPER(CONVERT(varchar(50),IndividualUid)) IndividualUid
    , CONVERT(date, EO.EventDate) ObservationDate
    , Unemployment
    FROM dbo.IndividualObservations IO
        JOIN dbo.Events EO ON IO.ObservationUid=EO.EventUid 
    WHERE NOT Unemployment IS NULL 
    """
    df =  DBInterface.execute(con, sql; iterate_rows = true) |> DataFrame
    DBInterface.close!(con)
    @info "Read $(nrow(df)) $(node) labour statuses from database"
    disallowmissing!(df, [:ObservationDate, :Unemployment])
    select!(df, :IndividualUid, :ObservationDate, :Unemployment, 
                :Unemployment => ByRow(x -> isset(x,1)) => :Studying,
                :Unemployment => ByRow(x -> isset(x,2)) => :Looking,
                :Unemployment => ByRow(x -> isset(x,4)) => :Retired,
                :Unemployment => ByRow(x -> isset(x,8)) => :Sick,
                :Unemployment => ByRow(x -> isset(x,16)) => :Pregnant,
                :Unemployment => ByRow(x -> isset(x,32)) => :ChildCare,
                :Unemployment => ByRow(x -> isset(x,64)) => :CareSick,
                :Unemployment => ByRow(x -> isset(x,128)) => :Retrenched,
                :Unemployment => ByRow(x -> isset(x,256)) => :NotLooking,
                :Unemployment => ByRow(x -> isset(x,512)) => :Other,
                :Unemployment => ByRow(x -> isset(x,1024)) => :DontKnow,
                :Unemployment => ByRow(x -> isset(x,2048)) => :Refused,
                :Unemployment => ByRow(x -> isset(x,4096)) => :Seasonal,
                :Unemployment => ByRow(x -> isset(x,8192)) => :Disabled)
    CSV.write(joinpath(stagingpath(node),"UnemploymentUnpacked.csv"), df)
    gf = combine(df, :Studying => sum, :Looking => sum, :Retired => sum, :Sick => sum, :Pregnant  => sum,
    :ChildCare => sum, :CareSick => sum, :Retrenched => sum, :NotLooking => sum, :Other => sum,
    :DontKnow => sum, :Refused => sum, :Seasonal => sum,  :Disabled => sum)
end

d = unpackunemployment("DIMAMO")