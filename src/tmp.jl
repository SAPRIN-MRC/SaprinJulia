using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using Query

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
    println("Read $(nrow(s)) $(node) education statuses from database")
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    individualbounds = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualBounds.arrow")) |> DataFrame
    m = leftjoin(si, individualbounds, on = :IndividualId => :IndividualId, makeunique=true, matchmissing=:equal)
    println("Read $(nrow(m)) $(node) bounded education statuses")
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
    println("Read $(nrow(m)) $(node) education statuses inside bounds")
    filter!([:CurrentEducation, :HighestSchoolLevel, :HighestNonSchoolLevel] => (x,y,z) -> !(x<0 && y<0 && z<0), m)
    println("Read $(nrow(m)) $(node) education statuses not missing")
    disallowmissing!(m,[:ObservationDate])
    select!(m,[:IndividualId,:ObservationDate,:CurrentEducation, :HighestSchoolLevel, :HighestNonSchoolLevel])
    sort!(m,[:IndividualId,:ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "EducationStatuses.arrow"), m, compress=:zstd)
    return m
end
node = "DIMAMO"
educationstatus(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
