using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays
using Missings
using XLSX
using CategoricalArrays


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
    WHERE NOT (CurrentEmployment IN (0,100)
    AND EmploymentSector IN (0,100)
    AND EmploymentType   IN (0,200)
    AND Employer IN (0,300));
    """
    s =  DBInterface.execute(con, sql;iterate_rows=true) |> DataFrame
    DBInterface.close!(con)
    println("Read $(nrow(s)) $(node) labour statuses from database")
    individualmap = Arrow.Table(joinpath(basedirectory,node,"Staging","IndividualMap.arrow")) |> DataFrame
    si = innerjoin(s, individualmap, on=:IndividualUid => :IndividualUid, makeunique=true, matchmissing=:equal)
    println("$(nrow(si)) $(node) labour statuses after individual map")
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
    @info "Read $(nrow(m)) $(node) labour statuses inside bounds"
    a = freqtable(m,:CurrentEmployment)
    println("CurrentEmployment breakdown for $(node)")
    show(a)
    println()
    select!(m,[:IndividualId, :ObservationDate, :CurrentEmployment, :EmploymentSector, :EmploymentType, :Employer])
    disallowmissing!(m,:ObservationDate)
    sort!(m, [:IndividualId, :ObservationDate])
    Arrow.write(joinpath(basedirectory, node, "Staging", "LabourStatus.arrow"), m, compress=:zstd)
    return nothing
end
node = "AHRI"
df = labourstatus(settings.Databases[node], node, settings.BaseDirectory, Date(settings.PeriodEnd), Date(settings.LeftCensorDates[node]))
