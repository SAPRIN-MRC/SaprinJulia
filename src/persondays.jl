using Arrow
using DataFrames
using SAPRINCore

function persondays(node)
    df = Arrow.Table(joinpath(episodepath(node), "SurveillanceEpisodesBasic_batched.arrow"))
    # sf = combine(df, :Days => sum => :PersonDays)
    return sum(df.Days)
end

days = persondays("DIMAMO") + persondays("Agincourt") + persondays("AHRI")
years = days/365.25
println("Years $(years) days $(days)")